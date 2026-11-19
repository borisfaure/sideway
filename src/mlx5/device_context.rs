//! mlx5 specific device attributes, queried with [`DeviceContext::query_mlx5_device`].
use rdma_mummy_sys::{mlx5dv_context, mlx5dv_context_comp_mask, mlx5dv_query_device};
use std::{io, mem::MaybeUninit};

use crate::ibverbs::device_context::DeviceContext;

/// Error returned by [`DeviceContext::query_mlx5_device`].
#[derive(Debug, thiserror::Error)]
#[error("failed to query mlx5 device attributes")]
#[non_exhaustive]
pub struct QueryMlx5DeviceError(#[from] pub QueryMlx5DeviceErrorKind);

/// The enum type for [`QueryMlx5DeviceError`].
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
#[non_exhaustive]
pub enum QueryMlx5DeviceErrorKind {
    /// The device is not driven by the mlx5 provider (`EOPNOTSUPP`).
    #[error("device is not an mlx5 device")]
    NotMlx5(#[source] io::Error),
    Ibverbs(#[from] io::Error),
}

/// The mlx5 specific attributes of a device.
#[derive(Clone, Copy)]
pub struct Mlx5DeviceAttribute {
    attr: mlx5dv_context,
}

impl std::fmt::Debug for Mlx5DeviceAttribute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mlx5DeviceAttribute")
            .field("flags", &self.flags())
            .field("dc_odp_caps", &self.dc_odp_caps())
            .field("max_dc_rd_atom", &self.max_dc_rd_atom())
            .finish()
    }
}

impl Mlx5DeviceAttribute {
    fn has(&self, mask: mlx5dv_context_comp_mask) -> bool {
        (self.attr.comp_mask & u64::from(mask.0)) != 0
    }

    /// Returns the raw `MLX5DV_CONTEXT_FLAGS_*` bits.
    pub fn flags(&self) -> u64 {
        self.attr.flags
    }

    /// Returns the On-Demand Paging capabilities of DC transport, if the device reports them.
    pub fn dc_odp_caps(&self) -> Option<u32> {
        self.has(mlx5dv_context_comp_mask::MLX5DV_CONTEXT_MASK_DC_ODP_CAPS)
            .then_some(self.attr.dc_odp_caps)
    }

    /// Returns the maximum number of outstanding RDMA READ / atomic a DC target can serve, and a
    /// DC initiator can issue, if the device reports them.
    pub fn max_dc_rd_atom(&self) -> Option<(u64, u64)> {
        self.has(mlx5dv_context_comp_mask::MLX5DV_CONTEXT_MASK_MAX_DC_RD_ATOM)
            .then_some((self.attr.max_dc_rd_atom, self.attr.max_dc_init_rd_atom))
    }
}

impl DeviceContext {
    /// Query the mlx5 specific attributes of the device, with [`mlx5dv_query_device`]. Fails with
    /// [`QueryMlx5DeviceErrorKind::NotMlx5`] on devices of other providers, which also makes it the
    /// way to tell whether DC transport could be used on a device.
    ///
    /// [`mlx5dv_query_device`]: https://man7.org/linux/man-pages/man3/mlx5dv_query_device.3.html
    ///
    pub fn query_mlx5_device(&self) -> Result<Mlx5DeviceAttribute, QueryMlx5DeviceError> {
        let mut attr: mlx5dv_context = unsafe { MaybeUninit::zeroed().assume_init() };
        attr.comp_mask = u64::from(
            (mlx5dv_context_comp_mask::MLX5DV_CONTEXT_MASK_DC_ODP_CAPS
                | mlx5dv_context_comp_mask::MLX5DV_CONTEXT_MASK_MAX_DC_RD_ATOM)
                .0,
        );
        let ret = unsafe { mlx5dv_query_device(self.context.as_ptr(), &mut attr) };
        match ret {
            0 => Ok(Mlx5DeviceAttribute { attr }),
            libc::EOPNOTSUPP => Err(QueryMlx5DeviceErrorKind::NotMlx5(io::Error::from_raw_os_error(ret)).into()),
            err => Err(QueryMlx5DeviceErrorKind::Ibverbs(io::Error::from_raw_os_error(err)).into()),
        }
    }
}
