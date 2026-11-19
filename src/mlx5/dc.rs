//! Dynamically Connected (DC) transport of mlx5 devices.
//!
//! A [`DcTarget`] (DCT) is the passive side: it is attached to a [`SharedReceiveQueue`] and serves
//! RDMA READ / WRITE (and SEND) from any [`DcInitiator`] that knows its number ([`DcTarget::dctn`]),
//! its address and its access key. A [`DcInitiator`] (DCI) is the active side: every Work Request
//! names the target it goes to, through [`DcPostSendGuard::setup_destination`].
//!
//! Neither side needs a connection handshake, so there is no RDMA CM involved: the target's
//! address (GID or LID), DCTN and access key are exchanged out of band.
//!
//! # State transitions
//!
//! Both QPs are moved with [`QueuePairAttribute`]s as RC QPs are:
//!
//! - DCT: `Reset -> Init` with port, pkey index and access flags, then `Init -> ReadyToReceive` with
//!   path MTU, address vector (GRH with the source GID index on RoCE) and min RNR timer. The DCTN
//!   is only valid after that last transition.
//! - DCI: `Reset -> Init` with port and pkey index, `Init -> ReadyToReceive` with path MTU and an
//!   address vector giving the port (and, on RoCE, a GRH with the source GID index), then
//!   `ReadyToReceive -> ReadyToSend` with timeout, retry count, RNR retry, SQ PSN and max read
//!   atomic.
//!
//! # Reference
//!
//! - rdma-core manpages: [`mlx5dv_create_qp`], [`mlx5dv_wr_post`]
//!
//! [`mlx5dv_create_qp`]: https://man7.org/linux/man-pages/man3/mlx5dv_create_qp.3.html
//! [`mlx5dv_wr_post`]: https://man7.org/linux/man-pages/man3/mlx5dv_wr_post.3.html
//!
use rdma_mummy_sys::{
    ibv_data_buf, ibv_destroy_qp, ibv_modify_qp, ibv_qp, ibv_qp_attr, ibv_qp_ex, ibv_qp_init_attr_ex,
    ibv_qp_init_attr_mask, ibv_qp_to_qp_ex, ibv_qp_type, ibv_sge, ibv_wr_abort, ibv_wr_atomic_cmp_swp,
    ibv_wr_atomic_fetch_add, ibv_wr_complete, ibv_wr_rdma_read, ibv_wr_rdma_write, ibv_wr_rdma_write_imm, ibv_wr_send,
    ibv_wr_send_imm, ibv_wr_set_inline_data, ibv_wr_set_inline_data_list, ibv_wr_set_sge, ibv_wr_set_sge_list,
    ibv_wr_start, mlx5dv_create_qp, mlx5dv_qp_ex, mlx5dv_qp_ex_from_ibv_qp_ex, mlx5dv_qp_init_attr,
    mlx5dv_qp_init_attr_mask, mlx5dv_wr_set_dc_addr, MLX5DV_DCTYPE_DCI, MLX5DV_DCTYPE_DCT,
};
use std::{
    fmt,
    io::{self, IoSlice},
    marker::PhantomData,
    mem::MaybeUninit,
    ptr::NonNull,
    sync::Arc,
};

use crate::ibverbs::{
    address::AddressHandle,
    completion::{CompletionQueue, GenericCompletionQueue},
    protection_domain::ProtectionDomain,
    queue_pair::{
        private_traits, wr_complete_result, ModifyQueuePairError, ModifyQueuePairErrorKind, PostSendError,
        PostSendGuard, QueuePairAttribute, QueuePairState, SendOperationFlags, WorkRequestFlags, WorkRequestHandle,
    },
    shared_receive_queue::SharedReceiveQueue,
};

/// Error returned by [`DcTargetBuilder::build`] and [`DcInitiatorBuilder::build`].
#[derive(Debug, thiserror::Error)]
#[error("failed to create DC queue pair")]
#[non_exhaustive]
pub struct CreateDcQueuePairError(#[from] pub CreateDcQueuePairErrorKind);

/// The enum type for [`CreateDcQueuePairError`].
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
#[non_exhaustive]
pub enum CreateDcQueuePairErrorKind {
    #[error("no completion queue set")]
    MissingCompletionQueue,
    #[error("no shared receive queue set, a DC target requires one")]
    MissingSharedReceiveQueue,
    Ibverbs(#[from] io::Error),
}

fn new_init_attr(pd: &ProtectionDomain, cq: &GenericCompletionQueue) -> ibv_qp_init_attr_ex {
    let mut attr: ibv_qp_init_attr_ex = unsafe { MaybeUninit::zeroed().assume_init() };
    let cq = unsafe { cq.cq().as_ptr() };
    attr.qp_type = ibv_qp_type::IBV_QPT_DRIVER;
    attr.send_cq = cq;
    attr.recv_cq = cq;
    attr.pd = pd.pd.as_ptr();
    attr.comp_mask = ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_PD.0;
    attr
}

fn create_dc_qp(
    pd: &ProtectionDomain, attr: &mut ibv_qp_init_attr_ex, dv_attr: &mut mlx5dv_qp_init_attr,
) -> Result<NonNull<ibv_qp>, CreateDcQueuePairError> {
    let qp = unsafe { mlx5dv_create_qp(pd._dev_ctx.context.as_ptr(), attr, dv_attr) };
    NonNull::new(qp).ok_or_else(|| CreateDcQueuePairErrorKind::Ibverbs(io::Error::last_os_error()).into())
}

fn modify_qp(qp: NonNull<ibv_qp>, attr: &QueuePairAttribute) -> Result<(), ModifyQueuePairError> {
    let mut qp_attr = ibv_qp_attr { ..attr.attr };
    let ret = unsafe { ibv_modify_qp(qp.as_ptr(), &mut qp_attr, attr.attr_mask.bits()) };
    match ret {
        0 => Ok(()),
        err => Err(ModifyQueuePairErrorKind::Ibverbs(io::Error::from_raw_os_error(err)).into()),
    }
}

/// A DC target, created with [`DcTargetBuilder::build`].
///
/// It holds references to its [`ProtectionDomain`], [`CompletionQueue`] and
/// [`SharedReceiveQueue`], so they are destroyed only after it.
pub struct DcTarget {
    qp: NonNull<ibv_qp>,
    _pd: Arc<ProtectionDomain>,
    _cq: GenericCompletionQueue,
    _srq: Arc<SharedReceiveQueue>,
}

unsafe impl Send for DcTarget {}
unsafe impl Sync for DcTarget {}

impl Drop for DcTarget {
    fn drop(&mut self) {
        let ret = unsafe { ibv_destroy_qp(self.qp.as_ptr()) };
        assert_eq!(ret, 0);
    }
}

impl fmt::Debug for DcTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DcTarget").field("qp", &self.qp).finish()
    }
}

impl DcTarget {
    /// # Safety
    ///
    /// Return the handle of the DCT; we mark this method unsafe because the lifetime of `ibv_qp`
    /// is not associated with the return value.
    pub unsafe fn qp(&self) -> NonNull<ibv_qp> {
        self.qp
    }

    /// Modify the DCT's attributes, see the [module documentation](self) for its transitions.
    pub fn modify(&mut self, attr: &QueuePairAttribute) -> Result<(), ModifyQueuePairError> {
        modify_qp(self.qp, attr)
    }

    /// Get the DCT's state.
    pub fn state(&self) -> QueuePairState {
        unsafe { self.qp.as_ref().state.into() }
    }

    /// Get the DC Target Number initiators address this DCT with. It is only assigned once the DCT
    /// reached [`QueuePairState::ReadyToReceive`].
    pub fn dctn(&self) -> u32 {
        unsafe { self.qp.as_ref().qp_num }
    }
}

/// A factory for creating [`DcTarget`]s.
pub struct DcTargetBuilder {
    pd: Arc<ProtectionDomain>,
    cq: Option<GenericCompletionQueue>,
    srq: Option<Arc<SharedReceiveQueue>>,
    access_key: u64,
}

impl DcTargetBuilder {
    pub fn new(pd: &Arc<ProtectionDomain>) -> Self {
        DcTargetBuilder {
            pd: Arc::clone(pd),
            cq: None,
            srq: None,
            access_key: 0,
        }
    }

    /// Setup the [`CompletionQueue`] of the DCT, it only receives completions of SENDs.
    pub fn setup_cq<C>(&mut self, cq: C) -> &mut Self
    where
        C: Into<GenericCompletionQueue>,
    {
        self.cq = Some(cq.into());
        self
    }

    /// Setup the [`SharedReceiveQueue`] the DCT takes receive buffers from, the hardware requires
    /// one even when only RDMA READ / WRITE target it.
    pub fn setup_shared_receive_queue(&mut self, srq: &Arc<SharedReceiveQueue>) -> &mut Self {
        self.srq = Some(Arc::clone(srq));
        self
    }

    /// Setup the access key initiators must present, see [`DcDestination::new`].
    pub fn setup_access_key(&mut self, access_key: u64) -> &mut Self {
        self.access_key = access_key;
        self
    }

    /// Create a [`DcTarget`] with [`mlx5dv_create_qp`].
    ///
    /// [`mlx5dv_create_qp`]: https://man7.org/linux/man-pages/man3/mlx5dv_create_qp.3.html
    ///
    pub fn build(&self) -> Result<DcTarget, CreateDcQueuePairError> {
        let cq = self
            .cq
            .clone()
            .ok_or(CreateDcQueuePairErrorKind::MissingCompletionQueue)?;
        let srq = self
            .srq
            .clone()
            .ok_or(CreateDcQueuePairErrorKind::MissingSharedReceiveQueue)?;

        let mut attr = new_init_attr(&self.pd, &cq);
        attr.srq = unsafe { srq.srq().as_ptr() };

        let mut dv_attr: mlx5dv_qp_init_attr = unsafe { MaybeUninit::zeroed().assume_init() };
        dv_attr.comp_mask = u64::from(mlx5dv_qp_init_attr_mask::MLX5DV_QP_INIT_ATTR_MASK_DC.0);
        dv_attr.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCT;
        unsafe { *dv_attr.dc_init_attr.__bindgen_anon_1.dct_access_key.as_mut() = self.access_key };

        Ok(DcTarget {
            qp: create_dc_qp(&self.pd, &mut attr, &mut dv_attr)?,
            _pd: Arc::clone(&self.pd),
            _cq: cq,
            _srq: srq,
        })
    }
}

/// Where a [`DcInitiator`]'s Work Request goes: the path to the target's port, its DCTN and its
/// access key.
#[derive(Debug, Clone)]
pub struct DcDestination {
    ah: Arc<AddressHandle>,
    dctn: u32,
    access_key: u64,
}

impl DcDestination {
    /// `ah` should be created on the [`ProtectionDomain`] of the [`DcInitiator`] using it.
    pub fn new(ah: Arc<AddressHandle>, dctn: u32, access_key: u64) -> Self {
        DcDestination { ah, dctn, access_key }
    }
}

/// A DC initiator, created with [`DcInitiatorBuilder::build`].
///
/// It holds references to its [`ProtectionDomain`] and [`CompletionQueue`], so they are destroyed
/// only after it.
pub struct DcInitiator {
    qp_ex: NonNull<ibv_qp_ex>,
    dv_qp_ex: NonNull<mlx5dv_qp_ex>,
    _pd: Arc<ProtectionDomain>,
    _cq: GenericCompletionQueue,
}

unsafe impl Send for DcInitiator {}
unsafe impl Sync for DcInitiator {}

impl Drop for DcInitiator {
    fn drop(&mut self) {
        let ret = unsafe { ibv_destroy_qp(self.qp().as_ptr()) };
        assert_eq!(ret, 0);
    }
}

impl fmt::Debug for DcInitiator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DcInitiator").field("qp_ex", &self.qp_ex).finish()
    }
}

impl DcInitiator {
    /// # Safety
    ///
    /// Return the basic handle of the DCI; we mark this method unsafe because the lifetime of
    /// `ibv_qp` is not associated with the return value.
    pub unsafe fn qp(&self) -> NonNull<ibv_qp> {
        NonNull::new_unchecked(&mut (*self.qp_ex.as_ptr()).qp_base as _)
    }

    /// Modify the DCI's attributes, see the [module documentation](self) for its transitions.
    pub fn modify(&mut self, attr: &QueuePairAttribute) -> Result<(), ModifyQueuePairError> {
        modify_qp(unsafe { self.qp() }, attr)
    }

    /// Get the DCI's state.
    pub fn state(&self) -> QueuePairState {
        unsafe { self.qp().as_ref().state.into() }
    }

    /// Get the DCI's number.
    pub fn qp_number(&self) -> u32 {
        unsafe { self.qp().as_ref().qp_num }
    }

    /// Starts a post send operation, a [`DcInitiator`] could hold only one [`DcPostSendGuard`] at
    /// the same time.
    pub fn start_post_send(&mut self) -> DcPostSendGuard<'_> {
        unsafe { ibv_wr_start(self.qp_ex.as_ptr()) };
        DcPostSendGuard {
            qp_ex: self.qp_ex,
            dv_qp_ex: self.dv_qp_ex,
            destination: None,
            missing_destination: false,
            _phantom: PhantomData,
        }
    }
}

/// A factory for creating [`DcInitiator`]s.
pub struct DcInitiatorBuilder {
    pd: Arc<ProtectionDomain>,
    cq: Option<GenericCompletionQueue>,
    max_send_wr: u32,
    max_send_sge: u32,
    max_inline_data: u32,
    send_ops_flags: SendOperationFlags,
}

impl DcInitiatorBuilder {
    pub fn new(pd: &Arc<ProtectionDomain>) -> Self {
        DcInitiatorBuilder {
            pd: Arc::clone(pd),
            cq: None,
            max_send_wr: 16,
            max_send_sge: 1,
            max_inline_data: 0,
            send_ops_flags: SendOperationFlags::Send
                | SendOperationFlags::SendWithImmediate
                | SendOperationFlags::Write
                | SendOperationFlags::WriteWithImmediate
                | SendOperationFlags::Read,
        }
    }

    /// Setup the [`CompletionQueue`] receiving the completions of the DCI's Work Requests.
    pub fn setup_cq<C>(&mut self, cq: C) -> &mut Self
    where
        C: Into<GenericCompletionQueue>,
    {
        self.cq = Some(cq.into());
        self
    }

    /// Setup the maximum number of outstanding RDMA Work Requests of the send queue.
    pub fn setup_max_send_wr(&mut self, max_send_wr: u32) -> &mut Self {
        self.max_send_wr = max_send_wr;
        self
    }

    /// Setup the maximum number of scatter / gather elements of a Work Request.
    pub fn setup_max_send_sge(&mut self, max_send_sge: u32) -> &mut Self {
        self.max_send_sge = max_send_sge;
        self
    }

    /// Setup the maximum message size that can be posted inline.
    pub fn setup_max_inline_data(&mut self, max_inline_data: u32) -> &mut Self {
        self.max_inline_data = max_inline_data;
        self
    }

    /// Setup the operations the DCI could post.
    pub fn setup_send_ops_flags(&mut self, send_ops_flags: SendOperationFlags) -> &mut Self {
        self.send_ops_flags = send_ops_flags;
        self
    }

    /// Create a [`DcInitiator`] with [`mlx5dv_create_qp`].
    ///
    /// [`mlx5dv_create_qp`]: https://man7.org/linux/man-pages/man3/mlx5dv_create_qp.3.html
    ///
    pub fn build(&self) -> Result<DcInitiator, CreateDcQueuePairError> {
        let cq = self
            .cq
            .clone()
            .ok_or(CreateDcQueuePairErrorKind::MissingCompletionQueue)?;

        let mut attr = new_init_attr(&self.pd, &cq);
        attr.comp_mask |= ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_SEND_OPS_FLAGS.0;
        attr.send_ops_flags = self.send_ops_flags.bits();
        attr.cap.max_send_wr = self.max_send_wr;
        attr.cap.max_send_sge = self.max_send_sge;
        attr.cap.max_inline_data = self.max_inline_data;

        let mut dv_attr: mlx5dv_qp_init_attr = unsafe { MaybeUninit::zeroed().assume_init() };
        dv_attr.comp_mask = u64::from(mlx5dv_qp_init_attr_mask::MLX5DV_QP_INIT_ATTR_MASK_DC.0);
        dv_attr.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCI;

        let qp = create_dc_qp(&self.pd, &mut attr, &mut dv_attr)?;
        // Owns the QP from here, so an early return below destroys it.
        let mut dci = DcInitiator {
            qp_ex: unsafe { NonNull::new_unchecked(ibv_qp_to_qp_ex(qp.as_ptr())) },
            dv_qp_ex: NonNull::dangling(),
            _pd: Arc::clone(&self.pd),
            _cq: cq,
        };
        dci.dv_qp_ex = NonNull::new(unsafe { mlx5dv_qp_ex_from_ibv_qp_ex(dci.qp_ex.as_ptr()) })
            .ok_or_else(|| CreateDcQueuePairErrorKind::Ibverbs(io::Error::last_os_error()))?;
        Ok(dci)
    }
}

/// The [`PostSendGuard`] of a [`DcInitiator`], built on the [`ibv_wr_*`] APIs.
///
/// Every Work Request goes to the destination last given to [`setup_destination`]; posting a Work
/// Request before any destination is set fails with [`PostSendError::InvalidWorkRequest`].
///
/// [`ibv_wr_*`]: https://manpages.debian.org/testing/libibverbs-dev/ibv_wr_post.3.en.html
/// [`setup_destination`]: DcPostSendGuard::setup_destination
///
pub struct DcPostSendGuard<'qp> {
    qp_ex: NonNull<ibv_qp_ex>,
    dv_qp_ex: NonNull<mlx5dv_qp_ex>,
    destination: Option<DcDestination>,
    missing_destination: bool,
    _phantom: PhantomData<&'qp ()>,
}

impl DcPostSendGuard<'_> {
    /// Setup the destination of the next Work Requests.
    pub fn setup_destination(&mut self, destination: &DcDestination) -> &mut Self {
        self.destination = Some(destination.clone());
        self
    }

    /// Set the DC address of the Work Request just started; the provider copies the address
    /// vector into it, so the [`AddressHandle`] only has to outlive this call.
    fn set_dc_addr(&mut self) {
        match &self.destination {
            Some(dest) => unsafe {
                mlx5dv_wr_set_dc_addr(
                    self.dv_qp_ex.as_ptr(),
                    dest.ah.ah().as_ptr(),
                    dest.dctn,
                    dest.access_key,
                )
            },
            None => self.missing_destination = true,
        }
    }
}

impl PostSendGuard for DcPostSendGuard<'_> {
    fn construct_wr(&mut self, wr_id: u64, wr_flags: WorkRequestFlags) -> WorkRequestHandle<'_, Self> {
        unsafe {
            self.qp_ex.as_mut().wr_id = wr_id;
            self.qp_ex.as_mut().wr_flags = wr_flags.bits();
        }
        WorkRequestHandle { guard: self }
    }

    /// Post all previously setuped RDMA Work Requests into the [`DcInitiator`]'s send queue, using
    /// [`ibv_wr_complete`].
    ///
    /// [`ibv_wr_complete`]: https://manpages.debian.org/testing/libibverbs-dev/ibv_wr_post.3.en.html
    ///
    fn post(self) -> Result<(), PostSendError> {
        if self.missing_destination {
            // Dropping self aborts the Work Requests.
            return Err(PostSendError::InvalidWorkRequest(io::Error::from_raw_os_error(
                libc::EINVAL,
            )));
        }
        let ret = unsafe { ibv_wr_complete(self.qp_ex.as_ptr()) };
        std::mem::forget(self);
        wr_complete_result(ret)
    }
}

impl private_traits::PostSendGuard for DcPostSendGuard<'_> {
    fn setup_send(&mut self) {
        unsafe { ibv_wr_send(self.qp_ex.as_ptr()) };
        self.set_dc_addr();
    }

    fn setup_send_imm(&mut self, imm_data: u32) {
        unsafe { ibv_wr_send_imm(self.qp_ex.as_ptr(), imm_data) };
        self.set_dc_addr();
    }

    fn setup_write(&mut self, rkey: u32, remote_addr: u64) {
        unsafe { ibv_wr_rdma_write(self.qp_ex.as_ptr(), rkey, remote_addr) };
        self.set_dc_addr();
    }

    fn setup_write_imm(&mut self, rkey: u32, remote_addr: u64, imm_data: u32) {
        unsafe { ibv_wr_rdma_write_imm(self.qp_ex.as_ptr(), rkey, remote_addr, imm_data) };
        self.set_dc_addr();
    }

    fn setup_read(&mut self, rkey: u32, remote_addr: u64) {
        unsafe { ibv_wr_rdma_read(self.qp_ex.as_ptr(), rkey, remote_addr) };
        self.set_dc_addr();
    }

    fn setup_atomic_compare_swap(&mut self, rkey: u32, remote_addr: u64, compare: u64, swap: u64) {
        unsafe { ibv_wr_atomic_cmp_swp(self.qp_ex.as_ptr(), rkey, remote_addr, compare, swap) };
        self.set_dc_addr();
    }

    fn setup_atomic_fetch_add(&mut self, rkey: u32, remote_addr: u64, add: u64) {
        unsafe { ibv_wr_atomic_fetch_add(self.qp_ex.as_ptr(), rkey, remote_addr, add) };
        self.set_dc_addr();
    }

    fn setup_inline_data(&mut self, buf: &[u8]) {
        unsafe { ibv_wr_set_inline_data(self.qp_ex.as_ptr(), buf.as_ptr() as _, buf.len()) }
    }

    fn setup_inline_data_list(&mut self, bufs: &[IoSlice<'_>]) {
        let buf_list: Vec<ibv_data_buf> = bufs
            .iter()
            .map(|x| ibv_data_buf {
                addr: x.as_ptr() as _,
                length: x.len(),
            })
            .collect();
        unsafe { ibv_wr_set_inline_data_list(self.qp_ex.as_ptr(), buf_list.len(), buf_list.as_ptr()) };
    }

    unsafe fn setup_sge(&mut self, lkey: u32, addr: u64, length: u32) {
        ibv_wr_set_sge(self.qp_ex.as_ptr(), lkey, addr, length);
    }

    unsafe fn setup_sge_list(&mut self, sg_list: &[ibv_sge]) {
        ibv_wr_set_sge_list(self.qp_ex.as_ptr(), sg_list.len(), sg_list.as_ptr());
    }
}

impl Drop for DcPostSendGuard<'_> {
    fn drop(&mut self) {
        unsafe { ibv_wr_abort(self.qp_ex.as_ptr()) };
    }
}

impl ProtectionDomain {
    /// Create a [`DcTargetBuilder`] for building DC targets on this protection domain.
    pub fn create_dc_target_builder(self: &Arc<Self>) -> DcTargetBuilder {
        DcTargetBuilder::new(self)
    }

    /// Create a [`DcInitiatorBuilder`] for building DC initiators on this protection domain.
    pub fn create_dc_initiator_builder(self: &Arc<Self>) -> DcInitiatorBuilder {
        DcInitiatorBuilder::new(self)
    }
}
