//! Extensions specific to NVIDIA mlx5 devices (ConnectX-4 and later), built on the
//! [mlx5 direct verbs](https://github.com/linux-rdma/rdma-core/tree/master/providers/mlx5).
//!
//! The main one is the Dynamically Connected (DC) transport, see [`dc`]: a reliable transport
//! where one [`DcInitiator`] reaches any [`DcTarget`] by naming it in each Work Request, instead
//! of the one QP per peer that RC needs.
//!
//! [`DcInitiator`]: dc::DcInitiator
//! [`DcTarget`]: dc::DcTarget
//!
pub mod dc;
pub mod device_context;
