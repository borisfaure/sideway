#![allow(clippy::while_let_on_iterator)]

use std::time::{Duration, Instant};

use sideway::ibverbs::{
    address::{AddressHandleAttribute, GidEntry, GidType},
    completion::{GenericCompletionQueue, WorkCompletionStatus},
    device::{self, DeviceInfo},
    device_context::{DeviceContext, LinkLayer},
    queue_pair::{PostSendGuard, QueuePairAttribute, QueuePairState, SetScatterGatherEntry, WorkRequestFlags},
    AccessFlags,
};
use sideway::mlx5::dc::{CreateDcQueuePairErrorKind, DcDestination};
use sideway::mlx5::device_context::QueryMlx5DeviceErrorKind;

const PORT: u8 = 1;
const ACCESS_KEY: u64 = 0xffeeddcc;

fn is_mlx5(ctx: &DeviceContext) -> bool {
    match ctx.query_mlx5_device() {
        Ok(attr) => {
            eprintln!("{}: {attr:?}", ctx.name());
            true
        },
        Err(e) => {
            assert!(matches!(e.0, QueryMlx5DeviceErrorKind::NotMlx5(_)), "{e:?}");
            eprintln!("skipping {}: not an mlx5 device", ctx.name());
            false
        },
    }
}

/// The GID a loopback on port 1 addresses itself with: RoCEv2 over RoCEv1 on Ethernet.
fn loopback_gid(ctx: &DeviceContext) -> Option<GidEntry> {
    let gids = ctx.query_gid_table().unwrap();
    let on_port = gids.into_iter().filter(|g| g.port_num() == u32::from(PORT));
    if ctx.query_port(PORT).unwrap().link_layer() == LinkLayer::Ethernet {
        on_port
            .filter(|g| !g.gid().is_unicast_link_local())
            .max_by_key(|g| g.gid_type() == GidType::RoceV2)
    } else {
        on_port.into_iter().next()
    }
}

/// The address vector of port 1, used as the DCT's own route and the DCI's path to it.
fn port_av(ctx: &DeviceContext, gid: &GidEntry) -> AddressHandleAttribute {
    let mut av = AddressHandleAttribute::new();
    av.setup_port(PORT).setup_dest_lid(ctx.query_port(PORT).unwrap().lid());
    if ctx.query_port(PORT).unwrap().link_layer() == LinkLayer::Ethernet {
        av.setup_grh_src_gid_index(gid.gid_index().try_into().unwrap())
            .setup_grh_dest_gid(&gid.gid())
            .setup_grh_hop_limit(64);
    }
    av
}

/// Wait for one completion on `cq` and check it succeeded.
fn wait_completion(cq: &GenericCompletionQueue) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(mut poller) = cq.start_poll() {
            if let Some(wc) = poller.next() {
                assert_eq!(wc.status(), WorkCompletionStatus::Success as u32);
                return;
            }
        }
        assert!(Instant::now() < deadline, "no completion within 5s");
        std::thread::yield_now();
    }
}

#[test]
fn query_mlx5_device_tells_non_mlx5_devices_apart() {
    let device_list = device::DeviceList::new().unwrap();
    for device in &device_list {
        let ctx = device.open().unwrap();
        // asserts the error kind on non-mlx5 devices
        is_mlx5(&ctx);
    }
}

#[test]
fn dc_target_requires_srq() {
    let device_list = device::DeviceList::new().unwrap();
    for device in &device_list {
        let ctx = device.open().unwrap();
        let pd = ctx.alloc_pd().unwrap();
        let cq = ctx.create_cq_builder().setup_cqe(2).build().unwrap();
        let err = pd.create_dc_target_builder().setup_cq(cq).build().unwrap_err();
        assert!(matches!(err.0, CreateDcQueuePairErrorKind::MissingSharedReceiveQueue));
    }
}

/// A DCI writes into, then reads back from, a buffer exposed by a DCT on the same port.
#[test]
fn dc_write_then_read_loopback() {
    let device_list = device::DeviceList::new().unwrap();
    for device in &device_list {
        let ctx = device.open().unwrap();
        if !is_mlx5(&ctx) {
            continue;
        }
        let Some(gid) = loopback_gid(&ctx) else {
            eprintln!("skipping {}: no usable GID on port {PORT}", ctx.name());
            continue;
        };
        let mtu = ctx.query_port(PORT).unwrap().active_mtu();
        let pd = ctx.alloc_pd().unwrap();
        let access = AccessFlags::LocalWrite | AccessFlags::RemoteWrite | AccessFlags::RemoteRead;

        // target side
        let target_buf = vec![0u8; 4096];
        let target_mr = unsafe { pd.reg_mr(target_buf.as_ptr() as _, target_buf.len(), access).unwrap() };
        let target_cq = ctx.create_cq_builder().setup_cqe(2).build().unwrap();
        let srq = pd.create_srq_builder().setup_max_wr(1).build().unwrap();
        let mut dct = pd
            .create_dc_target_builder()
            .setup_cq(target_cq)
            .setup_shared_receive_queue(&srq)
            .setup_access_key(ACCESS_KEY)
            .build()
            .unwrap();

        let mut attr = QueuePairAttribute::new();
        attr.setup_state(QueuePairState::Init)
            .setup_pkey_index(0)
            .setup_port(PORT)
            .setup_access_flags(access);
        dct.modify(&attr).unwrap();
        let mut attr = QueuePairAttribute::new();
        attr.setup_state(QueuePairState::ReadyToReceive)
            .setup_path_mtu(mtu)
            .setup_min_rnr_timer(12)
            .setup_address_vector(&port_av(&ctx, &gid));
        dct.modify(&attr).unwrap();
        assert_eq!(dct.state(), QueuePairState::ReadyToReceive);

        // initiator side
        let cq = GenericCompletionQueue::from(ctx.create_cq_builder().setup_cqe(4).build().unwrap());
        let mut dci = pd.create_dc_initiator_builder().setup_cq(cq.clone()).build().unwrap();
        let mut attr = QueuePairAttribute::new();
        attr.setup_state(QueuePairState::Init)
            .setup_pkey_index(0)
            .setup_port(PORT);
        dci.modify(&attr).unwrap();
        let mut attr = QueuePairAttribute::new();
        // on RoCE the DCI needs the GRH of its source GID too
        attr.setup_state(QueuePairState::ReadyToReceive)
            .setup_path_mtu(mtu)
            .setup_address_vector(&port_av(&ctx, &gid));
        dci.modify(&attr).unwrap();
        let mut attr = QueuePairAttribute::new();
        attr.setup_state(QueuePairState::ReadyToSend)
            .setup_timeout(14)
            .setup_retry_cnt(7)
            .setup_rnr_retry(7)
            .setup_sq_psn(0)
            .setup_max_read_atomic(1);
        dci.modify(&attr).unwrap();
        assert_eq!(dci.state(), QueuePairState::ReadyToSend);

        let ah = pd.create_ah(&port_av(&ctx, &gid)).unwrap();
        let dest = DcDestination::new(ah, dct.dctn(), ACCESS_KEY);

        let mut local = b"dc transport loopback".to_vec();
        let len = local.len() as u32;
        let local_mr = unsafe { pd.reg_mr(local.as_ptr() as _, local.len(), access).unwrap() };

        // RDMA WRITE into the target
        let mut guard = dci.start_post_send();
        guard.setup_destination(&dest);
        let wr = guard.construct_wr(1, WorkRequestFlags::Signaled);
        unsafe {
            wr.setup_write(target_mr.rkey(), target_buf.as_ptr() as u64).setup_sge(
                local_mr.lkey(),
                local.as_ptr() as u64,
                len,
            );
        }
        guard.post().unwrap();
        wait_completion(&cq);
        assert_eq!(&target_buf[..local.len()], &local[..]);

        // RDMA READ it back over a cleared local buffer
        local.fill(0);
        let mut guard = dci.start_post_send();
        guard.setup_destination(&dest);
        let wr = guard.construct_wr(2, WorkRequestFlags::Signaled);
        unsafe {
            wr.setup_read(target_mr.rkey(), target_buf.as_ptr() as u64).setup_sge(
                local_mr.lkey(),
                local.as_ptr() as u64,
                len,
            );
        }
        guard.post().unwrap();
        wait_completion(&cq);
        assert_eq!(&local[..], b"dc transport loopback");
    }
}

/// A Work Request posted before any destination is set is refused rather than sent unaddressed.
#[test]
fn dc_post_without_destination_fails() {
    let device_list = device::DeviceList::new().unwrap();
    for device in &device_list {
        let ctx = device.open().unwrap();
        if !is_mlx5(&ctx) {
            continue;
        }
        let pd = ctx.alloc_pd().unwrap();
        let cq = ctx.create_cq_builder().setup_cqe(2).build().unwrap();
        let mut dci = pd.create_dc_initiator_builder().setup_cq(cq).build().unwrap();
        let mut guard = dci.start_post_send();
        let wr = guard.construct_wr(1, WorkRequestFlags::Signaled);
        unsafe { wr.setup_write(0, 0).setup_sge(0, 0, 0) };
        assert!(guard.post().is_err());
    }
}
