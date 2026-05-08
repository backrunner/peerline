use super::{Libp2pRecvOptions, Libp2pSendOptions, recv_libp2p, send_libp2p};
use crate::discovery::{DiscoveryConfig, RouteKind};
use peerline_core::{Compression, HumanCode, HumanName};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};
use tokio::time;

#[tokio::test]
async fn libp2p_roundtrip_works_without_direct_endpoints() {
    let temp = tempfile::tempdir().unwrap();
    let src_dir = temp.path().join("src");
    let dst_dir = temp.path().join("dst");
    std::fs::create_dir(&src_dir).unwrap();
    std::fs::create_dir(&dst_dir).unwrap();
    std::fs::write(src_dir.join("hello.txt"), "hello libp2p").unwrap();

    let name = HumanName::parse("river-mango-42").unwrap();
    let code = HumanCode::parse("rose-lime-iris-jade-1234").unwrap();
    let discovery = DiscoveryConfig {
        min_candidate_diversity: 1,
        lookup_timeout: Duration::from_secs(20),
        enable_mdns: true,
        allow_loopback_endpoints: false,
        allow_relay_data_fallback: false,
        bootstrap_peers: Vec::new(),
    };

    let recv_task = tokio::spawn(recv_libp2p(Libp2pRecvOptions {
        name: name.clone(),
        code: code.clone(),
        direct_bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        destination: dst_dir.clone(),
        overwrite: false,
        discovery: discovery.clone(),
        events: None,
    }));

    time::sleep(Duration::from_secs(2)).await;

    let candidates = crate::discovery::discover_peer_candidates(&name, &code, discovery.clone())
        .await
        .unwrap();
    let candidate = candidates
        .into_iter()
        .find(|candidate| {
            !matches!(
                candidate.route,
                RouteKind::LanDirect | RouteKind::PublicDirect
            )
        })
        .expect("expected a libp2p candidate");

    let peer_id = candidate.peer_id.parse().unwrap();
    let addresses = candidate
        .addresses
        .iter()
        .map(|address| address.parse())
        .collect::<Result<Vec<libp2p::Multiaddr>, _>>()
        .unwrap();

    let sent = send_libp2p(Libp2pSendOptions {
        peer_id,
        addresses,
        name: name.clone(),
        code: code.clone(),
        paths: vec![src_dir.join("hello.txt")],
        compression: Compression::None,
        route_label: format!("{:?}", candidate.route),
    })
    .await
    .unwrap();
    let received = recv_task.await.unwrap().unwrap();
    assert_eq!(
        std::fs::read_to_string(dst_dir.join("hello.txt")).unwrap(),
        "hello libp2p"
    );
    assert_eq!(sent.files, 1);
    assert_eq!(received.files, 1);
    assert_eq!(received.bytes, sent.bytes);
}
