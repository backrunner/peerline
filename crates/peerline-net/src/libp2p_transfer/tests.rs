use super::{Libp2pRecvOptions, Libp2pSendOptions, recv_libp2p, send_libp2p};
use crate::discovery::{DiscoveryConfig, RouteKind};
use crate::rendezvous::RendezvousConfig;
use futures::StreamExt;
use libp2p::{multiaddr::Protocol, swarm::SwarmEvent};
use peerline_core::{Compression, HumanCode, HumanName, NodeId, PeerlineEvent};
use peerline_transfer::create_archive;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};
use tokio::time;

async fn spawn_bootstrap_peer() -> (String, tokio::task::JoinHandle<()>) {
    let mut swarm = super::behaviour::build_receiver_swarm(false).await.unwrap();
    swarm
        .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .unwrap();

    let listen_addr = loop {
        if let SwarmEvent::NewListenAddr { address, .. } = swarm.select_next_some().await {
            break address;
        }
    };

    let peer_id = *swarm.local_peer_id();
    let bootstrap_addr = listen_addr.with(Protocol::P2p(peer_id)).to_string();
    let handle = tokio::spawn(async move {
        let mut swarm = swarm;
        loop {
            let _ = swarm.select_next_some().await;
        }
    });

    (bootstrap_addr, handle)
}

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
    let source_id = peerline_core::NodeId::random();
    let (bootstrap_peer, bootstrap_handle) = spawn_bootstrap_peer().await;
    let discovery = DiscoveryConfig {
        min_candidate_diversity: 1,
        lookup_timeout: Duration::from_secs(5),
        enable_mdns: false,
        allow_loopback_endpoints: false,
        allow_relay_data_fallback: false,
        bootstrap_peers: vec![bootstrap_peer],
        rendezvous: RendezvousConfig::disabled(),
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

    let candidate = time::timeout(Duration::from_secs(60), async {
        loop {
            let candidates =
                crate::discovery::discover_peer_candidates(&name, &code, discovery.clone())
                    .await
                    .unwrap();
            if let Some(candidate) = candidates.into_iter().find(|candidate| {
                !matches!(
                    candidate.route,
                    RouteKind::LanDirect | RouteKind::PublicDirect
                )
            }) {
                break candidate;
            }
            time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("expected a libp2p candidate within timeout");

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
        source_id,
        paths: vec![src_dir.join("hello.txt")],
        compression: Compression::None,
        route: candidate.route.connection_route(),
        events: None,
    })
    .await
    .unwrap();
    let received = recv_task.await.unwrap().unwrap();
    bootstrap_handle.abort();
    let _ = bootstrap_handle.await;
    assert_eq!(
        std::fs::read_to_string(dst_dir.join("hello.txt")).unwrap(),
        "hello libp2p"
    );
    assert_eq!(sent.files, 1);
    assert_eq!(received.files, 1);
    assert_eq!(received.bytes, sent.bytes);
}

#[tokio::test]
async fn libp2p_resumes_after_sender_disconnects_mid_transfer() {
    let temp = tempfile::tempdir().unwrap();
    let src_dir = temp.path().join("src");
    let dst_dir = temp.path().join("dst");
    std::fs::create_dir(&src_dir).unwrap();
    std::fs::create_dir(&dst_dir).unwrap();
    std::fs::write(src_dir.join("large.bin"), vec![7u8; 2 * 1024 * 1024]).unwrap();

    let name = HumanName::parse("river-mango-42").unwrap();
    let code = HumanCode::parse("rose-lime-iris-jade-1234").unwrap();
    let source_id = NodeId::random();
    let archive_one = create_archive(
        std::slice::from_ref(&src_dir.join("large.bin")),
        Compression::None,
    )
    .unwrap();
    let archive_two = create_archive(
        std::slice::from_ref(&src_dir.join("large.bin")),
        Compression::None,
    )
    .unwrap();

    let (bootstrap_peer, bootstrap_handle) = spawn_bootstrap_peer().await;
    let discovery = DiscoveryConfig {
        min_candidate_diversity: 1,
        lookup_timeout: Duration::from_secs(5),
        enable_mdns: false,
        allow_loopback_endpoints: false,
        allow_relay_data_fallback: false,
        bootstrap_peers: vec![bootstrap_peer],
        rendezvous: RendezvousConfig::disabled(),
    };

    let (events, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let recv_task = tokio::spawn(recv_libp2p(Libp2pRecvOptions {
        name: name.clone(),
        code: code.clone(),
        direct_bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        destination: dst_dir.clone(),
        overwrite: false,
        discovery: discovery.clone(),
        events: Some(events),
    }));

    let candidate = time::timeout(Duration::from_secs(60), async {
        loop {
            let candidates =
                crate::discovery::discover_peer_candidates(&name, &code, discovery.clone())
                    .await
                    .unwrap();
            if let Some(candidate) = candidates.into_iter().find(|candidate| {
                !matches!(
                    candidate.route,
                    RouteKind::LanDirect | RouteKind::PublicDirect
                )
            }) {
                break candidate;
            }
            time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("expected a libp2p candidate within timeout");

    let peer_id = candidate.peer_id.parse().unwrap();
    let addresses = candidate
        .addresses
        .iter()
        .map(|address| address.parse())
        .collect::<Result<Vec<libp2p::Multiaddr>, _>>()
        .unwrap();
    let route = candidate.route.connection_route();

    let first_send = tokio::spawn(super::send_prebuilt_libp2p(
        Libp2pSendOptions {
            peer_id,
            addresses: addresses.clone(),
            name: name.clone(),
            code: code.clone(),
            source_id,
            paths: vec![src_dir.join("large.bin")],
            compression: Compression::None,
            route: route.clone(),
            events: None,
        },
        archive_one,
        peerline_core::TransferId::random(),
    ));

    loop {
        let event = time::timeout(Duration::from_secs(30), event_rx.recv())
            .await
            .expect("expected receiver events")
            .expect("event channel closed unexpectedly");
        match event {
            PeerlineEvent::Progress {
                bytes_done,
                bytes_total,
                ..
            } if bytes_done > 0 && bytes_done < bytes_total => {
                break;
            }
            _ => {}
        }
    }
    first_send.abort();
    let _ = first_send.await;
    time::timeout(Duration::from_secs(30), async {
        loop {
            if dst_dir.join(".peerline-resume").exists() {
                break;
            }
            time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("expected partial resume cache after disconnect");

    let second_send = tokio::spawn(super::send_prebuilt_libp2p(
        Libp2pSendOptions {
            peer_id,
            addresses,
            name: name.clone(),
            code: code.clone(),
            source_id,
            paths: vec![src_dir.join("large.bin")],
            compression: Compression::None,
            route,
            events: None,
        },
        archive_two,
        peerline_core::TransferId::random(),
    ));
    let (received, sent) = tokio::join!(
        async {
            time::timeout(Duration::from_secs(60), recv_task)
                .await
                .expect("receiver should finish")
                .expect("receiver task should succeed")
                .unwrap()
        },
        async {
            time::timeout(Duration::from_secs(60), second_send)
                .await
                .expect("second sender should finish")
                .expect("second sender task should succeed")
                .unwrap()
        }
    );

    bootstrap_handle.abort();
    let _ = bootstrap_handle.await;

    let mut events = Vec::new();
    while let Ok(event) = event_rx.try_recv() {
        events.push(event);
    }

    assert_eq!(
        std::fs::read(dst_dir.join("large.bin")).unwrap(),
        vec![7u8; 2 * 1024 * 1024]
    );
    assert_eq!(sent.files, 1);
    assert_eq!(received.files, 1);
    assert!(!dst_dir.join(".peerline-resume").exists());
    assert!(events.iter().any(|event| matches!(
        event,
        PeerlineEvent::TransferStarted {
            resume_offset,
            ..
        } if *resume_offset > 0
    )));
}
