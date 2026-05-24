use super::{
    Candidate, DiscoveryConfig, RouteKind, default_public_bootstrap_peers,
    default_webrtc_ice_servers, descriptor_record_key,
    endpoints::{
        LocalDirectNetworks, descriptor_candidates_for_discovery, direct_endpoints_from_ips,
        discovered_direct_endpoint_candidates,
    },
    now_unix_ms, parse_webrtc_ice_servers_config, provider_record_key, rank_candidates,
    snapshot::DiscoverySnapshot,
    swarm::build_discovery_swarm,
    turn_static_auth_credential,
};
use libp2p::{Multiaddr, PeerId};
use peerline_core::{HumanCode, HumanName, NameCode};
use peerline_rendezvous_model::{
    PeerDescriptor, PublicTunnelEndpoint, RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION,
};
use std::net::{IpAddr, Ipv4Addr};

#[test]
fn ranks_lan_before_relay_and_keeps_distinct_routes() {
    let ranked = rank_candidates(vec![
        Candidate {
            peer_id: "a".into(),
            addresses: vec![],
            route: RouteKind::Libp2pRelay,
        },
        Candidate {
            peer_id: "b".into(),
            addresses: vec![],
            route: RouteKind::LanDirect,
        },
        Candidate {
            peer_id: "a".into(),
            addresses: vec![],
            route: RouteKind::PublicDirect,
        },
    ]);
    assert_eq!(ranked[0].peer_id, "b");
    assert_eq!(ranked.len(), 3);
    assert!(matches!(ranked[1].route, RouteKind::PublicDirect));
    assert!(matches!(ranked[2].route, RouteKind::Libp2pRelay));
}

#[test]
fn rank_candidates_deduplicates_exact_duplicates() {
    let candidate = Candidate {
        peer_id: "a".into(),
        addresses: vec!["/ip4/1.2.3.4/tcp/1".into()],
        route: RouteKind::PublicDirect,
    };
    let ranked = rank_candidates(vec![candidate.clone(), candidate]);
    assert_eq!(ranked.len(), 1);
}

#[test]
fn dht_publish_and_discovery_keys_use_the_same_lookup_key() {
    let name = HumanName::parse("river-mango-42").unwrap();
    let code = HumanCode::parse("rose-lime-iris-jade-1234").unwrap();
    let lookup_key = NameCode::new(name.clone(), code.clone()).lookup_key();
    let receiver_lookup_key = NameCode::new(name, code).lookup_key();

    assert_eq!(lookup_key.hex(), receiver_lookup_key.hex());
    assert_eq!(
        descriptor_record_key(&lookup_key).to_vec(),
        descriptor_record_key(&receiver_lookup_key).to_vec()
    );
    assert_eq!(
        provider_record_key(&lookup_key).to_vec(),
        provider_record_key(&receiver_lookup_key).to_vec()
    );
    assert_eq!(
        String::from_utf8(descriptor_record_key(&lookup_key).to_vec()).unwrap(),
        format!("/peerline/descriptor/v1/{}", lookup_key.hex())
    );
    assert_eq!(
        String::from_utf8(provider_record_key(&lookup_key).to_vec()).unwrap(),
        format!("/peerline/v1/{}", lookup_key.hex())
    );
}

#[test]
fn descriptor_candidates_never_rank_loopback_first() {
    let descriptor = PeerDescriptor {
        protocol_version: RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION,
        peer_id: "peer".into(),
        direct_endpoints: vec![
            "127.0.0.1:43117".into(),
            "203.0.113.7:43117".into(),
            "192.168.1.20:43117".into(),
        ],
        libp2p_endpoints: vec![],
        public_endpoints: vec![],
        published_unix_ms: 0,
    };

    let endpoints = discovered_direct_endpoint_candidates(&descriptor, None, true);
    assert_eq!(endpoints[0], "192.168.1.20:43117".parse().unwrap());
    assert_eq!(endpoints[1], "203.0.113.7:43117".parse().unwrap());
    assert_eq!(endpoints[2], "127.0.0.1:43117".parse().unwrap());
}

#[test]
fn unspecified_bind_advertises_routable_ips_not_loopback_by_default() {
    let endpoints = direct_endpoints_from_ips(
        43117,
        [
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V4(Ipv4Addr::new(169, 254, 10, 1)),
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20)),
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)),
        ],
        false,
    );

    assert_eq!(
        endpoints,
        vec![
            "192.168.1.20:43117".parse().unwrap(),
            "203.0.113.7:43117".parse().unwrap(),
        ]
    );
}

#[test]
fn loopback_discovery_is_explicit_opt_in_and_ranked_last() {
    let endpoints = direct_endpoints_from_ips(
        43117,
        [
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 8)),
        ],
        true,
    );

    assert_eq!(
        endpoints,
        vec![
            "10.0.0.8:43117".parse().unwrap(),
            "127.0.0.1:43117".parse().unwrap(),
        ]
    );
}

#[test]
fn default_bootstrap_peers_are_configured_for_public_dht() {
    assert!(default_public_bootstrap_peers().len() >= 5);
    assert!(
        default_public_bootstrap_peers()
            .iter()
            .all(|addr| addr.starts_with("/dnsaddr/") || addr.starts_with("/ip4/"))
    );
    assert!(
        default_public_bootstrap_peers()
            .iter()
            .all(|addr| addr.parse::<Multiaddr>().is_ok())
    );
}

#[test]
fn default_webrtc_ice_servers_include_stun_and_turn_candidates() {
    let servers = default_webrtc_ice_servers();
    assert_eq!(servers.len(), 2);

    let stun = &servers[0];
    assert!(stun.username.is_empty());
    assert!(stun.credential.is_empty());
    assert_eq!(
        stun.urls,
        vec![
            String::from("stun:stun.l.google.com:19302"),
            String::from("stun:stun1.l.google.com:19302"),
            String::from("stun:stun2.l.google.com:19302"),
            String::from("stun:stun3.l.google.com:19302"),
            String::from("stun:stun4.l.google.com:19302"),
        ]
    );

    let turn = &servers[1];
    assert!(turn.username.parse::<u64>().is_ok());
    assert!(!turn.credential.is_empty());
    assert!(
        turn.urls
            .contains(&"turn:staticauth.openrelay.metered.ca:80?transport=udp".into())
    );
    assert!(
        turn.urls
            .contains(&"turn:staticauth.openrelay.metered.ca:443?transport=udp".into())
    );
    assert!(
        turn.urls
            .contains(&"turn:staticauth.openrelay.metered.ca:443?transport=tcp".into())
    );
    assert!(
        turn.urls
            .contains(&"turns:staticauth.openrelay.metered.ca:443?transport=tcp".into())
    );
}

#[test]
fn turn_static_auth_credentials_use_hmac_sha1() {
    assert_eq!(
        turn_static_auth_credential("openrelayprojectsecret", "1700000000"),
        "DpIWs03rTmTd/9c5XqDD+kE/LoI="
    );
}

#[test]
fn webrtc_ice_servers_config_accepts_json_and_empty_disable_value() {
    let parsed = parse_webrtc_ice_servers_config(
        r#"[{"urls":["turn:turn.example.net:3478?transport=udp"],"username":"user","credential":"pass"}]"#,
    )
    .unwrap();

    assert_eq!(parsed.len(), 1);
    assert_eq!(
        parsed[0].urls,
        vec!["turn:turn.example.net:3478?transport=udp"]
    );
    assert_eq!(parsed[0].username, "user");
    assert_eq!(parsed[0].credential, "pass");
    assert_eq!(parse_webrtc_ice_servers_config(" \n\t ").unwrap(), vec![]);
    assert!(parse_webrtc_ice_servers_config("{").is_err());
}

#[test]
fn mdns_can_be_disabled_for_deterministic_network_tests() {
    let swarm = build_discovery_swarm(false, false, false).unwrap();
    assert!(!swarm.behaviour().mdns.is_enabled());
}

#[test]
fn diversity_floor_requires_observed_peers_and_descriptor() {
    let mut snapshot = DiscoverySnapshot::new();
    snapshot.observed_peers.insert(PeerId::random());
    assert!(!snapshot.is_diverse_enough(2));

    let local_peer = PeerId::random();
    snapshot.observe_local_peer(local_peer);
    snapshot.insert_descriptor(PeerDescriptor {
        protocol_version: RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION,
        peer_id: local_peer.to_string(),
        direct_endpoints: vec!["192.168.1.20:43117".into()],
        libp2p_endpoints: vec![],
        public_endpoints: vec![],
        published_unix_ms: 1,
    });
    assert!(snapshot.is_diverse_enough(3));
}

#[test]
fn discovered_lan_candidates_require_matching_local_network() {
    let descriptor = PeerDescriptor {
        protocol_version: RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION,
        peer_id: "peer".into(),
        direct_endpoints: vec![
            "192.168.1.20:43117".into(),
            "192.168.2.20:43117".into(),
            "203.0.113.7:43117".into(),
        ],
        libp2p_endpoints: vec![],
        public_endpoints: vec![],
        published_unix_ms: 1,
    };
    let networks = LocalDirectNetworks::from_prefixes(
        [(Ipv4Addr::new(192, 168, 1, 9), 24)],
        std::iter::empty(),
    );

    let candidates = descriptor_candidates_for_discovery(
        &descriptor,
        Some(&networks),
        false,
        &Default::default(),
    );
    let addresses = candidates
        .iter()
        .map(|candidate| candidate.addresses[0].as_str())
        .collect::<Vec<_>>();

    assert!(addresses.contains(&"192.168.1.20:43117"));
    assert!(addresses.contains(&"203.0.113.7:43117"));
    assert!(!addresses.contains(&"192.168.2.20:43117"));
}

#[test]
fn remote_loopback_direct_candidates_are_not_discovered() {
    let descriptor = PeerDescriptor {
        protocol_version: RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION,
        peer_id: "peer".into(),
        direct_endpoints: vec!["127.0.0.1:43117".into(), "10.10.0.8:43117".into()],
        libp2p_endpoints: vec![],
        public_endpoints: vec![],
        published_unix_ms: 1,
    };
    let networks =
        LocalDirectNetworks::from_prefixes([(Ipv4Addr::new(10, 10, 0, 4), 24)], std::iter::empty());

    let candidates = descriptor_candidates_for_discovery(
        &descriptor,
        Some(&networks),
        false,
        &Default::default(),
    );

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].addresses, vec!["10.10.0.8:43117"]);
}

#[test]
fn mdns_observed_peers_keep_unverified_lan_candidates() {
    let mut snapshot = DiscoverySnapshot::new();
    let peer = PeerId::random();
    snapshot.observe_local_peer(peer);
    snapshot.insert_descriptor(PeerDescriptor {
        protocol_version: RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION,
        peer_id: peer.to_string(),
        direct_endpoints: vec!["192.168.50.20:43117".into()],
        libp2p_endpoints: vec![],
        public_endpoints: vec![],
        published_unix_ms: 1,
    });
    let networks =
        LocalDirectNetworks::from_prefixes([(Ipv4Addr::new(10, 10, 0, 4), 24)], std::iter::empty());

    let candidates = snapshot.into_candidates(Some(&networks), &Default::default());

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].addresses, vec!["192.168.50.20:43117"]);
}

#[test]
fn empty_descriptors_do_not_count_as_usable_candidates() {
    let mut snapshot = DiscoverySnapshot::new();
    let peer = PeerId::random();
    snapshot.observe_local_peer(peer);
    snapshot.insert_descriptor(PeerDescriptor {
        protocol_version: RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION,
        peer_id: peer.to_string(),
        direct_endpoints: vec![],
        libp2p_endpoints: vec![],
        public_endpoints: vec![],
        published_unix_ms: 1,
    });

    assert!(snapshot.is_diverse_enough(1));
    assert!(!snapshot.has_usable_candidates(None, &Default::default()));
}

#[test]
fn invalid_peer_ids_are_ignored_during_discovery() {
    let mut snapshot = DiscoverySnapshot::new();
    snapshot.insert_descriptor(PeerDescriptor {
        protocol_version: RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION,
        peer_id: "not-a-peer-id".into(),
        direct_endpoints: vec!["192.168.1.20:43117".into()],
        libp2p_endpoints: vec![],
        public_endpoints: vec![],
        published_unix_ms: 1,
    });

    assert!(snapshot.descriptors.is_empty());
    assert!(snapshot.observed_peers.is_empty());
}

#[test]
fn future_timestamps_are_clamped_during_discovery() {
    let mut snapshot = DiscoverySnapshot::new();
    let peer = PeerId::random();
    let before = now_unix_ms();
    snapshot.insert_descriptor(PeerDescriptor {
        protocol_version: RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION,
        peer_id: peer.to_string(),
        direct_endpoints: vec!["192.168.1.20:43117".into()],
        libp2p_endpoints: vec![],
        public_endpoints: vec![],
        published_unix_ms: u64::MAX,
    });
    let after = now_unix_ms();

    let stored = snapshot
        .descriptors
        .get(&peer.to_string())
        .expect("descriptor should be stored");
    assert!(stored.published_unix_ms <= after);
    assert!(stored.published_unix_ms >= before);
}

#[test]
fn discovery_flags_gate_expected_routes() {
    let mut config = DiscoveryConfig::default();
    config.enable_upnp = false;
    config.enable_natpmp_pcp = false;
    config.enable_quic = false;
    config.enable_dcutr = false;
    config.enable_turn = false;
    config.enable_public_tunnels = false;
    config.allow_relay_data_fallback = false;

    assert!(!config.port_mapping_enabled());
    assert!(config.route_enabled(&RouteKind::LanDirect));
    assert!(config.route_enabled(&RouteKind::PublicDirect));
    assert!(config.route_enabled(&RouteKind::WebRtcDirect));
    assert!(!config.route_enabled(&RouteKind::PublicTunnel));
    assert!(!config.route_enabled(&RouteKind::Libp2pQuic));
    assert!(!config.route_enabled(&RouteKind::Libp2pDcutr));
    assert!(!config.route_enabled(&RouteKind::WebRtcTurn));
    assert!(!config.route_enabled(&RouteKind::Libp2pRelay));
}

#[test]
fn public_tunnel_descriptors_become_candidates() {
    let descriptor = PeerDescriptor {
        protocol_version: RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION,
        peer_id: "peer".into(),
        direct_endpoints: vec![],
        libp2p_endpoints: vec![],
        public_endpoints: vec![PublicTunnelEndpoint {
            provider: "cloudflared".into(),
            url: "https://example.com/transfer".into(),
        }],
        published_unix_ms: 1,
    };

    let candidates = super::endpoints::descriptor_candidates_for_discovery(
        &descriptor,
        None,
        false,
        &DiscoveryConfig::default(),
    );

    assert_eq!(candidates.len(), 1);
    assert!(matches!(candidates[0].route, RouteKind::PublicTunnel));
    assert_eq!(candidates[0].addresses, vec!["wss://example.com/transfer"]);
}
