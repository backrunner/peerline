mod endpoints;
mod snapshot;
mod swarm;

#[cfg(test)]
mod tests;

use crate::rendezvous::{self, RendezvousConfig};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use endpoints::LocalDirectNetworks;
use futures::StreamExt;
use hmac::{Hmac, Mac};
use peerline_core::{ConnectionRoute, HumanCode, HumanName, LookupKey, NameCode};
use peerline_rendezvous_model::{
    PeerDescriptor, PublicTunnelEndpoint, RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION, TorOnionEndpoint,
};
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use snapshot::DiscoverySnapshot;
use std::{
    future,
    net::SocketAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use swarm::{
    apply_bootstrap, build_discovery_swarm, handle_discovery_swarm_event,
    handle_publish_swarm_event, publish_descriptor,
};
use tokio::{task::JoinHandle, time};

pub(crate) use endpoints::direct_endpoints_with_extra;
pub use endpoints::rank_candidates;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RouteKind {
    LanDirect,
    PublicDirect,
    PublicTunnel,
    TorOnion,
    Libp2pQuic,
    Libp2pDcutr,
    Libp2pRelay,
    WebRtcDirect,
    WebRtcTurn,
}

impl RouteKind {
    pub fn connection_route(&self) -> ConnectionRoute {
        match self {
            RouteKind::LanDirect => ConnectionRoute::LanDirect,
            RouteKind::PublicDirect => ConnectionRoute::PublicDirect,
            RouteKind::PublicTunnel => ConnectionRoute::PublicTunnel,
            RouteKind::TorOnion => ConnectionRoute::TorOnion,
            RouteKind::Libp2pQuic => ConnectionRoute::Libp2pQuic,
            RouteKind::Libp2pDcutr => ConnectionRoute::Libp2pDcutr,
            RouteKind::Libp2pRelay => ConnectionRoute::Libp2pRelay,
            RouteKind::WebRtcDirect => ConnectionRoute::WebRtcDirect,
            RouteKind::WebRtcTurn => ConnectionRoute::WebRtcTurn,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Candidate {
    pub peer_id: String,
    pub addresses: Vec<String>,
    pub route: RouteKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebRtcIceServer {
    pub urls: Vec<String>,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub credential: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveryConfig {
    pub min_candidate_diversity: usize,
    pub lookup_timeout: Duration,
    pub enable_mdns: bool,
    pub enable_upnp: bool,
    pub enable_natpmp_pcp: bool,
    pub enable_quic: bool,
    pub enable_dcutr: bool,
    pub enable_turn: bool,
    pub enable_public_tunnels: bool,
    pub enable_tor: bool,
    pub tor_socks_proxy: SocketAddr,
    pub allow_loopback_endpoints: bool,
    pub allow_relay_data_fallback: bool,
    pub bootstrap_peers: Vec<String>,
    pub relay_peers: Vec<String>,
    pub webrtc_ice_servers: Vec<WebRtcIceServer>,
    pub rendezvous: RendezvousConfig,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        let bootstrap_peers = peers_from_env("PEERLINE_BOOTSTRAP").unwrap_or_else(|| {
            default_public_bootstrap_peers()
                .iter()
                .map(|addr| (*addr).into())
                .collect()
        });
        let relay_peers =
            peers_from_env("PEERLINE_RELAY_PEERS").unwrap_or_else(|| bootstrap_peers.clone());
        let mut webrtc_ice_servers =
            webrtc_ice_servers_from_env().unwrap_or_else(default_webrtc_ice_servers);
        let enable_turn = env_flag("PEERLINE_DISABLE_TURN").is_none();
        if !enable_turn {
            webrtc_ice_servers = without_turn_ice_servers(&webrtc_ice_servers);
        }
        Self {
            // Prefer the first usable descriptor so named send feels immediate.
            min_candidate_diversity: 1,
            lookup_timeout: Duration::from_secs(15),
            enable_mdns: env_flag("PEERLINE_DISABLE_MDNS").is_none(),
            enable_upnp: env_flag("PEERLINE_DISABLE_UPNP").is_none(),
            enable_natpmp_pcp: env_flag("PEERLINE_DISABLE_NATPMP_PCP").is_none(),
            enable_quic: env_flag("PEERLINE_DISABLE_QUIC").is_none(),
            enable_dcutr: env_flag("PEERLINE_DISABLE_DCUTR").is_none(),
            enable_turn,
            enable_public_tunnels: env_flag("PEERLINE_DISABLE_PUBLIC_TUNNELS").is_none(),
            enable_tor: env_flag("PEERLINE_DISABLE_TOR").is_none(),
            tor_socks_proxy: SocketAddr::from(([127, 0, 0, 1], 9050)),
            allow_loopback_endpoints: env_flag("PEERLINE_ALLOW_LOOPBACK_DISCOVERY").is_some(),
            allow_relay_data_fallback: env_flag("PEERLINE_DISABLE_RELAY_FALLBACK").is_none(),
            bootstrap_peers,
            relay_peers,
            webrtc_ice_servers,
            rendezvous: RendezvousConfig::default(),
        }
    }
}

impl DiscoveryConfig {
    pub fn port_mapping_enabled(&self) -> bool {
        self.enable_upnp || self.enable_natpmp_pcp
    }

    pub fn route_enabled(&self, route: &RouteKind) -> bool {
        match route {
            RouteKind::PublicTunnel => self.enable_public_tunnels,
            RouteKind::TorOnion => self.enable_tor,
            RouteKind::Libp2pQuic => self.enable_quic,
            RouteKind::Libp2pDcutr => self.enable_dcutr,
            RouteKind::WebRtcTurn => self.enable_turn,
            RouteKind::Libp2pRelay => self.allow_relay_data_fallback,
            RouteKind::LanDirect | RouteKind::PublicDirect | RouteKind::WebRtcDirect => true,
        }
    }
}

pub fn default_webrtc_ice_servers() -> Vec<WebRtcIceServer> {
    let mut servers = vec![google_stun_servers()];
    servers.extend(static_auth_turn_servers(
        "staticauth.openrelay.metered.ca",
        "openrelayprojectsecret",
        Duration::from_secs(24 * 60 * 60),
    ));
    servers
}

pub fn without_turn_ice_servers(servers: &[WebRtcIceServer]) -> Vec<WebRtcIceServer> {
    servers
        .iter()
        .filter_map(|server| {
            let urls = server
                .urls
                .iter()
                .filter(|url| !is_turn_url(url))
                .cloned()
                .collect::<Vec<_>>();
            (!urls.is_empty()).then(|| WebRtcIceServer {
                urls,
                username: server.username.clone(),
                credential: server.credential.clone(),
            })
        })
        .collect()
}

fn is_turn_url(url: &str) -> bool {
    let lower = url.to_ascii_lowercase();
    lower.starts_with("turn:") || lower.starts_with("turns:")
}

fn google_stun_servers() -> WebRtcIceServer {
    WebRtcIceServer {
        urls: vec![
            "stun:stun.l.google.com:19302".into(),
            "stun:stun1.l.google.com:19302".into(),
            "stun:stun2.l.google.com:19302".into(),
            "stun:stun3.l.google.com:19302".into(),
            "stun:stun4.l.google.com:19302".into(),
        ],
        username: String::new(),
        credential: String::new(),
    }
}

fn static_auth_turn_servers(host: &str, secret: &str, ttl: Duration) -> Vec<WebRtcIceServer> {
    let expires = SystemTime::now()
        .checked_add(ttl)
        .unwrap_or_else(SystemTime::now)
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let username = expires.to_string();
    let credential = turn_static_auth_credential(secret, &username);
    vec![WebRtcIceServer {
        urls: vec![
            format!("turn:{host}:80?transport=udp"),
            format!("turn:{host}:443?transport=udp"),
            format!("turn:{host}:443?transport=tcp"),
            format!("turns:{host}:443?transport=tcp"),
        ],
        username,
        credential,
    }]
}

fn turn_static_auth_credential(secret: &str, username: &str) -> String {
    type HmacSha1 = Hmac<Sha1>;
    let mut mac =
        HmacSha1::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(username.as_bytes());
    BASE64_STANDARD.encode(mac.finalize().into_bytes())
}

pub fn default_public_bootstrap_peers() -> &'static [&'static str] {
    &[
        "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
        "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
        "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
        "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
        "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
    ]
}

pub fn provider_record_key(lookup_key: &LookupKey) -> libp2p::kad::RecordKey {
    libp2p::kad::RecordKey::new(&format!("/peerline/v1/{}", lookup_key.hex()))
}

pub fn descriptor_record_key(lookup_key: &LookupKey) -> libp2p::kad::RecordKey {
    libp2p::kad::RecordKey::new(&format!("/peerline/descriptor/v1/{}", lookup_key.hex()))
}

pub(crate) fn make_peer_descriptor(
    peer_id: impl Into<String>,
    direct_endpoints: Vec<String>,
    libp2p_endpoints: Vec<String>,
    public_endpoints: Vec<PublicTunnelEndpoint>,
    tor_endpoints: Vec<TorOnionEndpoint>,
) -> PeerDescriptor {
    PeerDescriptor {
        protocol_version: RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION,
        peer_id: peer_id.into(),
        direct_endpoints,
        libp2p_endpoints,
        public_endpoints,
        tor_endpoints,
        published_unix_ms: now_unix_ms(),
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub struct DiscoveryHandle {
    join: JoinHandle<()>,
}

impl DiscoveryHandle {
    pub async fn shutdown(mut self) {
        self.join.abort();
        let _ = (&mut self.join).await;
    }
}

impl Drop for DiscoveryHandle {
    fn drop(&mut self) {
        self.join.abort();
    }
}

pub fn spawn_descriptor_publisher(
    name: HumanName,
    code: HumanCode,
    direct_bind: SocketAddr,
    config: DiscoveryConfig,
) -> DiscoveryHandle {
    let join = tokio::spawn(async move {
        if let Err(error) = run_descriptor_publisher(name, code, direct_bind, config).await {
            tracing::warn!(%error, "libp2p descriptor publisher stopped");
        }
    });
    DiscoveryHandle { join }
}

pub async fn discover_direct_endpoint(
    name: &HumanName,
    code: &HumanCode,
    config: DiscoveryConfig,
) -> anyhow::Result<Option<SocketAddr>> {
    let local_networks = LocalDirectNetworks::current();
    Ok(
        discover_peer_descriptors(name, code, config, local_networks.as_ref())
            .await?
            .and_then(|snapshot| {
                snapshot
                    .best_direct_endpoints(local_networks.as_ref())
                    .into_iter()
                    .next()
            }),
    )
}

pub async fn discover_direct_endpoints(
    name: &HumanName,
    code: &HumanCode,
    config: DiscoveryConfig,
) -> anyhow::Result<Vec<SocketAddr>> {
    let local_networks = LocalDirectNetworks::current();
    Ok(
        discover_peer_descriptors(name, code, config, local_networks.as_ref())
            .await?
            .map(|snapshot| snapshot.best_direct_endpoints(local_networks.as_ref()))
            .unwrap_or_default(),
    )
}

pub async fn discover_peer_candidates(
    name: &HumanName,
    code: &HumanCode,
    config: DiscoveryConfig,
) -> anyhow::Result<Vec<Candidate>> {
    let local_networks = LocalDirectNetworks::current();
    let route_config = config.clone();
    Ok(
        discover_peer_descriptors(name, code, config, local_networks.as_ref())
            .await?
            .map(|snapshot| snapshot.into_candidates(local_networks.as_ref(), &route_config))
            .unwrap_or_default(),
    )
}

pub async fn discover_peer_descriptor(
    name: &HumanName,
    code: &HumanCode,
    config: DiscoveryConfig,
) -> anyhow::Result<Option<PeerDescriptor>> {
    let local_networks = LocalDirectNetworks::current();
    let route_config = config.clone();
    Ok(
        discover_peer_descriptors(name, code, config, local_networks.as_ref())
            .await?
            .and_then(|snapshot| snapshot.best_descriptor(local_networks.as_ref(), &route_config)),
    )
}

async fn discover_peer_descriptors(
    name: &HumanName,
    code: &HumanCode,
    config: DiscoveryConfig,
    local_networks: Option<&LocalDirectNetworks>,
) -> anyhow::Result<Option<DiscoverySnapshot>> {
    let name_code = NameCode::new(name.clone(), code.clone());
    let lookup_key = name_code.lookup_key();
    let descriptor_key = descriptor_record_key(&lookup_key);
    let provider_key = provider_record_key(&lookup_key);
    let mut snapshot = DiscoverySnapshot::new();
    let rendezvous_name = name.clone();
    let rendezvous_code = code.clone();
    let rendezvous_config = config.rendezvous.clone();
    let rendezvous_lookup = async move {
        rendezvous::discover_peer_descriptors(
            &rendezvous_name,
            &rendezvous_code,
            &rendezvous_config,
        )
        .await
    };
    tokio::pin!(rendezvous_lookup);
    let mut rendezvous_done = false;

    let mut swarm = build_discovery_swarm(false, config.enable_mdns, config.enable_upnp)?;
    apply_bootstrap(&mut swarm, &config);
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    let mut query_interval = time::interval(Duration::from_millis(750));
    let deadline = time::sleep(config.lookup_timeout);
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            _ = &mut deadline => break,
            result = &mut rendezvous_lookup, if !rendezvous_done => {
                rendezvous_done = true;
                match result {
                    Ok(descriptors) => {
                        if descriptors.is_empty() {
                            tracing::debug!("rendezvous discovery returned no descriptors");
                        } else {
                            tracing::info!(
                                count = descriptors.len(),
                                "rendezvous discovery returned descriptors"
                            );
                        }
                        for descriptor in descriptors {
                            snapshot.insert_descriptor(descriptor);
                        }
                        if snapshot.is_diverse_enough(config.min_candidate_diversity)
                            && snapshot.has_usable_candidates(local_networks, &config)
                        {
                            break;
                        }
                    }
                    Err(error) => {
                        tracing::debug!(%error, "rendezvous discovery failed");
                    }
                }
            }
            _ = query_interval.tick() => {
                let _ = swarm.behaviour_mut().kad.get_providers(provider_key.clone());
                let _ = swarm.behaviour_mut().kad.get_record(descriptor_key.clone());
            }
            event = swarm.select_next_some() => {
                handle_discovery_swarm_event(&mut swarm, &mut snapshot, event);
                if snapshot.is_diverse_enough(config.min_candidate_diversity)
                    && snapshot.has_usable_candidates(local_networks, &config)
                {
                    break;
                }
            }
        }
    }

    if snapshot.is_diverse_enough(config.min_candidate_diversity)
        && snapshot.has_usable_candidates(local_networks, &config)
    {
        Ok(Some(snapshot))
    } else {
        Ok(None)
    }
}

async fn run_descriptor_publisher(
    name: HumanName,
    code: HumanCode,
    direct_bind: SocketAddr,
    config: DiscoveryConfig,
) -> anyhow::Result<()> {
    let mut swarm = build_discovery_swarm(true, config.enable_mdns, config.enable_upnp)?;
    apply_bootstrap(&mut swarm, &config);
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    if config.enable_quic {
        swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
    }

    let name_code = NameCode::new(name, code);
    let record_key = descriptor_record_key(&name_code.lookup_key());
    let provider_key = provider_record_key(&name_code.lookup_key());
    let allow_loopback = config.allow_loopback_endpoints;
    let publish_interval = Duration::from_secs(60);
    let mut interval = time::interval_at(time::Instant::now() + publish_interval, publish_interval);
    let direct_mapping = config.port_mapping_enabled().then(|| {
        crate::direct::spawn_direct_port_mapping(
            direct_bind,
            crate::direct::DirectPortMappingConfig {
                enable_upnp: config.enable_upnp,
                enable_natpmp_pcp: config.enable_natpmp_pcp,
            },
        )
    });
    let mut direct_mapping_rx = direct_mapping
        .as_ref()
        .map(crate::direct::DirectPortMapping::subscribe);
    let mut mapped_direct_endpoints = direct_mapping
        .as_ref()
        .map(crate::direct::DirectPortMapping::endpoints)
        .unwrap_or_default();

    let descriptor = publish_descriptor(
        &mut swarm,
        record_key.clone(),
        provider_key.clone(),
        direct_bind,
        &mapped_direct_endpoints,
        allow_loopback,
    )?;
    let _rendezvous_registration = rendezvous::RendezvousRegistrationGuard::new(
        name_code.name.clone(),
        name_code.code.clone(),
        descriptor,
        config.rendezvous.clone(),
    );

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let descriptor = publish_descriptor(
                    &mut swarm,
                    record_key.clone(),
                    provider_key.clone(),
                    direct_bind,
                    &mapped_direct_endpoints,
                    allow_loopback,
                )?;
                _rendezvous_registration.update_descriptor(descriptor);
            }
            event = swarm.select_next_some() => {
                if handle_publish_swarm_event(&mut swarm, event)
                    && let Ok(descriptor) = publish_descriptor(
                        &mut swarm,
                        record_key.clone(),
                        provider_key.clone(),
                        direct_bind,
                        &mapped_direct_endpoints,
                        allow_loopback,
                    )
                {
                    _rendezvous_registration.update_descriptor(descriptor);
                }
            }
            endpoints = wait_for_direct_mapping_change(&mut direct_mapping_rx) => {
                if let Some(endpoints) = endpoints {
                    mapped_direct_endpoints = endpoints;
                    if let Ok(descriptor) = publish_descriptor(
                        &mut swarm,
                        record_key.clone(),
                        provider_key.clone(),
                        direct_bind,
                        &mapped_direct_endpoints,
                        allow_loopback,
                    ) {
                        _rendezvous_registration.update_descriptor(descriptor);
                    }
                }
            }
        }
    }
}

async fn wait_for_direct_mapping_change(
    receiver: &mut Option<tokio::sync::watch::Receiver<Vec<SocketAddr>>>,
) -> Option<Vec<SocketAddr>> {
    let Some(receiver) = receiver.as_mut() else {
        future::pending::<()>().await;
        return None;
    };
    receiver.changed().await.ok()?;
    Some(receiver.borrow().clone())
}

fn peers_from_env(name: &str) -> Option<Vec<String>> {
    std::env::var(name).ok().map(|raw| {
        raw.split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    })
}

fn webrtc_ice_servers_from_env() -> Option<Vec<WebRtcIceServer>> {
    let raw = std::env::var("PEERLINE_WEBRTC_ICE_SERVERS").ok()?;
    match parse_webrtc_ice_servers_config(&raw) {
        Ok(servers) => Some(servers),
        Err(error) => {
            tracing::warn!(%error, "ignoring invalid PEERLINE_WEBRTC_ICE_SERVERS JSON");
            None
        }
    }
}

fn parse_webrtc_ice_servers_config(raw: &str) -> Result<Vec<WebRtcIceServer>, serde_json::Error> {
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_str(raw)
}

fn env_flag(name: &str) -> Option<()> {
    match std::env::var(name).ok()?.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(()),
        _ => None,
    }
}
