use crate::rendezvous::{self, RendezvousConfig};
use futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, Swarm, SwarmBuilder, identify, kad, mdns, noise, ping,
    swarm::{NetworkBehaviour, SwarmEvent, behaviour::toggle::Toggle},
    yamux,
};
use peerline_core::{ConnectionRoute, HumanCode, HumanName, LookupKey, NameCode};
use peerline_rendezvous_model::PeerDescriptor;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::{
    net::{IpAddr, SocketAddr},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{task::JoinHandle, time};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RouteKind {
    LanDirect,
    PublicDirect,
    Libp2pDcutr,
    Libp2pRelay,
    WebRtcTurn,
}

impl RouteKind {
    pub fn connection_route(&self) -> ConnectionRoute {
        match self {
            RouteKind::LanDirect => ConnectionRoute::LanDirect,
            RouteKind::PublicDirect => ConnectionRoute::PublicDirect,
            RouteKind::Libp2pDcutr => ConnectionRoute::Libp2pDcutr,
            RouteKind::Libp2pRelay => ConnectionRoute::Libp2pRelay,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveryConfig {
    pub min_candidate_diversity: usize,
    pub lookup_timeout: Duration,
    pub enable_mdns: bool,
    pub allow_loopback_endpoints: bool,
    pub allow_relay_data_fallback: bool,
    pub bootstrap_peers: Vec<String>,
    pub rendezvous: RendezvousConfig,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            // Prefer the first usable descriptor so named send feels immediate.
            min_candidate_diversity: 1,
            lookup_timeout: Duration::from_secs(15),
            enable_mdns: env_flag("PEERLINE_DISABLE_MDNS").is_none(),
            allow_loopback_endpoints: env_flag("PEERLINE_ALLOW_LOOPBACK_DISCOVERY").is_some(),
            allow_relay_data_fallback: false,
            bootstrap_peers: bootstrap_peers_from_env().unwrap_or_else(|| {
                default_public_bootstrap_peers()
                    .iter()
                    .map(|addr| (*addr).into())
                    .collect()
            }),
            rendezvous: RendezvousConfig::default(),
        }
    }
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
    protocol_version: u16,
    peer_id: impl Into<String>,
    direct_endpoints: Vec<String>,
    libp2p_endpoints: Vec<String>,
) -> PeerDescriptor {
    PeerDescriptor {
        protocol_version,
        peer_id: peer_id.into(),
        direct_endpoints,
        libp2p_endpoints,
        published_unix_ms: now_unix_ms(),
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) fn direct_endpoint_candidates(descriptor: &PeerDescriptor) -> Vec<SocketAddr> {
    let mut endpoints = descriptor
        .direct_endpoints
        .iter()
        .filter_map(|endpoint| endpoint.parse().ok())
        .filter(|endpoint: &SocketAddr| is_usable_endpoint_ip(&endpoint.ip(), true))
        .collect::<Vec<_>>();
    endpoints.sort_by_key(direct_endpoint_priority);
    endpoints.dedup();
    endpoints
}

pub(crate) fn libp2p_endpoint_candidates(descriptor: &PeerDescriptor) -> Vec<Multiaddr> {
    let mut endpoints = descriptor
        .libp2p_endpoints
        .iter()
        .filter_map(|endpoint| endpoint.parse().ok())
        .filter(is_dialable_multiaddr)
        .collect::<Vec<_>>();
    endpoints.sort_by_key(libp2p_endpoint_priority);
    endpoints.dedup();
    endpoints
}

pub(crate) fn descriptor_candidates(descriptor: &PeerDescriptor) -> Vec<Candidate> {
    let mut candidates = Vec::new();
    let peer_id = descriptor.peer_id.clone();
    let direct = direct_endpoint_candidates(descriptor)
        .into_iter()
        .map(|endpoint| Candidate {
            peer_id: peer_id.clone(),
            addresses: vec![endpoint.to_string()],
            route: route_kind_from_direct_endpoint(&endpoint),
        });
    candidates.extend(direct);

    let libp2p = libp2p_endpoint_candidates(descriptor)
        .into_iter()
        .map(|addr| Candidate {
            peer_id: peer_id.clone(),
            addresses: vec![addr.to_string()],
            route: route_kind_from_multiaddr(&addr),
        });
    candidates.extend(libp2p);
    rank_candidates(candidates)
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

#[derive(NetworkBehaviour)]
#[behaviour(prelude = "libp2p::swarm::derive_prelude")]
struct DiscoveryBehaviour {
    kad: kad::Behaviour<kad::store::MemoryStore>,
    mdns: Toggle<mdns::tokio::Behaviour>,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
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
    Ok(discover_peer_descriptor(name, code, config)
        .await?
        .and_then(|descriptor| direct_endpoint_candidates(&descriptor).into_iter().next()))
}

pub async fn discover_direct_endpoints(
    name: &HumanName,
    code: &HumanCode,
    config: DiscoveryConfig,
) -> anyhow::Result<Vec<SocketAddr>> {
    Ok(discover_peer_descriptor(name, code, config)
        .await?
        .map(|descriptor| direct_endpoint_candidates(&descriptor))
        .unwrap_or_default())
}

pub async fn discover_peer_candidates(
    name: &HumanName,
    code: &HumanCode,
    config: DiscoveryConfig,
) -> anyhow::Result<Vec<Candidate>> {
    Ok(discover_peer_descriptors(name, code, config)
        .await?
        .map(|snapshot| snapshot.into_candidates())
        .unwrap_or_default())
}

pub async fn discover_peer_descriptor(
    name: &HumanName,
    code: &HumanCode,
    config: DiscoveryConfig,
) -> anyhow::Result<Option<PeerDescriptor>> {
    Ok(discover_peer_descriptors(name, code, config)
        .await?
        .and_then(|snapshot| snapshot.best_descriptor()))
}

struct DiscoverySnapshot {
    observed_peers: HashSet<PeerId>,
    local_peer_ids: HashSet<String>,
    descriptors: HashMap<String, PeerDescriptor>,
}

impl DiscoverySnapshot {
    fn new() -> Self {
        Self {
            observed_peers: HashSet::new(),
            local_peer_ids: HashSet::new(),
            descriptors: HashMap::new(),
        }
    }

    fn is_diverse_enough(&self, minimum: usize) -> bool {
        if self.descriptors.is_empty() {
            return false;
        }
        self.has_local_descriptor() || self.observed_peers.len() >= minimum
    }

    fn observe_local_peer(&mut self, peer: PeerId) {
        self.local_peer_ids.insert(peer.to_string());
        self.observed_peers.insert(peer);
    }

    fn has_local_descriptor(&self) -> bool {
        self.descriptors
            .keys()
            .any(|peer_id| self.local_peer_ids.contains(peer_id))
    }

    fn insert_descriptor(&mut self, descriptor: PeerDescriptor) {
        let Ok(peer_id) = descriptor.peer_id.parse::<PeerId>() else {
            return;
        };
        let mut descriptor = descriptor;
        descriptor.published_unix_ms = descriptor.published_unix_ms.min(now_unix_ms());
        match self.descriptors.get(&descriptor.peer_id) {
            Some(current) if current.published_unix_ms > descriptor.published_unix_ms => {}
            _ => {
                self.observed_peers.insert(peer_id);
                self.descriptors
                    .insert(descriptor.peer_id.clone(), descriptor);
            }
        }
    }

    fn best_descriptor(&self) -> Option<PeerDescriptor> {
        self.descriptors.values().cloned().max_by_key(|descriptor| {
            (
                descriptor.published_unix_ms,
                descriptor_candidates(descriptor).len(),
                descriptor.peer_id.clone(),
            )
        })
    }

    fn has_usable_candidates(&self) -> bool {
        self.descriptors
            .values()
            .any(|descriptor| !descriptor_candidates(descriptor).is_empty())
    }

    fn into_candidates(self) -> Vec<Candidate> {
        rank_candidates(
            self.descriptors
                .into_values()
                .flat_map(|descriptor| descriptor_candidates(&descriptor)),
        )
    }
}

async fn discover_peer_descriptors(
    name: &HumanName,
    code: &HumanCode,
    config: DiscoveryConfig,
) -> anyhow::Result<Option<DiscoverySnapshot>> {
    let name_code = NameCode::new(name.clone(), code.clone());
    let lookup_key = name_code.lookup_key();
    let descriptor_key = descriptor_record_key(&lookup_key);
    let provider_key = provider_record_key(&lookup_key);
    let mut snapshot = DiscoverySnapshot::new();

    match rendezvous::discover_peer_descriptors(name, code, &config.rendezvous).await {
        Ok(descriptors) => {
            if !descriptors.is_empty() {
                tracing::debug!(
                    count = descriptors.len(),
                    "rendezvous discovery returned descriptors"
                );
            }
            for descriptor in descriptors {
                snapshot.insert_descriptor(descriptor);
            }
            if snapshot.is_diverse_enough(config.min_candidate_diversity)
                && snapshot.has_usable_candidates()
            {
                return Ok(Some(snapshot));
            }
        }
        Err(error) => {
            tracing::debug!(%error, "rendezvous discovery failed");
        }
    }

    let mut swarm = build_discovery_swarm(false, config.enable_mdns)?;
    apply_bootstrap(&mut swarm, &config);
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    let mut query_interval = time::interval(Duration::from_millis(750));
    let deadline = time::sleep(config.lookup_timeout);
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = query_interval.tick() => {
                let _ = swarm.behaviour_mut().kad.get_providers(provider_key.clone());
                let _ = swarm.behaviour_mut().kad.get_record(descriptor_key.clone());
            }
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Kad(kad::Event::OutboundQueryProgressed { result, .. })) => {
                        handle_discovery_query_result(&mut snapshot, result);
                        if snapshot.is_diverse_enough(config.min_candidate_diversity) {
                            break;
                        }
                    }
                    SwarmEvent::Behaviour(event) => {
                        let _ = handle_discovery_event_with_snapshot(&mut swarm, event, Some(&mut snapshot));
                        if snapshot.is_diverse_enough(config.min_candidate_diversity) {
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    if snapshot.is_diverse_enough(config.min_candidate_diversity) {
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
    let mut swarm = build_discovery_swarm(true, config.enable_mdns)?;
    apply_bootstrap(&mut swarm, &config);
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;

    let name_code = NameCode::new(name, code);
    let record_key = descriptor_record_key(&name_code.lookup_key());
    let provider_key = provider_record_key(&name_code.lookup_key());
    let allow_loopback = config.allow_loopback_endpoints;
    let mut interval = time::interval(Duration::from_secs(60));

    let descriptor = publish_descriptor(
        &mut swarm,
        record_key.clone(),
        provider_key.clone(),
        direct_bind,
        allow_loopback,
    )?;
    let _ = rendezvous::publish_peer_descriptor(
        &name_code.name,
        &name_code.code,
        &descriptor,
        &config.rendezvous,
    )
    .await;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let descriptor = publish_descriptor(
                    &mut swarm,
                    record_key.clone(),
                    provider_key.clone(),
                    direct_bind,
                    allow_loopback,
                )?;
                let _ = rendezvous::publish_peer_descriptor(&name_code.name, &name_code.code, &descriptor, &config.rendezvous).await;
            }
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::Behaviour(event) => {
                        let should_publish = handle_discovery_event(&mut swarm, event);
                        if should_publish
                            && let Ok(descriptor) = publish_descriptor(
                                &mut swarm,
                                record_key.clone(),
                                provider_key.clone(),
                                direct_bind,
                                allow_loopback,
                            )
                        {
                            let _ = rendezvous::publish_peer_descriptor(
                                &name_code.name,
                                &name_code.code,
                                &descriptor,
                                &config.rendezvous,
                            )
                            .await;
                        }
                    }
                    SwarmEvent::ConnectionEstablished { .. } => {
                        if let Ok(descriptor) = publish_descriptor(
                            &mut swarm,
                            record_key.clone(),
                            provider_key.clone(),
                            direct_bind,
                            allow_loopback,
                        ) {
                            let _ = rendezvous::publish_peer_descriptor(
                                &name_code.name,
                                &name_code.code,
                                &descriptor,
                                &config.rendezvous,
                            )
                            .await;
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

fn build_discovery_swarm(
    server_mode: bool,
    enable_mdns: bool,
) -> anyhow::Result<Swarm<DiscoveryBehaviour>> {
    Ok(SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            Default::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_behaviour(move |key| {
            let local_peer_id = key.public().to_peer_id();
            let store = kad::store::MemoryStore::new(local_peer_id);
            let mut kad = kad::Behaviour::new(local_peer_id, store);
            kad.set_mode(Some(if server_mode {
                kad::Mode::Server
            } else {
                kad::Mode::Client
            }));
            Ok(DiscoveryBehaviour {
                kad,
                mdns: if enable_mdns {
                    Some(mdns::tokio::Behaviour::new(
                        mdns::Config::default(),
                        local_peer_id,
                    )?)
                } else {
                    None
                }
                .into(),
                identify: identify::Behaviour::new(identify::Config::new(
                    "/peerline/0.1.0".into(),
                    key.public(),
                )),
                ping: ping::Behaviour::default(),
            })
        })?
        .build())
}

fn publish_descriptor(
    swarm: &mut Swarm<DiscoveryBehaviour>,
    record_key: kad::RecordKey,
    provider_key: kad::RecordKey,
    direct_bind: SocketAddr,
    allow_loopback: bool,
) -> anyhow::Result<PeerDescriptor> {
    let descriptor = make_peer_descriptor(
        1,
        swarm.local_peer_id().to_string(),
        direct_endpoints(direct_bind, allow_loopback)
            .into_iter()
            .map(|endpoint| endpoint.to_string())
            .collect(),
        swarm
            .listeners()
            .chain(swarm.external_addresses())
            .map(ToString::to_string)
            .collect(),
    );
    let record = kad::Record::new(record_key, postcard::to_allocvec(&descriptor)?);
    if let Err(error) = swarm
        .behaviour_mut()
        .kad
        .put_record(record, kad::Quorum::One)
    {
        tracing::warn!(%error, "could not start DHT descriptor publish");
    }
    if let Err(error) = swarm.behaviour_mut().kad.start_providing(provider_key) {
        tracing::warn!(%error, "could not start DHT provider publish");
    }
    Ok(descriptor)
}

fn handle_discovery_query_result(snapshot: &mut DiscoverySnapshot, result: kad::QueryResult) {
    match result {
        kad::QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders {
            providers,
            ..
        })) => {
            snapshot.observed_peers.extend(providers);
        }
        kad::QueryResult::GetProviders(Ok(
            kad::GetProvidersOk::FinishedWithNoAdditionalRecord { closest_peers },
        )) => {
            snapshot.observed_peers.extend(closest_peers);
        }
        kad::QueryResult::GetRecord(Ok(kad::GetRecordOk::FoundRecord(record))) => {
            if let Some(peer) = record.peer {
                snapshot.observed_peers.insert(peer);
            }
            if let Some(publisher) = record.record.publisher {
                snapshot.observed_peers.insert(publisher);
            }
            if let Ok(descriptor) = postcard::from_bytes::<PeerDescriptor>(&record.record.value) {
                if let Ok(peer_id) = descriptor.peer_id.parse::<PeerId>() {
                    snapshot.observed_peers.insert(peer_id);
                }
                snapshot.insert_descriptor(descriptor);
            }
        }
        kad::QueryResult::GetProviders(Err(error)) => {
            tracing::debug!(%error, "DHT provider lookup failed");
        }
        kad::QueryResult::GetRecord(Err(error)) => {
            tracing::debug!(%error, "DHT descriptor lookup failed");
        }
        kad::QueryResult::Bootstrap(Ok(ok)) => {
            tracing::debug!(peer = %ok.peer, remaining = ok.num_remaining, "DHT bootstrap progressed");
        }
        kad::QueryResult::Bootstrap(Err(error)) => {
            tracing::debug!(%error, "DHT bootstrap failed");
        }
        kad::QueryResult::GetRecord(Ok(kad::GetRecordOk::FinishedWithNoAdditionalRecord {
            cache_candidates,
        })) => {
            snapshot
                .observed_peers
                .extend(cache_candidates.into_values());
        }
        _ => {}
    }
}

fn handle_discovery_event(
    swarm: &mut Swarm<DiscoveryBehaviour>,
    event: DiscoveryBehaviourEvent,
) -> bool {
    handle_discovery_event_with_snapshot(swarm, event, None)
}

fn handle_discovery_event_with_snapshot(
    swarm: &mut Swarm<DiscoveryBehaviour>,
    event: DiscoveryBehaviourEvent,
    mut snapshot: Option<&mut DiscoverySnapshot>,
) -> bool {
    match event {
        DiscoveryBehaviourEvent::Kad(kad::Event::OutboundQueryProgressed { result, .. }) => {
            log_publish_query_result(&result);
            false
        }
        DiscoveryBehaviourEvent::Mdns(mdns::Event::Discovered(peers)) => {
            let changed = !peers.is_empty();
            for (peer, addr) in peers {
                if let Some(snapshot) = snapshot.as_mut() {
                    snapshot.observe_local_peer(peer);
                }
                swarm.behaviour_mut().kad.add_address(&peer, addr);
            }
            changed
        }
        DiscoveryBehaviourEvent::Identify(identify::Event::Received { peer_id, info, .. }) => {
            let changed = !info.listen_addrs.is_empty();
            for addr in info.listen_addrs {
                swarm.behaviour_mut().kad.add_address(&peer_id, addr);
            }
            changed
        }
        _ => false,
    }
}

fn log_publish_query_result(result: &kad::QueryResult) {
    match result {
        kad::QueryResult::PutRecord(Ok(ok)) => {
            tracing::debug!(key = ?ok.key, "DHT descriptor published");
        }
        kad::QueryResult::PutRecord(Err(error)) => {
            tracing::warn!(%error, "DHT descriptor publish failed");
        }
        kad::QueryResult::StartProviding(Ok(ok)) => {
            tracing::debug!(key = ?ok.key, "DHT provider record published");
        }
        kad::QueryResult::StartProviding(Err(error)) => {
            tracing::warn!(%error, "DHT provider record publish failed");
        }
        kad::QueryResult::Bootstrap(Ok(ok)) => {
            tracing::debug!(peer = %ok.peer, remaining = ok.num_remaining, "DHT bootstrap progressed");
        }
        kad::QueryResult::Bootstrap(Err(error)) => {
            tracing::debug!(%error, "DHT bootstrap failed");
        }
        _ => {}
    }
}

fn apply_bootstrap(swarm: &mut Swarm<DiscoveryBehaviour>, config: &DiscoveryConfig) {
    for raw in &config.bootstrap_peers {
        let Ok(addr) = raw.parse::<Multiaddr>() else {
            continue;
        };
        if let Some((peer, without_peer)) = split_peer_addr(addr.clone()) {
            swarm.behaviour_mut().kad.add_address(&peer, without_peer);
        }
        let _ = swarm.dial(addr);
    }
    if let Err(error) = swarm.behaviour_mut().kad.bootstrap() {
        tracing::debug!(%error, "could not start DHT bootstrap");
    }
}

fn split_peer_addr(mut addr: Multiaddr) -> Option<(PeerId, Multiaddr)> {
    match addr.pop() {
        Some(libp2p::multiaddr::Protocol::P2p(peer)) => Some((peer, addr)),
        _ => None,
    }
}

pub(crate) fn direct_endpoints(bind: SocketAddr, allow_loopback: bool) -> Vec<SocketAddr> {
    if bind.ip().is_unspecified() {
        direct_endpoints_from_ips(
            bind.port(),
            local_ip_address::list_afinet_netifas()
                .map(|interfaces| interfaces.into_iter().map(|(_, ip)| ip).collect::<Vec<_>>())
                .unwrap_or_default(),
            allow_loopback,
        )
    } else if is_usable_endpoint_ip(&bind.ip(), allow_loopback) {
        vec![bind]
    } else {
        Vec::new()
    }
}

fn direct_endpoints_from_ips(
    port: u16,
    ips: impl IntoIterator<Item = IpAddr>,
    allow_loopback: bool,
) -> Vec<SocketAddr> {
    let mut endpoints = ips
        .into_iter()
        .filter(|ip| is_usable_endpoint_ip(ip, allow_loopback))
        .map(|ip| SocketAddr::new(ip, port))
        .collect::<Vec<_>>();
    endpoints.sort_by_key(direct_endpoint_priority);
    endpoints.dedup();
    endpoints
}

fn is_usable_endpoint_ip(ip: &IpAddr, allow_loopback: bool) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            !ip.is_unspecified()
                && (allow_loopback || !ip.is_loopback())
                && !ip.is_link_local()
                && !ip.is_multicast()
                && !ip.is_broadcast()
        }
        IpAddr::V6(ip) => {
            !ip.is_unspecified()
                && (allow_loopback || !ip.is_loopback())
                && !ip.is_unicast_link_local()
                && !ip.is_multicast()
        }
    }
}

pub(crate) fn direct_endpoint_priority(endpoint: &SocketAddr) -> u8 {
    match endpoint.ip() {
        IpAddr::V4(ip) if ip.is_private() => 0,
        IpAddr::V6(ip) if ip.is_unique_local() => 0,
        IpAddr::V4(ip) if !ip.is_loopback() => 1,
        IpAddr::V6(ip) if !ip.is_loopback() => 1,
        _ => 2,
    }
}

fn libp2p_endpoint_priority(endpoint: &Multiaddr) -> u8 {
    if is_relayed(endpoint) {
        4
    } else if is_webrtc(endpoint) {
        3
    } else if is_quic(endpoint) {
        2
    } else {
        1
    }
}

fn route_kind_from_multiaddr(addr: &Multiaddr) -> RouteKind {
    if is_relayed(addr) {
        RouteKind::Libp2pRelay
    } else if is_webrtc(addr) {
        RouteKind::WebRtcTurn
    } else {
        RouteKind::Libp2pDcutr
    }
}

fn route_kind_from_direct_endpoint(endpoint: &SocketAddr) -> RouteKind {
    match endpoint.ip() {
        IpAddr::V4(ip) if ip.is_private() || ip.is_loopback() => RouteKind::LanDirect,
        IpAddr::V6(ip) if ip.is_unique_local() || ip.is_loopback() => RouteKind::LanDirect,
        _ => RouteKind::PublicDirect,
    }
}

fn is_relayed(addr: &Multiaddr) -> bool {
    addr.iter()
        .any(|protocol| protocol == libp2p::multiaddr::Protocol::P2pCircuit)
}

fn is_webrtc(addr: &Multiaddr) -> bool {
    addr.iter()
        .any(|protocol| matches!(protocol, libp2p::multiaddr::Protocol::WebRTCDirect))
}

fn is_quic(addr: &Multiaddr) -> bool {
    addr.iter()
        .any(|protocol| matches!(protocol, libp2p::multiaddr::Protocol::QuicV1))
}

fn is_dialable_multiaddr(addr: &Multiaddr) -> bool {
    !addr.iter().any(|protocol| match protocol {
        libp2p::multiaddr::Protocol::Ip4(ip) => ip.is_unspecified(),
        libp2p::multiaddr::Protocol::Ip6(ip) => ip.is_unspecified(),
        libp2p::multiaddr::Protocol::Tcp(port) | libp2p::multiaddr::Protocol::Udp(port) => {
            port == 0
        }
        _ => false,
    })
}

fn bootstrap_peers_from_env() -> Option<Vec<String>> {
    std::env::var("PEERLINE_BOOTSTRAP").ok().map(|raw| {
        raw.split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    })
}

fn env_flag(name: &str) -> Option<()> {
    match std::env::var(name).ok()?.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(()),
        _ => None,
    }
}

pub fn rank_candidates(candidates: impl IntoIterator<Item = Candidate>) -> Vec<Candidate> {
    let mut candidates = candidates.into_iter().collect::<Vec<_>>();
    candidates.sort_by_key(|candidate| match candidate.route {
        RouteKind::LanDirect => 0,
        RouteKind::PublicDirect => 1,
        RouteKind::Libp2pDcutr => 2,
        RouteKind::WebRtcTurn => 3,
        RouteKind::Libp2pRelay => 4,
    });
    candidates.dedup();
    candidates
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

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
    fn descriptor_candidates_never_rank_loopback_first() {
        let descriptor = PeerDescriptor {
            protocol_version: 1,
            peer_id: "peer".into(),
            direct_endpoints: vec![
                "127.0.0.1:43117".into(),
                "203.0.113.7:43117".into(),
                "192.168.1.20:43117".into(),
            ],
            libp2p_endpoints: vec![],
            published_unix_ms: 0,
        };

        let endpoints = direct_endpoint_candidates(&descriptor);
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
    fn mdns_can_be_disabled_for_deterministic_network_tests() {
        let swarm = build_discovery_swarm(false, false).unwrap();
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
            protocol_version: 1,
            peer_id: local_peer.to_string(),
            direct_endpoints: vec!["192.168.1.20:43117".into()],
            libp2p_endpoints: vec![],
            published_unix_ms: 1,
        });
        assert!(snapshot.is_diverse_enough(3));
    }

    #[test]
    fn empty_descriptors_do_not_count_as_usable_candidates() {
        let mut snapshot = DiscoverySnapshot::new();
        let peer = PeerId::random();
        snapshot.observe_local_peer(peer);
        snapshot.insert_descriptor(PeerDescriptor {
            protocol_version: 1,
            peer_id: peer.to_string(),
            direct_endpoints: vec![],
            libp2p_endpoints: vec![],
            published_unix_ms: 1,
        });

        assert!(snapshot.is_diverse_enough(1));
        assert!(!snapshot.has_usable_candidates());
    }

    #[test]
    fn invalid_peer_ids_are_ignored_during_discovery() {
        let mut snapshot = DiscoverySnapshot::new();
        snapshot.insert_descriptor(PeerDescriptor {
            protocol_version: 1,
            peer_id: "not-a-peer-id".into(),
            direct_endpoints: vec!["192.168.1.20:43117".into()],
            libp2p_endpoints: vec![],
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
            protocol_version: 1,
            peer_id: peer.to_string(),
            direct_endpoints: vec!["192.168.1.20:43117".into()],
            libp2p_endpoints: vec![],
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
}
