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
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
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

fn discovered_direct_endpoint_candidates(
    descriptor: &PeerDescriptor,
    local_networks: Option<&LocalDirectNetworks>,
    allow_unverified_lan: bool,
) -> Vec<SocketAddr> {
    let mut endpoints = descriptor
        .direct_endpoints
        .iter()
        .filter_map(|endpoint| endpoint.parse().ok())
        .filter(|endpoint: &SocketAddr| is_usable_endpoint_ip(&endpoint.ip(), true))
        .filter(|endpoint| {
            let reachable = discovered_direct_endpoint_is_reachable(
                endpoint,
                local_networks,
                allow_unverified_lan,
            );
            if !reachable {
                tracing::debug!(
                    %endpoint,
                    "skipping discovered LAN endpoint outside local network"
                );
            }
            reachable
        })
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

fn descriptor_candidates_for_discovery(
    descriptor: &PeerDescriptor,
    local_networks: Option<&LocalDirectNetworks>,
    allow_unverified_lan: bool,
) -> Vec<Candidate> {
    let mut candidates = Vec::new();
    let peer_id = descriptor.peer_id.clone();
    let direct =
        discovered_direct_endpoint_candidates(descriptor, local_networks, allow_unverified_lan)
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
    Ok(
        discover_peer_descriptors(name, code, config, local_networks.as_ref())
            .await?
            .map(|snapshot| snapshot.into_candidates(local_networks.as_ref()))
            .unwrap_or_default(),
    )
}

pub async fn discover_peer_descriptor(
    name: &HumanName,
    code: &HumanCode,
    config: DiscoveryConfig,
) -> anyhow::Result<Option<PeerDescriptor>> {
    let local_networks = LocalDirectNetworks::current();
    Ok(
        discover_peer_descriptors(name, code, config, local_networks.as_ref())
            .await?
            .and_then(|snapshot| snapshot.best_descriptor(local_networks.as_ref())),
    )
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

    fn best_descriptor(
        &self,
        local_networks: Option<&LocalDirectNetworks>,
    ) -> Option<PeerDescriptor> {
        self.descriptors
            .values()
            .filter(|descriptor| {
                let allow_unverified_lan = self.local_peer_ids.contains(&descriptor.peer_id);
                !descriptor_candidates_for_discovery(
                    descriptor,
                    local_networks,
                    allow_unverified_lan,
                )
                .is_empty()
            })
            .cloned()
            .max_by_key(|descriptor| {
                let allow_unverified_lan = self.local_peer_ids.contains(&descriptor.peer_id);
                (
                    descriptor.published_unix_ms,
                    descriptor_candidates_for_discovery(
                        descriptor,
                        local_networks,
                        allow_unverified_lan,
                    )
                    .len(),
                    descriptor.peer_id.clone(),
                )
            })
    }

    fn best_direct_endpoints(
        &self,
        local_networks: Option<&LocalDirectNetworks>,
    ) -> Vec<SocketAddr> {
        self.best_descriptor(local_networks)
            .map(|descriptor| {
                let allow_unverified_lan = self.local_peer_ids.contains(&descriptor.peer_id);
                discovered_direct_endpoint_candidates(
                    &descriptor,
                    local_networks,
                    allow_unverified_lan,
                )
            })
            .unwrap_or_default()
    }

    fn has_usable_candidates(&self, local_networks: Option<&LocalDirectNetworks>) -> bool {
        self.descriptors.values().any(|descriptor| {
            let allow_unverified_lan = self.local_peer_ids.contains(&descriptor.peer_id);
            !descriptor_candidates_for_discovery(descriptor, local_networks, allow_unverified_lan)
                .is_empty()
        })
    }

    fn into_candidates(self, local_networks: Option<&LocalDirectNetworks>) -> Vec<Candidate> {
        let local_peer_ids = self.local_peer_ids;
        rank_candidates(self.descriptors.into_values().flat_map(|descriptor| {
            let allow_unverified_lan = local_peer_ids.contains(&descriptor.peer_id);
            descriptor_candidates_for_discovery(&descriptor, local_networks, allow_unverified_lan)
        }))
    }
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

    let mut swarm = build_discovery_swarm(false, config.enable_mdns)?;
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
                            && snapshot.has_usable_candidates(local_networks)
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
                match event {
                    SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Kad(kad::Event::OutboundQueryProgressed { result, .. })) => {
                        handle_discovery_query_result(&mut snapshot, result);
                        if snapshot.is_diverse_enough(config.min_candidate_diversity)
                            && snapshot.has_usable_candidates(local_networks)
                        {
                            break;
                        }
                    }
                    SwarmEvent::Behaviour(event) => {
                        let _ = handle_discovery_event_with_snapshot(&mut swarm, event, Some(&mut snapshot));
                        if snapshot.is_diverse_enough(config.min_candidate_diversity)
                            && snapshot.has_usable_candidates(local_networks)
                        {
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    if snapshot.is_diverse_enough(config.min_candidate_diversity)
        && snapshot.has_usable_candidates(local_networks)
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
    let mut swarm = build_discovery_swarm(true, config.enable_mdns)?;
    apply_bootstrap(&mut swarm, &config);
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;

    let name_code = NameCode::new(name, code);
    let record_key = descriptor_record_key(&name_code.lookup_key());
    let provider_key = provider_record_key(&name_code.lookup_key());
    let allow_loopback = config.allow_loopback_endpoints;
    let publish_interval = Duration::from_secs(60);
    let mut interval = time::interval_at(time::Instant::now() + publish_interval, publish_interval);

    let descriptor = publish_descriptor(
        &mut swarm,
        record_key.clone(),
        provider_key.clone(),
        direct_bind,
        allow_loopback,
    )?;
    let _rendezvous_registration = rendezvous::RendezvousRegistrationGuard::new(
        name_code.name.clone(),
        name_code.code.clone(),
        descriptor.peer_id.clone(),
        config.rendezvous.clone(),
    );
    rendezvous::publish_peer_descriptor_background(
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
                    allow_loopback,
                )?;
                rendezvous::publish_peer_descriptor_background(
                    name_code.name.clone(),
                    name_code.code.clone(),
                    descriptor,
                    config.rendezvous.clone(),
                );
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
                            rendezvous::publish_peer_descriptor_background(
                                name_code.name.clone(),
                                name_code.code.clone(),
                                descriptor,
                                config.rendezvous.clone(),
                            )
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
                            rendezvous::publish_peer_descriptor_background(
                                name_code.name.clone(),
                                name_code.code.clone(),
                                descriptor,
                                config.rendezvous.clone(),
                            )
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
    tracing::debug!(
        peer_id = %descriptor.peer_id,
        direct_endpoints = descriptor.direct_endpoints.len(),
        libp2p_endpoints = descriptor.libp2p_endpoints.len(),
        "publishing peer descriptor through DHT and rendezvous"
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
        kad::QueryResult::PutRecord(Err(kad::PutRecordError::QuorumFailed {
            success,
            quorum,
            ..
        })) if success.is_empty() => {
            tracing::debug!(
                stored = success.len(),
                needed = %quorum,
                "DHT descriptor publish waiting for peers"
            );
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct LocalDirectNetworks {
    ipv4: Vec<Ipv4Network>,
    ipv6: Vec<Ipv6Network>,
}

impl LocalDirectNetworks {
    fn current() -> Option<Self> {
        let mut networks = Self {
            ipv4: Vec::new(),
            ipv6: Vec::new(),
        };

        for interface in if_addrs::get_if_addrs().ok()? {
            if !interface.is_oper_up() {
                continue;
            }
            match interface.addr {
                if_addrs::IfAddr::V4(addr) => {
                    let ip = IpAddr::V4(addr.ip);
                    if is_private_lan_ip(&ip) && is_usable_endpoint_ip(&ip, false) {
                        networks
                            .ipv4
                            .push(Ipv4Network::new(addr.ip, addr.prefixlen));
                    }
                }
                if_addrs::IfAddr::V6(addr) => {
                    let ip = IpAddr::V6(addr.ip);
                    if is_private_lan_ip(&ip) && is_usable_endpoint_ip(&ip, false) {
                        networks
                            .ipv6
                            .push(Ipv6Network::new(addr.ip, addr.prefixlen));
                    }
                }
            }
        }

        networks.ipv4.sort();
        networks.ipv4.dedup();
        networks.ipv6.sort();
        networks.ipv6.dedup();

        Some(networks)
    }

    fn contains(&self, ip: &IpAddr) -> bool {
        match ip {
            IpAddr::V4(ip) => self.ipv4.iter().any(|network| network.contains(ip)),
            IpAddr::V6(ip) => self.ipv6.iter().any(|network| network.contains(ip)),
        }
    }

    #[cfg(test)]
    fn from_prefixes(
        ipv4: impl IntoIterator<Item = (Ipv4Addr, u8)>,
        ipv6: impl IntoIterator<Item = (Ipv6Addr, u8)>,
    ) -> Self {
        Self {
            ipv4: ipv4
                .into_iter()
                .map(|(ip, prefix_len)| Ipv4Network::new(ip, prefix_len))
                .collect(),
            ipv6: ipv6
                .into_iter()
                .map(|(ip, prefix_len)| Ipv6Network::new(ip, prefix_len))
                .collect(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Ipv4Network {
    network: u32,
    mask: u32,
}

impl Ipv4Network {
    fn new(ip: Ipv4Addr, prefix_len: u8) -> Self {
        let prefix_len = prefix_len.min(32);
        let mask = if prefix_len == 0 {
            0
        } else {
            u32::MAX << (32 - prefix_len)
        };
        Self {
            network: u32::from(ip) & mask,
            mask,
        }
    }

    fn contains(&self, ip: &Ipv4Addr) -> bool {
        u32::from(*ip) & self.mask == self.network
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Ipv6Network {
    network: u128,
    mask: u128,
}

impl Ipv6Network {
    fn new(ip: Ipv6Addr, prefix_len: u8) -> Self {
        let prefix_len = prefix_len.min(128);
        let mask = if prefix_len == 0 {
            0
        } else {
            u128::MAX << (128 - prefix_len)
        };
        Self {
            network: u128::from(ip) & mask,
            mask,
        }
    }

    fn contains(&self, ip: &Ipv6Addr) -> bool {
        u128::from(*ip) & self.mask == self.network
    }
}

fn discovered_direct_endpoint_is_reachable(
    endpoint: &SocketAddr,
    local_networks: Option<&LocalDirectNetworks>,
    allow_unverified_lan: bool,
) -> bool {
    if allow_unverified_lan || !is_private_lan_ip(&endpoint.ip()) {
        return true;
    }

    if endpoint.ip().is_loopback() {
        return false;
    }

    local_networks
        .map(|networks| networks.contains(&endpoint.ip()))
        .unwrap_or(true)
}

fn is_private_lan_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_private() || ip.is_loopback(),
        IpAddr::V6(ip) => ip.is_unique_local() || ip.is_loopback(),
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
    fn discovered_lan_candidates_require_matching_local_network() {
        let descriptor = PeerDescriptor {
            protocol_version: 1,
            peer_id: "peer".into(),
            direct_endpoints: vec![
                "192.168.1.20:43117".into(),
                "192.168.2.20:43117".into(),
                "203.0.113.7:43117".into(),
            ],
            libp2p_endpoints: vec![],
            published_unix_ms: 1,
        };
        let networks = LocalDirectNetworks::from_prefixes(
            [(Ipv4Addr::new(192, 168, 1, 9), 24)],
            std::iter::empty(),
        );

        let candidates = descriptor_candidates_for_discovery(&descriptor, Some(&networks), false);
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
            protocol_version: 1,
            peer_id: "peer".into(),
            direct_endpoints: vec!["127.0.0.1:43117".into(), "10.10.0.8:43117".into()],
            libp2p_endpoints: vec![],
            published_unix_ms: 1,
        };
        let networks = LocalDirectNetworks::from_prefixes(
            [(Ipv4Addr::new(10, 10, 0, 4), 24)],
            std::iter::empty(),
        );

        let candidates = descriptor_candidates_for_discovery(&descriptor, Some(&networks), false);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].addresses, vec!["10.10.0.8:43117"]);
    }

    #[test]
    fn mdns_observed_peers_keep_unverified_lan_candidates() {
        let mut snapshot = DiscoverySnapshot::new();
        let peer = PeerId::random();
        snapshot.observe_local_peer(peer);
        snapshot.insert_descriptor(PeerDescriptor {
            protocol_version: 1,
            peer_id: peer.to_string(),
            direct_endpoints: vec!["192.168.50.20:43117".into()],
            libp2p_endpoints: vec![],
            published_unix_ms: 1,
        });
        let networks = LocalDirectNetworks::from_prefixes(
            [(Ipv4Addr::new(10, 10, 0, 4), 24)],
            std::iter::empty(),
        );

        let candidates = snapshot.into_candidates(Some(&networks));

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].addresses, vec!["192.168.50.20:43117"]);
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
        assert!(!snapshot.has_usable_candidates(None));
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
