mod endpoints;
mod snapshot;
mod swarm;

#[cfg(test)]
mod tests;

use crate::rendezvous::{self, RendezvousConfig};
use endpoints::LocalDirectNetworks;
use futures::StreamExt;
use peerline_core::{ConnectionRoute, HumanCode, HumanName, LookupKey, NameCode};
use peerline_rendezvous_model::{PeerDescriptor, RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION};
use serde::{Deserialize, Serialize};
use snapshot::DiscoverySnapshot;
use std::{
    net::SocketAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use swarm::{
    apply_bootstrap, build_discovery_swarm, handle_discovery_swarm_event,
    handle_publish_swarm_event, publish_descriptor,
};
use tokio::{task::JoinHandle, time};

pub(crate) use endpoints::direct_endpoints;
pub use endpoints::rank_candidates;

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
    peer_id: impl Into<String>,
    direct_endpoints: Vec<String>,
    libp2p_endpoints: Vec<String>,
) -> PeerDescriptor {
    PeerDescriptor {
        protocol_version: RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION,
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
                handle_discovery_swarm_event(&mut swarm, &mut snapshot, event);
                if snapshot.is_diverse_enough(config.min_candidate_diversity)
                    && snapshot.has_usable_candidates(local_networks)
                {
                    break;
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
                if handle_publish_swarm_event(&mut swarm, event)
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
        }
    }
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
