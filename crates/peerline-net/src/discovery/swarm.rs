use super::{
    DiscoveryConfig, endpoints::direct_endpoints_with_extra, make_peer_descriptor,
    snapshot::DiscoverySnapshot,
};
use libp2p::{
    Multiaddr, PeerId, Swarm, SwarmBuilder, identify, kad, mdns, noise, ping,
    swarm::{NetworkBehaviour, SwarmEvent, behaviour::toggle::Toggle},
    upnp, yamux,
};
use peerline_rendezvous_model::{PeerDescriptor, PublicTunnelEndpoint, TorOnionEndpoint};
use std::net::SocketAddr;

#[derive(NetworkBehaviour)]
#[behaviour(prelude = "libp2p::swarm::derive_prelude")]
pub(super) struct DiscoveryBehaviour {
    pub(super) kad: kad::Behaviour<kad::store::MemoryStore>,
    pub(super) mdns: Toggle<mdns::tokio::Behaviour>,
    pub(super) upnp: Toggle<upnp::tokio::Behaviour>,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
}

pub(super) fn build_discovery_swarm(
    server_mode: bool,
    enable_mdns: bool,
    enable_upnp: bool,
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
                upnp: if enable_upnp {
                    Some(upnp::tokio::Behaviour::default())
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

pub(super) fn publish_descriptor(
    swarm: &mut Swarm<DiscoveryBehaviour>,
    record_key: kad::RecordKey,
    provider_key: kad::RecordKey,
    direct_bind: SocketAddr,
    extra_direct_endpoints: &[SocketAddr],
    allow_loopback: bool,
) -> anyhow::Result<PeerDescriptor> {
    let descriptor = make_peer_descriptor(
        swarm.local_peer_id().to_string(),
        direct_endpoints_with_extra(direct_bind, allow_loopback, extra_direct_endpoints)
            .into_iter()
            .map(|endpoint| endpoint.to_string())
            .collect(),
        swarm
            .listeners()
            .chain(swarm.external_addresses())
            .map(ToString::to_string)
            .collect(),
        Vec::<PublicTunnelEndpoint>::new(),
        Vec::<TorOnionEndpoint>::new(),
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

pub(super) fn handle_discovery_swarm_event(
    swarm: &mut Swarm<DiscoveryBehaviour>,
    snapshot: &mut DiscoverySnapshot,
    event: SwarmEvent<DiscoveryBehaviourEvent>,
) {
    match event {
        SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Kad(
            kad::Event::OutboundQueryProgressed { result, .. },
        )) => {
            handle_discovery_query_result(snapshot, result);
        }
        SwarmEvent::Behaviour(event) => {
            let _ = handle_discovery_event_with_snapshot(swarm, event, Some(snapshot));
        }
        _ => {}
    }
}

pub(super) fn handle_publish_swarm_event(
    swarm: &mut Swarm<DiscoveryBehaviour>,
    event: SwarmEvent<DiscoveryBehaviourEvent>,
) -> bool {
    match event {
        SwarmEvent::Behaviour(event) => handle_discovery_event(swarm, event),
        SwarmEvent::ConnectionEstablished { .. }
        | SwarmEvent::NewListenAddr { .. }
        | SwarmEvent::ExternalAddrConfirmed { .. }
        | SwarmEvent::ExternalAddrExpired { .. } => true,
        _ => false,
    }
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
        DiscoveryBehaviourEvent::Upnp(event) => match event {
            upnp::Event::NewExternalAddr(addr) => {
                tracing::debug!(%addr, "UPnP external address confirmed");
                if let Some(snapshot) = snapshot.as_mut() {
                    snapshot.observe_local_peer(*swarm.local_peer_id());
                }
                true
            }
            upnp::Event::ExpiredExternalAddr(addr) => {
                tracing::debug!(%addr, "UPnP external address expired");
                true
            }
            upnp::Event::GatewayNotFound => {
                tracing::debug!("UPnP gateway not found");
                false
            }
            upnp::Event::NonRoutableGateway => {
                tracing::debug!("UPnP gateway is not routable");
                false
            }
        },
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

pub(super) fn apply_bootstrap(swarm: &mut Swarm<DiscoveryBehaviour>, config: &DiscoveryConfig) {
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
