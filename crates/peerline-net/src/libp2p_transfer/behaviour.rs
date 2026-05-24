use super::codec::WireCodec;
use crate::{
    discovery::{DiscoveryConfig, WebRtcIceServer},
    protocol::LIBP2P_TRANSFER_PROTOCOL,
};
use libp2p::{
    PeerId, StreamProtocol, Swarm, SwarmBuilder, autonat, dcutr, identify, kad, mdns, noise, ping,
    relay,
    request_response::{self, ProtocolSupport},
    swarm::{NetworkBehaviour, behaviour::toggle::Toggle},
    upnp, yamux,
};
use rand::thread_rng;
use std::{error::Error as StdError, time::Duration};

#[derive(NetworkBehaviour)]
#[behaviour(prelude = "libp2p::swarm::derive_prelude")]
pub(crate) struct TransferBehaviour {
    pub(crate) kad: kad::Behaviour<kad::store::MemoryStore>,
    pub(crate) mdns: Toggle<mdns::tokio::Behaviour>,
    pub(crate) upnp: Toggle<upnp::tokio::Behaviour>,
    pub(crate) identify: identify::Behaviour,
    pub(crate) ping: ping::Behaviour,
    pub(crate) relay: relay::client::Behaviour,
    pub(crate) dcutr: dcutr::Behaviour,
    pub(crate) autonat: autonat::v1::Behaviour,
    pub(crate) transfer: request_response::Behaviour<WireCodec>,
}

pub(crate) async fn build_sender_swarm(
    enable_mdns: bool,
    enable_upnp: bool,
    webrtc_ice_servers: &[WebRtcIceServer],
) -> anyhow::Result<Swarm<TransferBehaviour>> {
    build_swarm(enable_mdns, enable_upnp, false, webrtc_ice_servers).await
}

pub(crate) async fn build_receiver_swarm(
    enable_mdns: bool,
    enable_upnp: bool,
    webrtc_ice_servers: &[WebRtcIceServer],
) -> anyhow::Result<Swarm<TransferBehaviour>> {
    build_swarm(enable_mdns, enable_upnp, true, webrtc_ice_servers).await
}

async fn build_swarm(
    enable_mdns: bool,
    enable_upnp: bool,
    server_mode: bool,
    webrtc_ice_servers: &[WebRtcIceServer],
) -> anyhow::Result<Swarm<TransferBehaviour>> {
    let webrtc_ice_servers = webrtc_transport_ice_servers(webrtc_ice_servers);
    let builder = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            Default::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_other_transport(move |key| {
            let certificate = libp2p_webrtc::tokio::Certificate::generate(&mut thread_rng())
                .map_err(|err| Box::new(err) as Box<dyn StdError + Send + Sync>)?;
            Ok(libp2p_webrtc::tokio::Transport::new_with_ice_servers(
                key.clone(),
                certificate,
                webrtc_ice_servers.clone(),
            ))
        })?
        .with_dns()?
        .with_relay_client(noise::Config::new, yamux::Config::default)?;

    Ok(builder
        .with_behaviour(|key, relay| {
            let local_peer_id = key.public().to_peer_id();
            let mut kad =
                kad::Behaviour::new(local_peer_id, kad::store::MemoryStore::new(local_peer_id));
            kad.set_mode(Some(if server_mode {
                kad::Mode::Server
            } else {
                kad::Mode::Client
            }));

            let protocols = std::iter::once((
                StreamProtocol::new(LIBP2P_TRANSFER_PROTOCOL),
                ProtocolSupport::Full,
            ));
            let transfer = request_response::Behaviour::with_codec(
                WireCodec,
                protocols,
                request_response::Config::default()
                    .with_request_timeout(Duration::from_secs(30))
                    .with_max_concurrent_streams(16),
            );

            Ok(TransferBehaviour {
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
                relay,
                dcutr: dcutr::Behaviour::new(local_peer_id),
                autonat: autonat::v1::Behaviour::new(local_peer_id, autonat::v1::Config::default()),
                transfer,
            })
        })?
        .build())
}

fn webrtc_transport_ice_servers(
    servers: &[WebRtcIceServer],
) -> Vec<libp2p_webrtc::tokio::IceServer> {
    servers
        .iter()
        .map(|server| libp2p_webrtc::tokio::IceServer {
            urls: server.urls.clone(),
            username: server.username.clone(),
            credential: server.credential.clone(),
        })
        .collect()
}

pub(crate) fn maybe_enable_relay_listeners(
    swarm: &mut Swarm<TransferBehaviour>,
    config: &DiscoveryConfig,
) {
    if !config.allow_relay_data_fallback {
        return;
    }

    let local_peer_id = *swarm.local_peer_id();
    for raw in &config.relay_peers {
        let Ok(addr) = raw.parse::<libp2p::Multiaddr>() else {
            continue;
        };
        let Some((relay_peer, relay_addr)) = split_peer_addr(addr.clone()) else {
            continue;
        };
        let circuit_addr = relay_addr
            .with(libp2p::multiaddr::Protocol::P2p(relay_peer))
            .with(libp2p::multiaddr::Protocol::P2pCircuit)
            .with(libp2p::multiaddr::Protocol::P2p(local_peer_id));
        let _ = swarm.listen_on(circuit_addr);
    }
}

pub(crate) fn apply_bootstrap(swarm: &mut Swarm<TransferBehaviour>, config: &DiscoveryConfig) {
    for raw in &config.bootstrap_peers {
        let Ok(addr) = raw.parse::<libp2p::Multiaddr>() else {
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

fn split_peer_addr(mut addr: libp2p::Multiaddr) -> Option<(PeerId, libp2p::Multiaddr)> {
    match addr.pop() {
        Some(libp2p::multiaddr::Protocol::P2p(peer)) => Some((peer, addr)),
        _ => None,
    }
}
