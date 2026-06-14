use super::make_peer_descriptor;
use libp2p::{Multiaddr, PeerId, Swarm, multiaddr::Protocol, rendezvous, swarm::NetworkBehaviour};
use peerline_core::LookupKey;
use peerline_rendezvous_model::{I2pEndpoint, PublicTunnelEndpoint, TorOnionEndpoint};

const RECEIVER_NAMESPACE_PREFIX: &str = "peerline/receiver/v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Libp2pRendezvousPeer {
    pub peer_id: PeerId,
    pub address: Multiaddr,
}

impl Libp2pRendezvousPeer {
    pub fn dial_addr(&self) -> Multiaddr {
        self.address.clone().with(Protocol::P2p(self.peer_id))
    }
}

pub(crate) fn libp2p_rendezvous_peers_from_env() -> Option<Vec<Libp2pRendezvousPeer>> {
    std::env::var("PEERLINE_LIBP2P_RENDEZVOUS_PEERS")
        .ok()
        .map(|raw| parse_libp2p_rendezvous_peers(&raw))
}

pub(crate) fn parse_libp2p_rendezvous_peers(raw: &str) -> Vec<Libp2pRendezvousPeer> {
    raw.split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter_map(|value| match parse_libp2p_rendezvous_peer(value) {
            Ok(peer) => Some(peer),
            Err(error) => {
                tracing::warn!(%error, value, "ignoring invalid libp2p rendezvous peer");
                None
            }
        })
        .collect()
}

fn parse_libp2p_rendezvous_peer(raw: &str) -> anyhow::Result<Libp2pRendezvousPeer> {
    let addr = raw.parse::<Multiaddr>()?;
    let Some((peer_id, address)) = split_peer_addr(addr) else {
        anyhow::bail!("rendezvous peer multiaddr must end with /p2p/<peer-id>");
    };
    Ok(Libp2pRendezvousPeer { peer_id, address })
}

pub(crate) fn receiver_namespace(
    lookup_key: &LookupKey,
) -> Result<rendezvous::Namespace, rendezvous::NamespaceTooLong> {
    rendezvous::Namespace::new(format!("{RECEIVER_NAMESPACE_PREFIX}/{}", lookup_key.hex()))
}

pub(crate) fn dial_configured_rendezvous<B: NetworkBehaviour>(
    swarm: &mut Swarm<B>,
    peers: &[Libp2pRendezvousPeer],
) {
    for peer in peers {
        let addr = peer.dial_addr();
        if let Err(error) = swarm.dial(addr.clone()) {
            tracing::debug!(%error, %addr, "could not dial libp2p rendezvous peer");
        }
    }
}

pub(crate) fn refresh_external_addresses_for_rendezvous<B: NetworkBehaviour>(
    swarm: &mut Swarm<B>,
    allow_loopback: bool,
) {
    let addresses = swarm
        .listeners()
        .chain(swarm.external_addresses())
        .filter(|addr| is_dialable_rendezvous_addr(addr, allow_loopback))
        .cloned()
        .collect::<Vec<_>>();
    for addr in addresses {
        swarm.add_external_address(addr);
    }
}

pub(crate) fn descriptor_from_registration(
    registration: &rendezvous::Registration,
    allow_loopback: bool,
) -> Option<peerline_rendezvous_model::PeerDescriptor> {
    let endpoints = registration
        .record
        .addresses()
        .iter()
        .filter(|addr| is_dialable_rendezvous_addr(addr, allow_loopback))
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if endpoints.is_empty() {
        return None;
    }
    Some(make_peer_descriptor(
        registration.record.peer_id().to_string(),
        Vec::new(),
        endpoints,
        Vec::<PublicTunnelEndpoint>::new(),
        Vec::<TorOnionEndpoint>::new(),
        Vec::<I2pEndpoint>::new(),
    ))
}

fn is_dialable_rendezvous_addr(addr: &Multiaddr, allow_loopback: bool) -> bool {
    !addr.iter().any(|protocol| match protocol {
        Protocol::Ip4(ip) => {
            ip.is_unspecified()
                || (!allow_loopback && ip.is_loopback())
                || ip.is_link_local()
                || ip.is_multicast()
                || ip.is_broadcast()
        }
        Protocol::Ip6(ip) => {
            ip.is_unspecified()
                || (!allow_loopback && ip.is_loopback())
                || ip.is_unicast_link_local()
                || ip.is_multicast()
        }
        Protocol::Tcp(port) | Protocol::Udp(port) => port == 0,
        _ => false,
    })
}

fn split_peer_addr(mut addr: Multiaddr) -> Option<(PeerId, Multiaddr)> {
    match addr.pop() {
        Some(Protocol::P2p(peer)) => Some((peer, addr)),
        _ => None,
    }
}
