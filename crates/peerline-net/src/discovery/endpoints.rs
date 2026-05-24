use super::{Candidate, DiscoveryConfig, RouteKind};
use libp2p::Multiaddr;
use peerline_rendezvous_model::{PeerDescriptor, PublicTunnelEndpoint, TorOnionEndpoint};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

pub(super) fn discovered_direct_endpoint_candidates(
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

pub(super) fn descriptor_candidates_for_discovery(
    descriptor: &PeerDescriptor,
    local_networks: Option<&LocalDirectNetworks>,
    allow_unverified_lan: bool,
    config: &DiscoveryConfig,
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

    let public_tunnels = public_tunnel_endpoint_candidates(descriptor)
        .into_iter()
        .map(|endpoint| Candidate {
            peer_id: peer_id.clone(),
            addresses: vec![endpoint.url],
            route: RouteKind::PublicTunnel,
        });
    candidates.extend(public_tunnels);

    let tor_onions = tor_onion_endpoint_candidates(descriptor)
        .into_iter()
        .map(|endpoint| Candidate {
            peer_id: peer_id.clone(),
            addresses: vec![endpoint.url],
            route: RouteKind::TorOnion,
        });
    candidates.extend(tor_onions);

    let libp2p = libp2p_endpoint_candidates(descriptor)
        .into_iter()
        .map(|addr| Candidate {
            peer_id: peer_id.clone(),
            addresses: vec![addr.to_string()],
            route: route_kind_from_multiaddr(&addr, config.enable_turn),
        });
    candidates.extend(libp2p);
    rank_candidates(
        candidates
            .into_iter()
            .filter(|candidate| config.route_enabled(&candidate.route)),
    )
}

pub(crate) fn public_tunnel_endpoint_candidates(
    descriptor: &PeerDescriptor,
) -> Vec<PublicTunnelEndpoint> {
    let mut endpoints = descriptor
        .public_endpoints
        .iter()
        .filter_map(|endpoint| {
            let url = crate::tunnel::normalize_public_tunnel_url(&endpoint.url).ok()?;
            Some(PublicTunnelEndpoint {
                provider: endpoint.provider.clone(),
                url,
            })
        })
        .collect::<Vec<_>>();
    endpoints.sort_by(|left, right| left.url.cmp(&right.url));
    endpoints.dedup_by(|left, right| left.url == right.url);
    endpoints
}

pub(crate) fn tor_onion_endpoint_candidates(descriptor: &PeerDescriptor) -> Vec<TorOnionEndpoint> {
    let mut endpoints = descriptor
        .tor_endpoints
        .iter()
        .filter_map(|endpoint| {
            let url = crate::tunnel::normalize_tor_onion_url(&endpoint.url).ok()?;
            Some(TorOnionEndpoint { url })
        })
        .collect::<Vec<_>>();
    endpoints.sort_by(|left, right| left.url.cmp(&right.url));
    endpoints.dedup_by(|left, right| left.url == right.url);
    endpoints
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

pub(crate) fn direct_endpoints_with_extra(
    bind: SocketAddr,
    allow_loopback: bool,
    extra_endpoints: &[SocketAddr],
) -> Vec<SocketAddr> {
    let mut endpoints = direct_endpoints(bind, allow_loopback);
    endpoints.extend(
        extra_endpoints
            .iter()
            .copied()
            .filter(|endpoint| is_usable_endpoint_ip(&endpoint.ip(), allow_loopback)),
    );
    endpoints.sort_by_key(direct_endpoint_priority);
    endpoints.dedup();
    endpoints
}

pub(super) fn direct_endpoints_from_ips(
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
pub(super) struct LocalDirectNetworks {
    ipv4: Vec<Ipv4Network>,
    ipv6: Vec<Ipv6Network>,
}

impl LocalDirectNetworks {
    pub(super) fn current() -> Option<Self> {
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

    pub(super) fn contains(&self, ip: &IpAddr) -> bool {
        match ip {
            IpAddr::V4(ip) => self.ipv4.iter().any(|network| network.contains(ip)),
            IpAddr::V6(ip) => self.ipv6.iter().any(|network| network.contains(ip)),
        }
    }

    #[cfg(test)]
    pub(super) fn from_prefixes(
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

fn route_kind_from_multiaddr(addr: &Multiaddr, enable_turn: bool) -> RouteKind {
    if is_relayed(addr) {
        RouteKind::Libp2pRelay
    } else if is_webrtc(addr) {
        if enable_turn {
            RouteKind::WebRtcTurn
        } else {
            RouteKind::WebRtcDirect
        }
    } else if is_quic(addr) {
        RouteKind::Libp2pQuic
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

pub fn rank_candidates(candidates: impl IntoIterator<Item = Candidate>) -> Vec<Candidate> {
    let mut candidates = candidates.into_iter().collect::<Vec<_>>();
    candidates.sort_by_key(|candidate| match candidate.route {
        RouteKind::LanDirect => 0,
        RouteKind::PublicDirect => 1,
        RouteKind::PublicTunnel => 2,
        RouteKind::Libp2pQuic => 3,
        RouteKind::Libp2pDcutr => 4,
        RouteKind::WebRtcDirect => 5,
        RouteKind::WebRtcTurn => 6,
        RouteKind::TorOnion => 7,
        RouteKind::Libp2pRelay => 8,
    });
    candidates.dedup();
    candidates
}
