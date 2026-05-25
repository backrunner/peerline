use ::pkarr::{
    Client, Keypair, PublicKey, SignedPacket,
    dns::{
        CLASS, ResourceRecord,
        rdata::{NULL, RData},
    },
};
use anyhow::{Context, anyhow};
use libp2p::Multiaddr;
use peerline_core::LookupKey;
use peerline_rendezvous_model::{
    PeerDescriptor, PublicTunnelEndpoint, RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION, TorOnionEndpoint,
};
#[cfg(test)]
use std::sync::{LazyLock, Mutex};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};
use tokio::{sync::mpsc, task::JoinHandle};

const FORMAT_VERSION: u8 = 1;
const RECORD_DIRECT_V4: u8 = 1;
const RECORD_DIRECT_V6: u8 = 2;
const RECORD_LIBP2P: u8 = 3;
const RECORD_TOR: u8 = 4;
const RECORD_PUBLIC_TUNNEL: u8 = 5;
const PKARR_TTL_SECS: u32 = 60;
const PUBLIC_TUNNEL_PROVIDER_LABEL: &str = "pkarr";

pub(crate) const MAX_RAW_PAYLOAD_BYTES: usize = 924;

#[cfg(test)]
static TEST_BOOTSTRAP: LazyLock<Mutex<Option<Vec<String>>>> = LazyLock::new(|| Mutex::new(None));

pub(crate) struct Publisher {
    client: Client,
    keypair: Keypair,
    last_payload: Option<Vec<u8>>,
}

struct PublishRequest {
    descriptor: PeerDescriptor,
    force: bool,
}

pub(crate) struct PublishWorker {
    sender: mpsc::UnboundedSender<PublishRequest>,
    join: JoinHandle<()>,
}

impl Publisher {
    pub(crate) fn new(lookup_key: &LookupKey, timeout: Duration) -> anyhow::Result<Self> {
        Ok(Self {
            client: build_client(timeout)?,
            keypair: keypair_for_lookup_key(lookup_key),
            last_payload: None,
        })
    }

    pub(crate) async fn publish_descriptor(
        &mut self,
        descriptor: &PeerDescriptor,
        force: bool,
    ) -> anyhow::Result<()> {
        let announcement = PkarrAnnouncementV1::from_descriptor(descriptor)?;
        let payload = announcement.encode_with_budget(MAX_RAW_PAYLOAD_BYTES)?;
        if !force && self.last_payload.as_deref() == Some(payload.as_slice()) {
            return Ok(());
        }

        let signed_packet = build_signed_packet(&self.keypair, &payload)?;
        self.client
            .publish(&signed_packet, None)
            .await
            .context("pkarr publish failed")?;
        self.last_payload = Some(payload);

        Ok(())
    }
}

impl PublishWorker {
    pub(crate) fn new(lookup_key: &LookupKey, timeout: Duration) -> anyhow::Result<Self> {
        let publisher = Publisher::new(lookup_key, timeout)?;
        let (sender, mut receiver) = mpsc::unbounded_channel::<PublishRequest>();
        let join = tokio::spawn(async move {
            let mut publisher = publisher;
            while let Some(request) = receiver.recv().await {
                if let Err(error) = publisher
                    .publish_descriptor(&request.descriptor, request.force)
                    .await
                {
                    tracing::warn!(
                        %error,
                        peer_id = %request.descriptor.peer_id,
                        "pkarr publish failed"
                    );
                }
            }
        });

        Ok(Self { sender, join })
    }

    pub(crate) fn publish_descriptor(&self, descriptor: &PeerDescriptor, force: bool) {
        let _ = self.sender.send(PublishRequest {
            descriptor: descriptor.clone(),
            force,
        });
    }
}

impl Drop for PublishWorker {
    fn drop(&mut self) {
        self.join.abort();
    }
}

pub(crate) async fn resolve_peer_descriptor(
    lookup_key: &LookupKey,
    timeout: Duration,
) -> anyhow::Result<Option<PeerDescriptor>> {
    let client = build_client(timeout)?;
    let public_key = public_key_for_lookup_key(lookup_key);
    let Some(signed_packet) = client.resolve_most_recent(&public_key).await else {
        return Ok(None);
    };

    Ok(Some(PkarrAnnouncementV1::from_signed_packet(
        &signed_packet,
    )?))
}

pub(crate) fn public_key_for_lookup_key(lookup_key: &LookupKey) -> PublicKey {
    keypair_for_lookup_key(lookup_key).public_key()
}

fn build_client(timeout: Duration) -> anyhow::Result<Client> {
    let mut builder = Client::builder();
    if let Some(bootstrap) = configured_bootstrap() {
        builder.no_default_network().bootstrap(&bootstrap);
        if bootstrap_targets_localhost(&bootstrap) {
            builder.dht(|dht| dht.bind_address(Ipv4Addr::LOCALHOST));
        }
    } else {
        builder.no_relays();
    }
    builder.no_relays().request_timeout(timeout);
    builder.build().context("could not build pkarr client")
}

fn keypair_for_lookup_key(lookup_key: &LookupKey) -> Keypair {
    Keypair::from_secret_key(&secret_key_for_lookup_key(lookup_key))
}

fn secret_key_for_lookup_key(lookup_key: &LookupKey) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"peerline:pkarr:v1");
    hasher.update(&lookup_key.bytes());
    *hasher.finalize().as_bytes()
}

fn build_signed_packet(keypair: &Keypair, payload: &[u8]) -> anyhow::Result<SignedPacket> {
    let record = ResourceRecord::new(
        ".".try_into().expect("apex record name should be valid"),
        CLASS::IN,
        PKARR_TTL_SECS,
        RData::NULL(
            10,
            NULL::new(payload).context("pkarr payload exceeds NULL RR limit")?,
        ),
    );

    SignedPacket::builder()
        .record(record)
        .sign(keypair)
        .context("pkarr signed packet build failed")
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PkarrAnnouncementV1 {
    peer_id: libp2p::PeerId,
    records: Vec<AnnouncementRecord>,
}

impl PkarrAnnouncementV1 {
    fn from_descriptor(descriptor: &PeerDescriptor) -> anyhow::Result<Self> {
        let peer_id = descriptor
            .peer_id
            .parse::<libp2p::PeerId>()
            .context("descriptor peer_id is invalid for pkarr")?;
        let mut records = Vec::new();

        records.extend(
            collect_direct_endpoints(&descriptor.direct_endpoints)
                .into_iter()
                .map(AnnouncementRecord::Direct),
        );
        records.extend(
            collect_tor_endpoints(&descriptor.tor_endpoints)
                .into_iter()
                .map(AnnouncementRecord::Tor),
        );
        records.extend(
            collect_libp2p_endpoints(&descriptor.libp2p_endpoints)
                .into_iter()
                .map(AnnouncementRecord::Libp2p),
        );
        records.extend(
            collect_public_tunnel_endpoints(&descriptor.public_endpoints)
                .into_iter()
                .map(AnnouncementRecord::PublicTunnel),
        );

        if records.is_empty() {
            anyhow::bail!("descriptor has no publishable endpoints for pkarr");
        }

        Ok(Self { peer_id, records })
    }

    fn from_signed_packet(packet: &SignedPacket) -> anyhow::Result<PeerDescriptor> {
        let payload = packet
            .resource_records("@")
            .find_map(|record| match &record.rdata {
                RData::NULL(_, data) => Some(data.get_data()),
                _ => None,
            })
            .ok_or_else(|| anyhow!("pkarr packet is missing apex NULL payload"))?;
        let announcement = Self::decode(payload)?;
        Ok(announcement.into_descriptor(packet.timestamp().as_u64() / 1_000))
    }

    fn encode_with_budget(&self, max_bytes: usize) -> anyhow::Result<Vec<u8>> {
        let peer_id_bytes = self.peer_id.to_bytes();
        if peer_id_bytes.len() > u8::MAX as usize {
            anyhow::bail!("peer_id is too large for pkarr announcement");
        }

        let mut payload = Vec::with_capacity(max_bytes.min(128));
        payload.push(FORMAT_VERSION);
        payload.push(peer_id_bytes.len() as u8);
        payload.extend_from_slice(&peer_id_bytes);

        let mut packed = self
            .records
            .iter()
            .map(PackedRecord::from_record)
            .collect::<Vec<_>>();
        packed.sort_by_key(|record| (record.priority, record.data.len(), record.kind));

        let mut included = 0usize;
        for record in packed {
            if record.data.len() > u8::MAX as usize {
                continue;
            }
            let needed = 2 + record.data.len();
            if payload.len() + needed > max_bytes {
                continue;
            }
            payload.push(record.kind);
            payload.push(record.data.len() as u8);
            payload.extend_from_slice(&record.data);
            included += 1;
        }

        if included == 0 {
            anyhow::bail!("pkarr announcement budget left no usable endpoints");
        }

        Ok(payload)
    }

    fn decode(payload: &[u8]) -> anyhow::Result<Self> {
        if payload.len() < 2 {
            anyhow::bail!("pkarr announcement is too short");
        }
        if payload[0] != FORMAT_VERSION {
            anyhow::bail!("unsupported pkarr announcement version {}", payload[0]);
        }

        let peer_id_len = payload[1] as usize;
        let header_len = 2 + peer_id_len;
        if payload.len() < header_len {
            anyhow::bail!("pkarr announcement peer_id is truncated");
        }

        let peer_id = libp2p::PeerId::from_bytes(&payload[2..header_len])
            .context("pkarr announcement peer_id is invalid")?;
        let mut records = Vec::new();
        let mut cursor = header_len;

        while cursor < payload.len() {
            if cursor + 2 > payload.len() {
                anyhow::bail!("pkarr announcement record header is truncated");
            }
            let kind = payload[cursor];
            let len = payload[cursor + 1] as usize;
            cursor += 2;
            if cursor + len > payload.len() {
                anyhow::bail!("pkarr announcement record payload is truncated");
            }

            records.push(AnnouncementRecord::decode(
                kind,
                &payload[cursor..cursor + len],
            )?);
            cursor += len;
        }

        if records.is_empty() {
            anyhow::bail!("pkarr announcement contained no endpoints");
        }

        Ok(Self { peer_id, records })
    }

    fn into_descriptor(self, published_unix_ms: u64) -> PeerDescriptor {
        let mut direct_endpoints = Vec::new();
        let mut libp2p_endpoints = Vec::new();
        let mut public_endpoints = Vec::new();
        let mut tor_endpoints = Vec::new();

        for record in self.records {
            match record {
                AnnouncementRecord::Direct(endpoint) => direct_endpoints.push(endpoint.to_string()),
                AnnouncementRecord::Libp2p(addr) => libp2p_endpoints.push(addr.to_string()),
                AnnouncementRecord::Tor(url) => tor_endpoints.push(TorOnionEndpoint { url }),
                AnnouncementRecord::PublicTunnel(url) => {
                    public_endpoints.push(PublicTunnelEndpoint {
                        provider: PUBLIC_TUNNEL_PROVIDER_LABEL.into(),
                        url,
                    });
                }
            }
        }

        PeerDescriptor {
            protocol_version: RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION,
            peer_id: self.peer_id.to_string(),
            direct_endpoints,
            libp2p_endpoints,
            public_endpoints,
            tor_endpoints,
            published_unix_ms,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AnnouncementRecord {
    Direct(SocketAddr),
    Libp2p(Multiaddr),
    Tor(String),
    PublicTunnel(String),
}

impl AnnouncementRecord {
    fn decode(kind: u8, data: &[u8]) -> anyhow::Result<Self> {
        match kind {
            RECORD_DIRECT_V4 => {
                if data.len() != 6 {
                    anyhow::bail!("invalid IPv4 direct endpoint length {}", data.len());
                }
                let ip = IpAddr::V4(std::net::Ipv4Addr::new(data[0], data[1], data[2], data[3]));
                let port = u16::from_be_bytes([data[4], data[5]]);
                Ok(Self::Direct(SocketAddr::new(ip, port)))
            }
            RECORD_DIRECT_V6 => {
                if data.len() != 18 {
                    anyhow::bail!("invalid IPv6 direct endpoint length {}", data.len());
                }
                let mut ip = [0u8; 16];
                ip.copy_from_slice(&data[..16]);
                let port = u16::from_be_bytes([data[16], data[17]]);
                Ok(Self::Direct(SocketAddr::new(
                    IpAddr::V6(std::net::Ipv6Addr::from(ip)),
                    port,
                )))
            }
            RECORD_LIBP2P => Ok(Self::Libp2p(
                Multiaddr::try_from(data.to_vec()).context("invalid libp2p multiaddr bytes")?,
            )),
            RECORD_TOR => {
                let url = String::from_utf8(data.to_vec()).context("Tor endpoint is not UTF-8")?;
                Ok(Self::Tor(
                    crate::tunnel::normalize_tor_onion_url(&url)
                        .context("invalid Tor endpoint URL")?,
                ))
            }
            RECORD_PUBLIC_TUNNEL => {
                let url = String::from_utf8(data.to_vec())
                    .context("public tunnel endpoint is not UTF-8")?;
                Ok(Self::PublicTunnel(
                    crate::tunnel::normalize_public_tunnel_url(&url)
                        .context("invalid public tunnel URL")?,
                ))
            }
            other => anyhow::bail!("unsupported pkarr announcement record kind {other}"),
        }
    }
}

#[derive(Debug)]
struct PackedRecord {
    kind: u8,
    priority: u8,
    data: Vec<u8>,
}

impl PackedRecord {
    fn from_record(record: &AnnouncementRecord) -> Self {
        match record {
            AnnouncementRecord::Direct(endpoint) => match endpoint.ip() {
                IpAddr::V4(ip) => Self {
                    kind: RECORD_DIRECT_V4,
                    priority: direct_record_priority(endpoint),
                    data: ip
                        .octets()
                        .into_iter()
                        .chain(endpoint.port().to_be_bytes())
                        .collect(),
                },
                IpAddr::V6(ip) => Self {
                    kind: RECORD_DIRECT_V6,
                    priority: direct_record_priority(endpoint),
                    data: ip
                        .octets()
                        .into_iter()
                        .chain(endpoint.port().to_be_bytes())
                        .collect(),
                },
            },
            AnnouncementRecord::Libp2p(addr) => Self {
                kind: RECORD_LIBP2P,
                priority: libp2p_record_priority(addr),
                data: addr.to_vec(),
            },
            AnnouncementRecord::Tor(url) => Self {
                kind: RECORD_TOR,
                priority: 2,
                data: url.as_bytes().to_vec(),
            },
            AnnouncementRecord::PublicTunnel(url) => Self {
                kind: RECORD_PUBLIC_TUNNEL,
                priority: 6,
                data: url.as_bytes().to_vec(),
            },
        }
    }
}

fn collect_direct_endpoints(raw: &[String]) -> Vec<SocketAddr> {
    let mut endpoints = raw
        .iter()
        .filter_map(|endpoint| endpoint.parse::<SocketAddr>().ok())
        .filter(|endpoint| is_publishable_endpoint_ip(&endpoint.ip()))
        .collect::<Vec<_>>();
    endpoints.sort_by_key(direct_record_priority);
    endpoints.dedup();
    endpoints
}

fn collect_libp2p_endpoints(raw: &[String]) -> Vec<Multiaddr> {
    let mut endpoints = raw
        .iter()
        .filter_map(|endpoint| endpoint.parse::<Multiaddr>().ok())
        .filter(is_dialable_multiaddr)
        .collect::<Vec<_>>();
    endpoints.sort_by_key(libp2p_record_priority);
    endpoints.dedup();
    endpoints
}

fn collect_public_tunnel_endpoints(raw: &[PublicTunnelEndpoint]) -> Vec<String> {
    let mut endpoints = raw
        .iter()
        .filter_map(|endpoint| crate::tunnel::normalize_public_tunnel_url(&endpoint.url).ok())
        .collect::<Vec<_>>();
    endpoints.sort();
    endpoints.dedup();
    endpoints
}

fn collect_tor_endpoints(raw: &[TorOnionEndpoint]) -> Vec<String> {
    let mut endpoints = raw
        .iter()
        .filter_map(|endpoint| crate::tunnel::normalize_tor_onion_url(&endpoint.url).ok())
        .collect::<Vec<_>>();
    endpoints.sort();
    endpoints.dedup();
    endpoints
}

fn is_publishable_endpoint_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            !ip.is_unspecified()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_multicast()
                && !ip.is_broadcast()
        }
        IpAddr::V6(ip) => !ip.is_unspecified() && !ip.is_loopback() && !ip.is_multicast(),
    }
}

fn direct_record_priority(endpoint: &SocketAddr) -> u8 {
    match endpoint.ip() {
        IpAddr::V4(ip) if ip.is_private() => 0,
        IpAddr::V6(ip) if ip.is_unique_local() => 0,
        IpAddr::V4(_) | IpAddr::V6(_) => 1,
    }
}

fn libp2p_record_priority(addr: &Multiaddr) -> u8 {
    if is_relayed(addr) {
        7
    } else if is_webrtc(addr) {
        5
    } else if is_quic(addr) {
        4
    } else {
        3
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

fn bootstrap_from_env(name: &str) -> Option<Vec<String>> {
    std::env::var(name).ok().map(|raw| {
        raw.split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    })
}

fn configured_bootstrap() -> Option<Vec<String>> {
    #[cfg(test)]
    if let Some(bootstrap) = TEST_BOOTSTRAP.lock().unwrap().clone() {
        return Some(bootstrap);
    }

    bootstrap_from_env("PEERLINE_PKARR_BOOTSTRAP")
}

fn bootstrap_targets_localhost(bootstrap: &[String]) -> bool {
    !bootstrap.is_empty()
        && bootstrap
            .iter()
            .all(|value| value.starts_with("127.0.0.1:") || value.starts_with("localhost:"))
}

#[cfg(test)]
pub(crate) struct TestBootstrapGuard;

#[cfg(test)]
pub(crate) fn set_test_bootstrap(bootstrap: Vec<String>) -> TestBootstrapGuard {
    *TEST_BOOTSTRAP.lock().unwrap() = Some(bootstrap);
    TestBootstrapGuard
}

#[cfg(test)]
impl Drop for TestBootstrapGuard {
    fn drop(&mut self) {
        *TEST_BOOTSTRAP.lock().unwrap() = None;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_RAW_PAYLOAD_BYTES, PUBLIC_TUNNEL_PROVIDER_LABEL, PackedRecord, PkarrAnnouncementV1,
        build_signed_packet, public_key_for_lookup_key,
    };
    use libp2p::PeerId;
    use peerline_core::{HumanCode, HumanName, NameCode};
    use peerline_rendezvous_model::{
        PeerDescriptor, PublicTunnelEndpoint, RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION,
        TorOnionEndpoint,
    };

    #[test]
    fn pkarr_public_key_derivation_is_deterministic() {
        let name = HumanName::parse("amber-123").unwrap();
        let code = HumanCode::parse("cedar-cloud-mint-123456").unwrap();
        let lookup_key = NameCode::new(name.clone(), code.clone()).lookup_key();
        let lookup_key_again = NameCode::new(name, code).lookup_key();

        assert_eq!(
            public_key_for_lookup_key(&lookup_key).to_z32(),
            public_key_for_lookup_key(&lookup_key_again).to_z32()
        );
    }

    #[test]
    fn pkarr_announcement_roundtrips_through_signed_packet() {
        let descriptor = sample_descriptor();
        let announcement = PkarrAnnouncementV1::from_descriptor(&descriptor).unwrap();
        let payload = announcement
            .encode_with_budget(MAX_RAW_PAYLOAD_BYTES)
            .unwrap();
        let lookup_key = NameCode::new(
            HumanName::parse("river-123").unwrap(),
            HumanCode::parse("cedar-cloud-mint-123456").unwrap(),
        )
        .lookup_key();
        let signed_packet =
            build_signed_packet(&super::keypair_for_lookup_key(&lookup_key), &payload).unwrap();

        let decoded = PkarrAnnouncementV1::from_signed_packet(&signed_packet).unwrap();

        assert_eq!(
            decoded.protocol_version,
            RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION
        );
        assert_eq!(decoded.peer_id, descriptor.peer_id);
        assert_eq!(decoded.direct_endpoints, descriptor.direct_endpoints);
        assert_eq!(decoded.libp2p_endpoints, descriptor.libp2p_endpoints);
        assert_eq!(
            decoded
                .public_endpoints
                .iter()
                .map(|endpoint| endpoint.url.clone())
                .collect::<Vec<_>>(),
            descriptor
                .public_endpoints
                .iter()
                .map(|endpoint| endpoint.url.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(decoded.tor_endpoints, descriptor.tor_endpoints);
    }

    #[test]
    fn pkarr_announcement_fits_dns_packet_budget() {
        let descriptor = expanded_descriptor();
        let announcement = PkarrAnnouncementV1::from_descriptor(&descriptor).unwrap();
        let payload = announcement
            .encode_with_budget(MAX_RAW_PAYLOAD_BYTES)
            .unwrap();
        let lookup_key = NameCode::new(
            HumanName::parse("lagoon-123").unwrap(),
            HumanCode::parse("cedar-cloud-mint-123456").unwrap(),
        )
        .lookup_key();
        let signed_packet =
            build_signed_packet(&super::keypair_for_lookup_key(&lookup_key), &payload).unwrap();

        assert!(payload.len() <= MAX_RAW_PAYLOAD_BYTES);
        assert!(signed_packet.encoded_packet().len() <= 1_000);
    }

    #[test]
    fn pkarr_budget_keeps_high_priority_records_first() {
        let descriptor = sample_descriptor();
        let announcement = PkarrAnnouncementV1::from_descriptor(&descriptor).unwrap();
        let peer_len =
            PeerId::from_bytes(&descriptor.peer_id.parse::<PeerId>().unwrap().to_bytes())
                .unwrap()
                .to_bytes()
                .len();
        let direct_only_budget = 2 + peer_len + 2 + 6;

        let payload = announcement.encode_with_budget(direct_only_budget).unwrap();
        let decoded = PkarrAnnouncementV1::decode(&payload).unwrap();

        assert_eq!(decoded.records.len(), 1);
        match &decoded.records[0] {
            super::AnnouncementRecord::Direct(endpoint) => {
                assert_eq!(endpoint.to_string(), "192.168.1.20:43117");
            }
            other => panic!("unexpected record retained under tight budget: {other:?}"),
        }
    }

    #[test]
    fn pkarr_public_tunnel_provider_label_is_stable() {
        assert_eq!(PUBLIC_TUNNEL_PROVIDER_LABEL, "pkarr");
    }

    #[test]
    fn packed_record_prefers_smaller_same_priority_entries() {
        let a = PackedRecord::from_record(&super::AnnouncementRecord::Tor("ws://a.onion/".into()));
        let b = PackedRecord::from_record(&super::AnnouncementRecord::Tor(
            "ws://abcdefghijklmnopqrstuvwxyzabcdefghijklmnop.onion/".into(),
        ));
        assert!(a.data.len() < b.data.len());
    }

    fn sample_descriptor() -> PeerDescriptor {
        let peer_id = PeerId::random();
        PeerDescriptor {
            protocol_version: RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION,
            peer_id: peer_id.to_string(),
            direct_endpoints: vec!["192.168.1.20:43117".into(), "203.0.113.7:43117".into()],
            libp2p_endpoints: vec![
                format!("/ip4/203.0.113.7/tcp/43118/p2p/{peer_id}"),
                format!("/ip4/203.0.113.7/udp/43119/quic-v1/p2p/{peer_id}"),
            ],
            public_endpoints: vec![PublicTunnelEndpoint {
                provider: "cloudflared".into(),
                url: crate::tunnel::normalize_public_tunnel_url("https://example.com/transfer")
                    .unwrap(),
            }],
            tor_endpoints: vec![TorOnionEndpoint {
                url: crate::tunnel::normalize_tor_onion_url(
                    "abcdefghijklmnopqrstuvwxyzabcdefghijklmnop.onion",
                )
                .unwrap(),
            }],
            published_unix_ms: 0,
        }
    }

    fn expanded_descriptor() -> PeerDescriptor {
        let peer_id = PeerId::random();
        PeerDescriptor {
            protocol_version: RENDEZVOUS_DESCRIPTOR_PROTOCOL_VERSION,
            peer_id: peer_id.to_string(),
            direct_endpoints: vec![
                "192.168.1.20:43117".into(),
                "203.0.113.7:43117".into(),
                "[2001:db8::7]:43117".into(),
            ],
            libp2p_endpoints: vec![
                format!("/ip4/203.0.113.7/tcp/43118/p2p/{peer_id}"),
                format!("/ip4/203.0.113.7/udp/43119/quic-v1/p2p/{peer_id}"),
                format!("/ip4/203.0.113.7/udp/43120/webrtc-direct/p2p/{peer_id}"),
                format!(
                    "/dns4/relay.example.net/tcp/443/wss/p2p/{peer_id}/p2p-circuit/p2p/{peer_id}"
                ),
            ],
            public_endpoints: vec![
                PublicTunnelEndpoint {
                    provider: "cloudflared".into(),
                    url: crate::tunnel::normalize_public_tunnel_url("https://example.com/transfer")
                        .unwrap(),
                },
                PublicTunnelEndpoint {
                    provider: "localtunnel".into(),
                    url: crate::tunnel::normalize_public_tunnel_url(
                        "https://example.net/upload/socket",
                    )
                    .unwrap(),
                },
            ],
            tor_endpoints: vec![TorOnionEndpoint {
                url: crate::tunnel::normalize_tor_onion_url(
                    "abcdefghijklmnopqrstuvwxyzabcdefghijklmnop.onion",
                )
                .unwrap(),
            }],
            published_unix_ms: 0,
        }
    }
}
