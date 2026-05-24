use crate::{
    protocol::{
        PROTOCOL_VERSION, SecureFrame, WireFrame, direct_transcript, read_secure, read_wire,
        write_secure, write_wire,
    },
    resume,
};
use peerline_core::{
    Compression, ConnectionRoute, DEFAULT_DIRECT_PORT, DEFAULT_DIRECT_PORT_WINDOW, HumanCode,
    HumanName, NodeId, PeerlineEvent, TransferDescriptor, TransferId, TransferStage,
    direct_port_candidates,
};
use peerline_crypto::{
    ChunkAead, ClientHandshake, OpaqueClientStart, ServerHello, Transcript, create_server_record,
    start_client_login, start_server_login,
};
use peerline_transfer::{archive::Archive, create_archive, unpack_archive_from_reader};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinHandle,
    time,
};

const DIRECT_PORT_PROBE_TIMEOUT: Duration = Duration::from_secs(1);
const DIRECT_SESSION_OPEN_TIMEOUT: Duration = Duration::from_secs(10);
const DIRECT_UPNP_MAPPING_LEASE_SECONDS: u32 = 3600;
const DIRECT_UPNP_RENEW_AFTER: Duration =
    Duration::from_secs(DIRECT_UPNP_MAPPING_LEASE_SECONDS as u64 / 2);
const DIRECT_UPNP_RETRY_AFTER: Duration = Duration::from_secs(60);
const DIRECT_UPNP_DESCRIPTION: &str = "peerline direct tcp";

#[derive(Clone, Debug)]
pub struct RecvOptions {
    pub name: HumanName,
    pub code: HumanCode,
    pub bind: SocketAddr,
    pub destination: PathBuf,
    pub overwrite: bool,
    pub events: Option<tokio::sync::mpsc::UnboundedSender<PeerlineEvent>>,
}

#[derive(Clone, Debug)]
pub struct SendOptions {
    pub endpoint: SocketAddr,
    pub name: Option<HumanName>,
    pub code: HumanCode,
    pub source_id: NodeId,
    pub paths: Vec<PathBuf>,
    pub compression: Compression,
    pub events: Option<tokio::sync::mpsc::UnboundedSender<PeerlineEvent>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceivedTransfer {
    pub peer: String,
    pub files: usize,
    pub bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SentTransfer {
    pub endpoint: String,
    pub files: usize,
    pub bytes: u64,
}

pub(crate) struct DirectPortMapping {
    receiver: watch::Receiver<Vec<SocketAddr>>,
    _join: JoinHandle<()>,
}

impl DirectPortMapping {
    pub(crate) fn subscribe(&self) -> watch::Receiver<Vec<SocketAddr>> {
        self.receiver.clone()
    }

    pub(crate) fn endpoints(&self) -> Vec<SocketAddr> {
        self.receiver.borrow().clone()
    }
}

impl Default for RecvOptions {
    fn default() -> Self {
        Self {
            name: HumanName::generate(),
            code: HumanCode::generate(),
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), DEFAULT_DIRECT_PORT),
            destination: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            overwrite: false,
            events: None,
        }
    }
}

pub(crate) fn spawn_direct_port_mapping(bind: SocketAddr) -> DirectPortMapping {
    let (sender, receiver) = watch::channel(Vec::new());
    let join = tokio::spawn(async move {
        run_direct_port_mapping(bind, sender).await;
    });
    DirectPortMapping {
        receiver,
        _join: join,
    }
}

async fn run_direct_port_mapping(bind: SocketAddr, sender: watch::Sender<Vec<SocketAddr>>) {
    if bind.port() == 0 {
        tracing::debug!("skipping direct TCP UPnP mapping for ephemeral port");
        return;
    }

    loop {
        if sender.is_closed() {
            return;
        }

        match try_direct_port_mapping(bind).await {
            Ok(Some(mapping)) => {
                let external_endpoint = mapping.external_endpoint;
                tracing::debug!(
                    local_endpoint = %mapping.local_endpoint,
                    %external_endpoint,
                    "direct TCP UPnP port mapping established"
                );
                let _ = sender.send(vec![external_endpoint]);
                maintain_direct_port_mapping(mapping, &sender).await;
            }
            Ok(None) => {
                let _ = sender.send(Vec::new());
                tracing::debug!("direct TCP UPnP mapping unavailable");
            }
            Err(error) => {
                let _ = sender.send(Vec::new());
                tracing::debug!(%error, "direct TCP UPnP mapping failed");
            }
        }

        tokio::select! {
            _ = sender.closed() => return,
            _ = time::sleep(DIRECT_UPNP_RETRY_AFTER) => {}
        }
    }
}

type TokioGateway = igd_next::aio::Gateway<igd_next::aio::tokio::Tokio>;

struct DirectPortMappingLease {
    gateway: TokioGateway,
    local_endpoint: SocketAddr,
    external_endpoint: SocketAddr,
}

async fn try_direct_port_mapping(
    bind: SocketAddr,
) -> anyhow::Result<Option<DirectPortMappingLease>> {
    let local_endpoints = direct_port_mapping_local_endpoints(bind);
    if local_endpoints.is_empty() {
        tracing::debug!(%bind, "no local IPv4 address available for direct TCP UPnP mapping");
        return Ok(None);
    }

    let gateway = igd_next::aio::tokio::search_gateway(igd_next::SearchOptions {
        timeout: Some(Duration::from_secs(5)),
        single_search_timeout: Some(Duration::from_secs(2)),
        ..Default::default()
    })
    .await?;
    let external_ip = gateway.get_external_ip().await?;
    if !is_public_endpoint_ip(&external_ip) {
        tracing::debug!(%external_ip, "direct TCP UPnP gateway external address is not public");
        return Ok(None);
    }

    for local_endpoint in local_endpoints {
        match gateway
            .add_port(
                igd_next::PortMappingProtocol::TCP,
                bind.port(),
                local_endpoint,
                DIRECT_UPNP_MAPPING_LEASE_SECONDS,
                DIRECT_UPNP_DESCRIPTION,
            )
            .await
        {
            Ok(()) => {
                return Ok(Some(DirectPortMappingLease {
                    gateway,
                    local_endpoint,
                    external_endpoint: SocketAddr::new(external_ip, bind.port()),
                }));
            }
            Err(error) => {
                tracing::debug!(
                    %local_endpoint,
                    %error,
                    "could not map direct TCP UPnP port with matching external port"
                );
            }
        }

        match gateway
            .add_any_port(
                igd_next::PortMappingProtocol::TCP,
                local_endpoint,
                DIRECT_UPNP_MAPPING_LEASE_SECONDS,
                DIRECT_UPNP_DESCRIPTION,
            )
            .await
        {
            Ok(external_port) => {
                return Ok(Some(DirectPortMappingLease {
                    gateway,
                    local_endpoint,
                    external_endpoint: SocketAddr::new(external_ip, external_port),
                }));
            }
            Err(error) => {
                tracing::debug!(
                    %local_endpoint,
                    %error,
                    "could not map direct TCP UPnP port with router-selected external port"
                );
            }
        }
    }

    Ok(None)
}

async fn maintain_direct_port_mapping(
    mapping: DirectPortMappingLease,
    sender: &watch::Sender<Vec<SocketAddr>>,
) {
    loop {
        tokio::select! {
            _ = sender.closed() => {
                remove_direct_port_mapping(&mapping).await;
                return;
            }
            _ = time::sleep(DIRECT_UPNP_RENEW_AFTER) => {}
        }

        match mapping
            .gateway
            .add_port(
                igd_next::PortMappingProtocol::TCP,
                mapping.external_endpoint.port(),
                mapping.local_endpoint,
                DIRECT_UPNP_MAPPING_LEASE_SECONDS,
                DIRECT_UPNP_DESCRIPTION,
            )
            .await
        {
            Ok(()) => {
                tracing::debug!(
                    local_endpoint = %mapping.local_endpoint,
                    external_endpoint = %mapping.external_endpoint,
                    "direct TCP UPnP port mapping renewed"
                );
            }
            Err(error) => {
                tracing::debug!(
                    local_endpoint = %mapping.local_endpoint,
                    external_endpoint = %mapping.external_endpoint,
                    %error,
                    "direct TCP UPnP port mapping renewal failed"
                );
                let _ = sender.send(Vec::new());
                return;
            }
        }
    }
}

async fn remove_direct_port_mapping(mapping: &DirectPortMappingLease) {
    if let Err(error) = mapping
        .gateway
        .remove_port(
            igd_next::PortMappingProtocol::TCP,
            mapping.external_endpoint.port(),
        )
        .await
    {
        tracing::debug!(
            external_endpoint = %mapping.external_endpoint,
            %error,
            "could not remove direct TCP UPnP port mapping"
        );
    }
}

fn direct_port_mapping_local_endpoints(bind: SocketAddr) -> Vec<SocketAddr> {
    match bind.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => {
            let mut endpoints = local_ip_address::list_afinet_netifas()
                .map(|interfaces| {
                    interfaces
                        .into_iter()
                        .filter_map(|(_, ip)| match ip {
                            IpAddr::V4(ip) if is_usable_mapping_ipv4(&ip) => {
                                Some(SocketAddr::new(IpAddr::V4(ip), bind.port()))
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            endpoints.sort();
            endpoints.dedup();
            endpoints
        }
        IpAddr::V4(ip) if is_usable_mapping_ipv4(&ip) => vec![bind],
        _ => Vec::new(),
    }
}

fn is_usable_mapping_ipv4(ip: &Ipv4Addr) -> bool {
    !ip.is_unspecified()
        && !ip.is_loopback()
        && !ip.is_link_local()
        && !ip.is_multicast()
        && !ip.is_broadcast()
}

fn is_public_endpoint_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            !(ip.octets()[0] == 0
                || ip.is_private()
                || (ip.octets()[0] == 100 && (ip.octets()[1] & 0b1100_0000 == 0b0100_0000))
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_documentation()
                || ip.is_multicast()
                || ip.is_broadcast()
                || (ip.octets()[0] & 240 == 240 && !ip.is_broadcast()))
        }
        IpAddr::V6(ip) => {
            !(ip.is_unspecified()
                || ip.is_loopback()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || ip.is_multicast())
        }
    }
}

fn emit_event(
    events: &Option<tokio::sync::mpsc::UnboundedSender<PeerlineEvent>>,
    event: PeerlineEvent,
) {
    if let Some(sender) = events {
        let _ = sender.send(event);
    }
}

fn emit_stage(
    events: &Option<tokio::sync::mpsc::UnboundedSender<PeerlineEvent>>,
    stage: TransferStage,
) {
    emit_event(events, PeerlineEvent::StageChanged(stage));
}

fn emit_message(
    events: &Option<tokio::sync::mpsc::UnboundedSender<PeerlineEvent>>,
    message: impl Into<String>,
) {
    emit_event(events, PeerlineEvent::Message(message.into()));
}

fn emit_transfer_started(
    events: &Option<tokio::sync::mpsc::UnboundedSender<PeerlineEvent>>,
    id: TransferId,
    peer: impl Into<String>,
    files: usize,
    bytes: u64,
    resume_offset: u64,
) {
    emit_event(
        events,
        PeerlineEvent::TransferStarted {
            id,
            peer: peer.into(),
            files,
            bytes,
            resume_offset,
        },
    );
}

fn emit_progress(
    events: &Option<tokio::sync::mpsc::UnboundedSender<PeerlineEvent>>,
    id: TransferId,
    bytes_done: u64,
    bytes_total: u64,
) {
    emit_event(
        events,
        PeerlineEvent::Progress {
            id,
            bytes_done,
            bytes_total,
        },
    );
}

fn connection_route_from_endpoint(endpoint: &SocketAddr) -> ConnectionRoute {
    match endpoint.ip() {
        IpAddr::V4(ip) if ip.is_private() || ip.is_loopback() => ConnectionRoute::LanDirect,
        IpAddr::V6(ip) if ip.is_unique_local() || ip.is_loopback() => ConnectionRoute::LanDirect,
        _ => ConnectionRoute::PublicDirect,
    }
}

struct DirectSession {
    endpoint: SocketAddr,
    stream: TcpStream,
    opaque_client: OpaqueClientStart,
    server_response: Vec<u8>,
    client_handshake: ClientHandshake,
    server_hello: ServerHello,
    transcript: Transcript,
    resume_offset: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferErrorKind {
    Transient,
    Fatal,
}

#[derive(Debug)]
pub struct TransferError {
    kind: TransferErrorKind,
    source: anyhow::Error,
}

impl TransferError {
    pub fn transient(error: impl Into<anyhow::Error>) -> Self {
        Self {
            kind: TransferErrorKind::Transient,
            source: error.into(),
        }
    }

    pub fn fatal(error: impl Into<anyhow::Error>) -> Self {
        Self {
            kind: TransferErrorKind::Fatal,
            source: error.into(),
        }
    }

    pub fn kind(&self) -> TransferErrorKind {
        self.kind
    }

    pub fn into_inner(self) -> anyhow::Error {
        self.source
    }
}

impl std::fmt::Display for TransferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(f)
    }
}

impl std::error::Error for TransferError {}

pub async fn bind_direct_listener(bind: SocketAddr) -> anyhow::Result<(TcpListener, SocketAddr)> {
    bind_direct_listener_with_window(bind, DEFAULT_DIRECT_PORT_WINDOW).await
}

pub async fn bind_direct_listener_with_window(
    bind: SocketAddr,
    window: u16,
) -> anyhow::Result<(TcpListener, SocketAddr)> {
    if bind.port() == 0 || window <= 1 {
        let listener = TcpListener::bind(bind).await?;
        let actual_bind = listener.local_addr()?;
        return Ok((listener, actual_bind));
    }

    let mut last_error: Option<anyhow::Error> = None;
    for port in direct_port_candidates(bind.port(), window) {
        let candidate = SocketAddr::new(bind.ip(), port);
        match TcpListener::bind(candidate).await {
            Ok(listener) => {
                let actual_bind = listener.local_addr()?;
                return Ok((listener, actual_bind));
            }
            Err(error) => last_error = Some(error.into()),
        }
    }

    Err(last_error.unwrap_or_else(|| {
        anyhow::anyhow!(
            "could not bind direct listener on {}..{}",
            bind.port(),
            bind.port().saturating_add(window.saturating_sub(1))
        )
    }))
}

async fn open_direct_session(
    endpoint: SocketAddr,
    name: Option<&HumanName>,
    code: &HumanCode,
    descriptor: TransferDescriptor,
) -> anyhow::Result<DirectSession> {
    let mut stream = TcpStream::connect(endpoint).await?;
    let transcript = direct_transcript(name);
    let opaque_client = start_client_login(code.as_str().as_bytes())?;
    let client_handshake = ClientHandshake::start();
    write_wire(
        &mut stream,
        &WireFrame::ClientIntro {
            version: PROTOCOL_VERSION,
            name: name.cloned(),
            descriptor,
            opaque_request: opaque_client.request.clone(),
            client_hello: client_handshake.hello.clone(),
        },
    )
    .await?;

    let server_intro = match read_wire(&mut stream).await? {
        WireFrame::ServerIntro {
            version,
            resume_offset,
            opaque_response,
            server_hello,
        } if version == PROTOCOL_VERSION => (resume_offset, opaque_response, server_hello),
        WireFrame::ServerIntro { version, .. } => {
            return Err(TransferError::fatal(anyhow::anyhow!(
                "incompatible peerline protocol version {version}; expected {PROTOCOL_VERSION}"
            ))
            .into());
        }
        WireFrame::Error { message } => {
            return Err(TransferError::fatal(anyhow::anyhow!(message)).into());
        }
        _ => anyhow::bail!("unexpected server handshake frame"),
    };

    Ok(DirectSession {
        endpoint,
        stream,
        opaque_client,
        resume_offset: server_intro.0,
        server_response: server_intro.1,
        client_handshake,
        server_hello: server_intro.2,
        transcript,
    })
}

async fn open_direct_session_with_timeout(
    endpoint: SocketAddr,
    name: Option<&HumanName>,
    code: &HumanCode,
    descriptor: TransferDescriptor,
    timeout: Duration,
) -> anyhow::Result<DirectSession> {
    match time::timeout(
        timeout,
        open_direct_session(endpoint, name, code, descriptor),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => anyhow::bail!(
            "timed out connecting to {} after {} seconds",
            endpoint,
            timeout.as_secs()
        ),
    }
}

async fn complete_direct_transfer(
    mut session: DirectSession,
    archive: &Archive,
    options: &SendOptions,
    transfer_id: TransferId,
    descriptor: TransferDescriptor,
) -> anyhow::Result<SentTransfer> {
    if session.resume_offset > descriptor.archive_bytes {
        return Err(TransferError::fatal(anyhow::anyhow!(
            "receiver requested resume offset beyond archive size"
        ))
        .into());
    }
    emit_stage(&options.events, TransferStage::Authenticating);
    let opaque_finish = session
        .opaque_client
        .finish(options.code.as_str().as_bytes(), &session.server_response)?;
    let (client_kem, keys) = session.client_handshake.finish(
        &session.server_hello,
        &opaque_finish.session_key,
        &session.transcript,
    )?;
    write_wire(
        &mut session.stream,
        &WireFrame::ClientFinish {
            opaque_finalization: opaque_finish.finalization,
            client_kem,
        },
    )
    .await?;

    let aead = ChunkAead::new(keys.send_key, *b"pl01");
    emit_transfer_started(
        &options.events,
        transfer_id,
        session.endpoint.to_string(),
        descriptor.files,
        descriptor.archive_bytes,
        session.resume_offset,
    );
    if session.resume_offset > 0 {
        emit_message(
            &options.events,
            format!("resuming at {} bytes", session.resume_offset),
        );
    }
    emit_stage(&options.events, TransferStage::Transferring);
    let mut sequence = 0u64;
    let mut bytes_sent = session.resume_offset;
    let mut archive_reader = tokio::fs::File::from_std(archive.reader()?);
    archive_reader
        .seek(std::io::SeekFrom::Start(session.resume_offset))
        .await?;
    write_secure(
        &mut session.stream,
        &aead,
        &mut sequence,
        &SecureFrame::Header {
            compression: archive.compression,
        },
    )
    .await?;
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = archive_reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        bytes_sent += read as u64;
        write_secure(
            &mut session.stream,
            &aead,
            &mut sequence,
            &SecureFrame::ArchiveChunk {
                bytes: buffer[..read].to_vec(),
            },
        )
        .await?;
        emit_progress(
            &options.events,
            transfer_id,
            bytes_sent,
            descriptor.archive_bytes,
        );
    }
    write_secure(
        &mut session.stream,
        &aead,
        &mut sequence,
        &SecureFrame::Done,
    )
    .await?;
    match read_wire(&mut session.stream).await? {
        WireFrame::Ack => {}
        WireFrame::Error { message } => {
            return Err(TransferError::fatal(anyhow::anyhow!(message)).into());
        }
        other => {
            return Err(TransferError::fatal(anyhow::anyhow!(
                "unexpected completion response: {other:?}"
            ))
            .into());
        }
    }
    session.stream.shutdown().await?;
    emit_stage(&options.events, TransferStage::Complete);

    Ok(SentTransfer {
        endpoint: session.endpoint.to_string(),
        files: descriptor.files,
        bytes: archive.manifest.total_bytes,
    })
}

pub async fn recv_once(options: RecvOptions) -> anyhow::Result<ReceivedTransfer> {
    emit_event(
        &options.events,
        PeerlineEvent::StageChanged(TransferStage::Discovering),
    );
    let (listener, _) =
        bind_direct_listener_with_window(options.bind, DEFAULT_DIRECT_PORT_WINDOW).await?;
    recv_once_bound(&listener, options).await
}

pub async fn recv_once_bound(
    listener: &TcpListener,
    options: RecvOptions,
) -> anyhow::Result<ReceivedTransfer> {
    loop {
        let (stream, peer) = listener.accept().await?;
        emit_event(
            &options.events,
            PeerlineEvent::StageChanged(TransferStage::Connecting(ConnectionRoute::LanDirect)),
        );
        match receive_stream(stream, peer, options.clone()).await {
            Ok(result) => return Ok(result),
            Err(error) => {
                tracing::warn!(%error, %peer, "direct transfer failed; waiting for another connection");
                emit_event(
                    &options.events,
                    PeerlineEvent::StageChanged(TransferStage::Failed(error.to_string())),
                );
                emit_event(
                    &options.events,
                    PeerlineEvent::Message(format!("{peer}: {}", error)),
                );
            }
        }
    }
}

pub async fn send_direct(options: SendOptions) -> anyhow::Result<SentTransfer> {
    emit_event(
        &options.events,
        PeerlineEvent::Message("building archive from selected paths".into()),
    );
    let archive = create_archive(&options.paths, options.compression)?;
    let transfer_id = TransferId::random();
    send_prebuilt_direct(options, archive, transfer_id).await
}

pub async fn send_prebuilt_direct(
    options: SendOptions,
    archive: Archive,
    transfer_id: TransferId,
) -> anyhow::Result<SentTransfer> {
    let descriptor = descriptor_for_archive(&options, &archive);
    emit_stage(
        &options.events,
        TransferStage::Connecting(connection_route_from_endpoint(&options.endpoint)),
    );
    emit_message(&options.events, format!("dialing {}", options.endpoint));
    let session = open_direct_session_with_timeout(
        options.endpoint,
        options.name.as_ref(),
        &options.code,
        descriptor.clone(),
        DIRECT_SESSION_OPEN_TIMEOUT,
    )
    .await?;
    complete_direct_transfer(session, &archive, &options, transfer_id, descriptor).await
}

pub async fn send_direct_probe(options: SendOptions) -> anyhow::Result<SentTransfer> {
    emit_event(
        &options.events,
        PeerlineEvent::Message("building archive from selected paths".into()),
    );
    let archive = create_archive(&options.paths, options.compression)?;
    let transfer_id = TransferId::random();
    send_prebuilt_direct_probe(options, archive, transfer_id).await
}

pub async fn send_prebuilt_direct_probe(
    options: SendOptions,
    archive: Archive,
    transfer_id: TransferId,
) -> anyhow::Result<SentTransfer> {
    let descriptor = descriptor_for_archive(&options, &archive);
    let endpoint_ip = options.endpoint.ip();
    let start_port = options.endpoint.port();
    let end_port = direct_port_candidates(start_port, DEFAULT_DIRECT_PORT_WINDOW)
        .last()
        .unwrap_or(start_port);
    emit_stage(
        &options.events,
        TransferStage::Connecting(connection_route_from_endpoint(&SocketAddr::new(
            endpoint_ip,
            start_port,
        ))),
    );
    emit_message(
        &options.events,
        format!(
            "probing {} on ports {}..{}",
            endpoint_ip, start_port, end_port
        ),
    );

    let mut last_error = None;
    for port in direct_port_candidates(start_port, DEFAULT_DIRECT_PORT_WINDOW) {
        let endpoint = SocketAddr::new(endpoint_ip, port);
        match time::timeout(
            DIRECT_PORT_PROBE_TIMEOUT,
            open_direct_session(
                endpoint,
                options.name.as_ref(),
                &options.code,
                descriptor.clone(),
            ),
        )
        .await
        {
            Ok(Ok(session)) => {
                emit_message(&options.events, format!("dialing {}", endpoint));
                return complete_direct_transfer(
                    session,
                    &archive,
                    &options,
                    transfer_id,
                    descriptor,
                )
                .await;
            }
            Ok(Err(error)) => {
                tracing::debug!(%endpoint, %error, "direct port probe failed");
                last_error = Some(error);
            }
            Err(_) => {
                let error = anyhow::anyhow!("timed out probing {}", endpoint);
                tracing::debug!(%endpoint, "direct port probe timed out");
                last_error = Some(error);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        anyhow::anyhow!(
            "could not find a peerline receiver on {} starting at port {}",
            endpoint_ip,
            start_port
        )
    }))
}

pub fn descriptor_for_archive(options: &SendOptions, archive: &Archive) -> TransferDescriptor {
    let files = archive
        .manifest
        .entries
        .iter()
        .filter(|entry| entry.blake3.is_some())
        .count();
    TransferDescriptor {
        source_id: options.source_id,
        resource_id: archive.resource_id,
        archive_bytes: archive.len(),
        logical_bytes: archive.manifest.total_bytes,
        files,
        compression: archive.compression,
    }
}

async fn receive_stream(
    mut stream: TcpStream,
    peer: SocketAddr,
    options: RecvOptions,
) -> anyhow::Result<ReceivedTransfer> {
    let intro = match read_wire(&mut stream).await? {
        WireFrame::ClientIntro {
            version,
            name,
            descriptor,
            opaque_request,
            client_hello,
        } if version == PROTOCOL_VERSION => (name, descriptor, opaque_request, client_hello),
        WireFrame::ClientIntro { version, .. } => {
            let message = format!(
                "incompatible peerline protocol version {version}; expected {PROTOCOL_VERSION}"
            );
            let _ = write_wire(
                &mut stream,
                &WireFrame::Error {
                    message: message.clone(),
                },
            )
            .await;
            anyhow::bail!(message)
        }
        _ => anyhow::bail!("unexpected client handshake frame"),
    };
    let transfer_id = TransferId::random();
    if let Some(name) = intro.0.as_ref()
        && name != &options.name
    {
        anyhow::bail!("receiver name mismatch");
    }
    let mut resume_state = resume::resume_state(&options.destination, &intro.1)?;
    emit_event(
        &options.events,
        PeerlineEvent::TransferStarted {
            id: transfer_id,
            peer: peer.to_string(),
            files: intro.1.files,
            bytes: intro.1.archive_bytes,
            resume_offset: resume_state.offset,
        },
    );
    emit_event(
        &options.events,
        PeerlineEvent::StageChanged(TransferStage::Authenticating),
    );
    let transcript = direct_transcript(intro.0.as_ref());

    let record = create_server_record(
        options.code.as_str().as_bytes(),
        options.name.as_str().as_bytes(),
    )?;
    let opaque_server = start_server_login(&record, &intro.2)?;
    let server_handshake = peerline_crypto::handshake::ServerHandshake::start(&intro.3)?;
    write_wire(
        &mut stream,
        &WireFrame::ServerIntro {
            version: PROTOCOL_VERSION,
            resume_offset: resume_state.offset,
            opaque_response: opaque_server.response.clone(),
            server_hello: server_handshake.hello.clone(),
        },
    )
    .await?;

    let finish = match read_wire(&mut stream).await? {
        WireFrame::ClientFinish {
            opaque_finalization,
            client_kem,
        } => (opaque_finalization, client_kem),
        _ => anyhow::bail!("unexpected client finish frame"),
    };
    let opaque_key = opaque_server.finish(&finish.0)?;
    let keys = server_handshake.finish(&intro.3, &finish.1, &opaque_key, &transcript)?;
    let aead = ChunkAead::new(keys.recv_key, *b"pl01");

    let mut expected_sequence = 0u64;
    let header = read_secure(&mut stream, &aead, &mut expected_sequence).await?;
    let compression = match header {
        SecureFrame::Header { compression } => compression,
        _ => anyhow::bail!("secure stream must start with header"),
    };
    if compression != intro.1.compression {
        anyhow::bail!("secure header compression mismatch");
    }
    emit_event(
        &options.events,
        PeerlineEvent::StageChanged(TransferStage::ReceivingManifest),
    );

    std::fs::create_dir_all(&options.destination)?;
    loop {
        match read_secure(&mut stream, &aead, &mut expected_sequence).await? {
            SecureFrame::ArchiveChunk { bytes } => {
                let bytes_received = resume::append_chunk(&mut resume_state, &intro.1, &bytes)?;
                emit_event(
                    &options.events,
                    PeerlineEvent::Progress {
                        id: transfer_id,
                        bytes_done: bytes_received,
                        bytes_total: intro.1.archive_bytes,
                    },
                );
            }
            SecureFrame::Done => break,
            SecureFrame::Header { .. } => anyhow::bail!("duplicate secure header"),
        }
    }
    emit_event(
        &options.events,
        PeerlineEvent::StageChanged(TransferStage::Verifying),
    );

    let archive = resume::complete_partial(&resume_state, &intro.1)?;
    let result = unpack_archive_from_reader(
        &options.destination,
        compression,
        archive,
        options.overwrite,
    )
    .map(|manifest| ReceivedTransfer {
        peer: peer.to_string(),
        files: manifest
            .entries
            .iter()
            .filter(|entry| entry.blake3.is_some())
            .count(),
        bytes: manifest.total_bytes,
    });

    match result {
        Ok(received) => {
            resume::remove_partial(&resume_state)?;
            write_wire(&mut stream, &WireFrame::Ack).await?;
            emit_event(
                &options.events,
                PeerlineEvent::StageChanged(TransferStage::Complete),
            );
            Ok(received)
        }
        Err(error) => {
            let _ = resume::remove_partial(&resume_state);
            let message = error.to_string();
            let _ = write_wire(&mut stream, &WireFrame::Error { message }).await;
            Err(TransferError::fatal(error).into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{SecureFrame, WireFrame, direct_transcript, write_secure, write_wire};
    use peerline_core::{ConnectionRoute, PeerlineEvent, TransferStage};
    use peerline_crypto::{ChunkAead, ClientHandshake, start_client_login};
    use std::io::Read;
    use std::sync::{Arc, OnceLock};

    static DIRECT_TEST_PORTS: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();

    async fn reserve_probe_window() -> (u16, Vec<TcpListener>) {
        loop {
            let base_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let base = base_listener.local_addr().unwrap().port();
            drop(base_listener);

            let mut occupied = Vec::new();
            let mut ok = true;
            for offset in 0..4u16 {
                let Some(port) = base.checked_add(offset) else {
                    ok = false;
                    break;
                };
                match TcpListener::bind(("127.0.0.1", port)).await {
                    Ok(listener) => occupied.push(listener),
                    Err(_) => {
                        ok = false;
                        break;
                    }
                }
            }
            if !ok {
                continue;
            }

            let Some(next_port) = base.checked_add(4) else {
                continue;
            };
            if TcpListener::bind(("127.0.0.1", next_port)).await.is_err() {
                continue;
            }

            return (base, occupied);
        }
    }

    async fn direct_test_guard() -> tokio::sync::OwnedSemaphorePermit {
        let semaphore = DIRECT_TEST_PORTS
            .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
            .clone();
        semaphore.acquire_owned().await.unwrap()
    }

    #[tokio::test]
    async fn sends_file_over_loopback() {
        let _guard = direct_test_guard().await;
        let temp = tempfile::tempdir().unwrap();
        let src_dir = temp.path().join("src");
        let dst_dir = temp.path().join("dst");
        std::fs::create_dir(&src_dir).unwrap();
        std::fs::create_dir(&dst_dir).unwrap();
        std::fs::write(src_dir.join("hello.txt"), "hello peerline").unwrap();

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let name = HumanName::parse("river-mango-42").unwrap();
        let code = HumanCode::parse("rose-lime-iris-jade-1234").unwrap();
        let source_id = NodeId::random();
        let recv_task = tokio::spawn(recv_once(RecvOptions {
            name,
            code: code.clone(),
            bind: addr,
            destination: dst_dir.clone(),
            overwrite: false,
            events: None,
        }));
        let send_task = tokio::spawn(send_direct(SendOptions {
            endpoint: addr,
            name: None,
            code,
            source_id,
            paths: vec![src_dir.join("hello.txt")],
            compression: Compression::Zstd,
            events: None,
        }));

        recv_task.await.unwrap().unwrap();
        send_task.await.unwrap().unwrap();

        assert_eq!(
            std::fs::read_to_string(dst_dir.join("hello.txt")).unwrap(),
            "hello peerline"
        );
    }

    #[tokio::test]
    async fn direct_transfer_emits_stage_and_progress_events() {
        let _guard = direct_test_guard().await;
        let temp = tempfile::tempdir().unwrap();
        let src_dir = temp.path().join("src");
        let dst_dir = temp.path().join("dst");
        std::fs::create_dir(&src_dir).unwrap();
        std::fs::create_dir(&dst_dir).unwrap();
        std::fs::write(src_dir.join("hello.txt"), "hello peerline").unwrap();

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let (events, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let name = HumanName::parse("river-mango-42").unwrap();
        let code = HumanCode::parse("rose-lime-iris-jade-1234").unwrap();
        let source_id = NodeId::random();
        let recv_task = tokio::spawn(recv_once(RecvOptions {
            name,
            code: code.clone(),
            bind: addr,
            destination: dst_dir.clone(),
            overwrite: false,
            events: Some(events),
        }));
        let send_task = tokio::spawn(send_direct(SendOptions {
            endpoint: addr,
            name: None,
            code,
            source_id,
            paths: vec![src_dir.join("hello.txt")],
            compression: Compression::Zstd,
            events: None,
        }));

        let received = recv_task.await.unwrap().unwrap();
        let sent = send_task.await.unwrap().unwrap();
        let mut events = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            events.push(event);
        }

        assert_eq!(received.files, 1);
        assert_eq!(sent.files, 1);
        assert!(events.iter().any(|event| matches!(
            event,
            PeerlineEvent::StageChanged(TransferStage::Discovering)
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            PeerlineEvent::StageChanged(TransferStage::Connecting(ConnectionRoute::LanDirect))
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            PeerlineEvent::StageChanged(TransferStage::Authenticating)
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            PeerlineEvent::StageChanged(TransferStage::ReceivingManifest)
        )));
        assert!(
            events.iter().any(|event| matches!(
                event,
                PeerlineEvent::StageChanged(TransferStage::Verifying)
            ))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, PeerlineEvent::StageChanged(TransferStage::Complete)))
        );
        assert!(events.iter().any(|event| matches!(
            event,
            PeerlineEvent::TransferStarted {
                files: 1,
                bytes,
                ..
            } if *bytes > 0
        )));
        let transfer_bytes = events
            .iter()
            .find_map(|event| match event {
                PeerlineEvent::TransferStarted { bytes, .. } => Some(*bytes),
                _ => None,
            })
            .unwrap();
        assert!(events.iter().any(|event| matches!(
            event,
            PeerlineEvent::Progress {
                bytes_done,
                bytes_total,
                ..
            } if bytes_done == bytes_total && *bytes_total == transfer_bytes
        )));
    }

    #[tokio::test]
    async fn direct_ip_probe_walks_the_port_window() {
        let _guard = direct_test_guard().await;
        let temp = tempfile::tempdir().unwrap();
        let src_dir = temp.path().join("src");
        let dst_dir = temp.path().join("dst");
        std::fs::create_dir(&src_dir).unwrap();
        std::fs::create_dir(&dst_dir).unwrap();
        std::fs::write(src_dir.join("hello.txt"), "hello peerline").unwrap();

        let (base_port, occupied) = reserve_probe_window().await;
        assert_eq!(occupied.len(), 4);

        let name = HumanName::parse("river-mango-42").unwrap();
        let code = HumanCode::parse("rose-lime-iris-jade-1234").unwrap();
        let source_id = NodeId::random();
        let recv_task = tokio::spawn(recv_once(RecvOptions {
            name,
            code: code.clone(),
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), base_port),
            destination: dst_dir.clone(),
            overwrite: false,
            events: None,
        }));

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let send_task = tokio::spawn(send_direct_probe(SendOptions {
            endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), base_port),
            name: None,
            code,
            source_id,
            paths: vec![src_dir.join("hello.txt")],
            compression: Compression::Zstd,
            events: None,
        }));

        recv_task.await.unwrap().unwrap();
        send_task.await.unwrap().unwrap();

        assert_eq!(
            std::fs::read_to_string(dst_dir.join("hello.txt")).unwrap(),
            "hello peerline"
        );
    }

    async fn send_direct_prefix_then_disconnect(
        endpoint: SocketAddr,
        code: HumanCode,
        source_id: NodeId,
        archive: &Archive,
        max_bytes: u64,
    ) -> anyhow::Result<()> {
        let descriptor = TransferDescriptor {
            source_id,
            resource_id: archive.resource_id,
            archive_bytes: archive.len(),
            logical_bytes: archive.manifest.total_bytes,
            files: archive
                .manifest
                .entries
                .iter()
                .filter(|entry| entry.blake3.is_some())
                .count(),
            compression: archive.compression,
        };
        let mut stream = TcpStream::connect(endpoint).await?;
        let opaque_client = start_client_login(code.as_str().as_bytes())?;
        let client_handshake = ClientHandshake::start();
        write_wire(
            &mut stream,
            &WireFrame::ClientIntro {
                version: PROTOCOL_VERSION,
                name: None,
                descriptor,
                opaque_request: opaque_client.request.clone(),
                client_hello: client_handshake.hello.clone(),
            },
        )
        .await?;
        let (opaque_response, server_hello) = match read_wire(&mut stream).await? {
            WireFrame::ServerIntro {
                version,
                opaque_response,
                server_hello,
                ..
            } if version == PROTOCOL_VERSION => (opaque_response, server_hello),
            other => anyhow::bail!("unexpected server intro: {other:?}"),
        };
        let opaque_finish = opaque_client.finish(code.as_str().as_bytes(), &opaque_response)?;
        let (client_kem, keys) = client_handshake.finish(
            &server_hello,
            &opaque_finish.session_key,
            &direct_transcript(None),
        )?;
        write_wire(
            &mut stream,
            &WireFrame::ClientFinish {
                opaque_finalization: opaque_finish.finalization,
                client_kem,
            },
        )
        .await?;
        let aead = ChunkAead::new(keys.send_key, *b"pl01");
        let mut sequence = 0u64;
        write_secure(
            &mut stream,
            &aead,
            &mut sequence,
            &SecureFrame::Header {
                compression: archive.compression,
            },
        )
        .await?;
        let mut reader = archive.reader()?;
        let mut remaining = max_bytes as usize;
        let mut buffer = vec![0u8; 64 * 1024];
        while remaining > 0 {
            let read = reader.read(&mut buffer[..remaining.min(64 * 1024)])?;
            if read == 0 {
                break;
            }
            remaining -= read;
            write_secure(
                &mut stream,
                &aead,
                &mut sequence,
                &SecureFrame::ArchiveChunk {
                    bytes: buffer[..read].to_vec(),
                },
            )
            .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn direct_transfer_resumes_after_disconnect() {
        let _guard = direct_test_guard().await;
        let temp = tempfile::tempdir().unwrap();
        let src_dir = temp.path().join("src");
        let dst_dir = temp.path().join("dst");
        std::fs::create_dir(&src_dir).unwrap();
        std::fs::create_dir(&dst_dir).unwrap();
        let payload = vec![7u8; 192 * 1024];
        std::fs::write(src_dir.join("large.bin"), &payload).unwrap();

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let code = HumanCode::parse("rose-lime-iris-jade-1234").unwrap();
        let source_id = NodeId::random();
        let archive = create_archive(&[src_dir.join("large.bin")], Compression::None).unwrap();
        let (events, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let recv_task = tokio::spawn(recv_once(RecvOptions {
            name: HumanName::parse("river-mango-42").unwrap(),
            code: code.clone(),
            bind: addr,
            destination: dst_dir.clone(),
            overwrite: false,
            events: Some(events),
        }));

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        send_direct_prefix_then_disconnect(addr, code.clone(), source_id, &archive, 65_536)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(dst_dir.join(".peerline-resume").exists());

        let sent = send_direct(SendOptions {
            endpoint: addr,
            name: None,
            code,
            source_id,
            paths: vec![src_dir.join("large.bin")],
            compression: Compression::None,
            events: None,
        })
        .await
        .unwrap();
        let received = recv_task.await.unwrap().unwrap();
        let mut events = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            events.push(event);
        }

        assert_eq!(sent.files, 1);
        assert_eq!(received.files, 1);
        assert_eq!(std::fs::read(dst_dir.join("large.bin")).unwrap(), payload);
        assert!(
            !dst_dir
                .join(".peerline-resume")
                .join(source_id.hex())
                .exists()
        );
        assert!(events.iter().any(|event| matches!(
            event,
            PeerlineEvent::TransferStarted {
                resume_offset,
                ..
            } if *resume_offset > 0
        )));
        assert!(
            events.iter().any(|event| matches!(
                event,
                PeerlineEvent::StageChanged(TransferStage::Failed(_))
            ))
        );
    }

    #[tokio::test]
    async fn named_transfer_rejects_receiver_name_mismatch() {
        let _guard = direct_test_guard().await;
        let temp = tempfile::tempdir().unwrap();
        let src_dir = temp.path().join("src");
        let dst_dir = temp.path().join("dst");
        std::fs::create_dir(&src_dir).unwrap();
        std::fs::create_dir(&dst_dir).unwrap();
        std::fs::write(src_dir.join("secret.txt"), "secret").unwrap();

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let code = HumanCode::parse("rose-lime-iris-jade-1234").unwrap();
        let source_id = NodeId::random();
        let recv_task = tokio::spawn(recv_once(RecvOptions {
            name: HumanName::parse("river-mango-42").unwrap(),
            code: code.clone(),
            bind: addr,
            destination: dst_dir.clone(),
            overwrite: false,
            events: None,
        }));
        let send_task = tokio::spawn(send_direct(SendOptions {
            endpoint: addr,
            name: Some(HumanName::parse("wrong-name-42").unwrap()),
            code,
            source_id,
            paths: vec![src_dir.join("secret.txt")],
            compression: Compression::None,
            events: None,
        }));

        assert!(send_task.await.unwrap().is_err());
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(!recv_task.is_finished());
        recv_task.abort();
        let _ = recv_task.await;
        assert!(!dst_dir.join("secret.txt").exists());
    }
}
