use crate::{
    direct::{RecvOptions, SendOptions, SentTransfer, TransferError},
    protocol::{
        PROTOCOL_VERSION, SecureFrame, WireFrame, decrypt_secure, direct_transcript_for_route,
        encrypt_secure,
    },
    resume,
};
use futures::{SinkExt, StreamExt};
use peerline_core::{ConnectionRoute, PeerlineEvent, TransferId, TransferStage};
use peerline_crypto::{
    ChunkAead, ClientHandshake, OpaqueClientStart, ServerHello, Transcript, create_server_record,
    start_client_login, start_server_login,
};
use peerline_transfer::{Archive, create_archive, unpack_archive_from_reader};
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    net::TcpListener,
    time,
};
use tokio_socks::tcp::Socks5Stream;
use tokio_tungstenite::{
    WebSocketStream, accept_hdr_async, client_async, connect_async,
    tungstenite::{
        Message,
        handshake::server::{Request, Response},
    },
};

const PUBLIC_TUNNEL_ROUTE_LABEL: &str = "public-tunnel";
const TOR_ONION_ROUTE_LABEL: &str = "tor-onion";
const PUBLIC_TUNNEL_SESSION_OPEN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublicTunnelProvider {
    Cloudflared,
    Localtunnel,
    Tmole,
}

impl PublicTunnelProvider {
    pub fn label(self) -> &'static str {
        match self {
            Self::Cloudflared => "cloudflared",
            Self::Localtunnel => "localtunnel",
            Self::Tmole => "tmole",
        }
    }
}

pub fn normalize_public_tunnel_url(raw: &str) -> anyhow::Result<String> {
    let mut url = reqwest::Url::parse(raw)?;
    match url.scheme() {
        "http" => {
            url.set_scheme("ws")
                .map_err(|_| anyhow::anyhow!("could not convert tunnel URL to ws"))?;
        }
        "https" => {
            url.set_scheme("wss")
                .map_err(|_| anyhow::anyhow!("could not convert tunnel URL to wss"))?;
        }
        "ws" | "wss" => {}
        other => anyhow::bail!("unsupported tunnel URL scheme: {other}"),
    }

    if matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "::1")) {
        anyhow::bail!("public tunnel URL must not point at loopback");
    }

    Ok(url.to_string())
}

pub fn normalize_tor_onion_url(raw: &str) -> anyhow::Result<String> {
    let raw = if raw.contains("://") {
        raw.to_string()
    } else {
        format!("ws://{raw}")
    };
    let mut url = reqwest::Url::parse(&raw)?;
    match url.scheme() {
        "http" => {
            url.set_scheme("ws")
                .map_err(|_| anyhow::anyhow!("could not convert onion URL to ws"))?;
        }
        "ws" => {}
        "https" | "wss" => {
            anyhow::bail!("Tor onion transport currently supports ws/http, not wss/https")
        }
        other => anyhow::bail!("unsupported onion URL scheme: {other}"),
    }

    let Some(host) = url.host_str() else {
        anyhow::bail!("Tor onion URL is missing a host");
    };
    if !host.to_ascii_lowercase().ends_with(".onion") {
        anyhow::bail!("Tor onion URL host must end with .onion");
    }

    Ok(url.to_string())
}

pub async fn bind_public_tunnel_listener() -> anyhow::Result<(TcpListener, SocketAddr)> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let actual = listener.local_addr()?;
    Ok((listener, actual))
}

pub async fn bind_tor_onion_listener() -> anyhow::Result<(TcpListener, SocketAddr)> {
    bind_public_tunnel_listener().await
}

pub async fn recv_public_tunnel_bound(
    listener: &TcpListener,
    options: RecvOptions,
    endpoint_label: String,
) -> anyhow::Result<crate::direct::ReceivedTransfer> {
    recv_ws_bound(
        listener,
        options,
        endpoint_label,
        ConnectionRoute::PublicTunnel,
        PUBLIC_TUNNEL_ROUTE_LABEL,
        "public tunnel",
    )
    .await
}

pub async fn recv_tor_onion_bound(
    listener: &TcpListener,
    options: RecvOptions,
    endpoint_label: String,
) -> anyhow::Result<crate::direct::ReceivedTransfer> {
    recv_ws_bound(
        listener,
        options,
        endpoint_label,
        ConnectionRoute::TorOnion,
        TOR_ONION_ROUTE_LABEL,
        "Tor onion",
    )
    .await
}

async fn recv_ws_bound(
    listener: &TcpListener,
    options: RecvOptions,
    endpoint_label: String,
    route: ConnectionRoute,
    route_label: &'static str,
    log_label: &'static str,
) -> anyhow::Result<crate::direct::ReceivedTransfer> {
    loop {
        let (stream, peer) = listener.accept().await?;
        let accept = |req: &Request, response: Response| {
            tracing::debug!(path = %req.uri().path(), route = log_label, "accepted websocket bridge");
            Ok(response)
        };
        let mut ws_stream = match accept_hdr_async(stream, accept).await {
            Ok(ws_stream) => ws_stream,
            Err(error) => {
                tracing::debug!(%error, route = log_label, "websocket bridge handshake failed");
                continue;
            }
        };

        emit_event(
            &options.events,
            PeerlineEvent::StageChanged(TransferStage::Connecting(route.clone())),
        );
        match receive_ws_stream(
            &mut ws_stream,
            endpoint_label.clone(),
            options.clone(),
            route_label,
        )
        .await
        {
            Ok(result) => return Ok(result),
            Err(error) => {
                tracing::warn!(%error, %peer, route = log_label, "websocket bridge transfer failed; waiting for another connection");
                emit_event(
                    &options.events,
                    PeerlineEvent::StageChanged(TransferStage::Failed(error.to_string())),
                );
                emit_event(
                    &options.events,
                    PeerlineEvent::Message(format!("{endpoint_label}: {}", error)),
                );
            }
        }
    }
}

pub async fn send_public_tunnel(
    options: SendOptions,
    endpoint: String,
) -> anyhow::Result<SentTransfer> {
    emit_event(
        &options.events,
        PeerlineEvent::Message("building archive from selected paths".into()),
    );
    let archive = create_archive(&options.paths, options.compression)?;
    let transfer_id = TransferId::random();
    send_prebuilt_public_tunnel(options, archive, transfer_id, endpoint).await
}

pub async fn send_prebuilt_public_tunnel(
    options: SendOptions,
    archive: Archive,
    transfer_id: TransferId,
    endpoint: String,
) -> anyhow::Result<SentTransfer> {
    let endpoint = normalize_public_tunnel_url(&endpoint)?;
    let descriptor = crate::direct::descriptor_for_archive(&options, &archive);
    emit_stage(
        &options.events,
        TransferStage::Connecting(ConnectionRoute::PublicTunnel),
    );
    emit_message(&options.events, format!("dialing {}", endpoint));

    let session = match time::timeout(
        PUBLIC_TUNNEL_SESSION_OPEN_TIMEOUT,
        open_public_tunnel_session(
            endpoint.clone(),
            options.name.as_ref(),
            &options.code,
            descriptor.clone(),
        ),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => anyhow::bail!(
            "timed out connecting to {} after {} seconds",
            endpoint,
            PUBLIC_TUNNEL_SESSION_OPEN_TIMEOUT.as_secs()
        ),
    };
    complete_public_tunnel_transfer(session, &archive, &options, transfer_id, descriptor).await
}

pub async fn send_prebuilt_tor_onion(
    options: SendOptions,
    archive: Archive,
    transfer_id: TransferId,
    endpoint: String,
    socks_proxy: SocketAddr,
) -> anyhow::Result<SentTransfer> {
    let endpoint = normalize_tor_onion_url(&endpoint)?;
    let descriptor = crate::direct::descriptor_for_archive(&options, &archive);
    emit_stage(
        &options.events,
        TransferStage::Connecting(ConnectionRoute::TorOnion),
    );
    emit_message(
        &options.events,
        format!("dialing {} via SOCKS5 {}", endpoint, socks_proxy),
    );

    let session = match time::timeout(
        PUBLIC_TUNNEL_SESSION_OPEN_TIMEOUT,
        open_tor_onion_session(
            endpoint.clone(),
            socks_proxy,
            options.name.as_ref(),
            &options.code,
            descriptor.clone(),
        ),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => anyhow::bail!(
            "timed out connecting to {} after {} seconds",
            endpoint,
            PUBLIC_TUNNEL_SESSION_OPEN_TIMEOUT.as_secs()
        ),
    };
    complete_public_tunnel_transfer(session, &archive, &options, transfer_id, descriptor).await
}

struct PublicTunnelSession<S> {
    endpoint: String,
    stream: WebSocketStream<S>,
    opaque_client: OpaqueClientStart,
    server_response: Vec<u8>,
    client_handshake: ClientHandshake,
    server_hello: ServerHello,
    transcript: Transcript,
    resume_offset: u64,
}

async fn open_public_tunnel_session(
    endpoint: String,
    name: Option<&peerline_core::HumanName>,
    code: &peerline_core::HumanCode,
    descriptor: peerline_core::TransferDescriptor,
) -> anyhow::Result<PublicTunnelSession<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>> {
    let (stream, _) = connect_async(&endpoint).await?;
    open_websocket_session(
        stream,
        endpoint,
        name,
        code,
        descriptor,
        PUBLIC_TUNNEL_ROUTE_LABEL,
    )
    .await
}

async fn open_tor_onion_session(
    endpoint: String,
    socks_proxy: SocketAddr,
    name: Option<&peerline_core::HumanName>,
    code: &peerline_core::HumanCode,
    descriptor: peerline_core::TransferDescriptor,
) -> anyhow::Result<PublicTunnelSession<Socks5Stream<tokio::net::TcpStream>>> {
    let url = reqwest::Url::parse(&endpoint)?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("Tor onion URL is missing a host"))?
        .to_string();
    let port = url.port_or_known_default().unwrap_or(80);
    let stream = Socks5Stream::connect(socks_proxy, (host.as_str(), port)).await?;
    let (stream, _) = client_async(&endpoint, stream).await?;
    open_websocket_session(
        stream,
        endpoint,
        name,
        code,
        descriptor,
        TOR_ONION_ROUTE_LABEL,
    )
    .await
}

async fn open_websocket_session<S>(
    mut stream: WebSocketStream<S>,
    endpoint: String,
    name: Option<&peerline_core::HumanName>,
    code: &peerline_core::HumanCode,
    descriptor: peerline_core::TransferDescriptor,
    route_label: &'static str,
) -> anyhow::Result<PublicTunnelSession<S>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let transcript = direct_transcript_for_route(name, route_label);
    let opaque_client = start_client_login(code.as_str().as_bytes())?;
    let client_handshake = ClientHandshake::start();
    ws_write_wire(
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

    let server_intro = match ws_read_wire(&mut stream).await? {
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
        _ => anyhow::bail!("unexpected websocket handshake frame"),
    };

    Ok(PublicTunnelSession {
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

async fn complete_public_tunnel_transfer<S>(
    mut session: PublicTunnelSession<S>,
    archive: &Archive,
    options: &SendOptions,
    transfer_id: TransferId,
    descriptor: peerline_core::TransferDescriptor,
) -> anyhow::Result<SentTransfer>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
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
    ws_write_wire(
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
    ws_write_secure(
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
        ws_write_secure(
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
    ws_write_secure(
        &mut session.stream,
        &aead,
        &mut sequence,
        &SecureFrame::Done,
    )
    .await?;
    match ws_read_wire(&mut session.stream).await? {
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
    let _ = session.stream.close(None).await;
    emit_stage(&options.events, TransferStage::Complete);

    Ok(SentTransfer {
        endpoint: session.endpoint.to_string(),
        files: descriptor.files,
        bytes: archive.manifest.total_bytes,
    })
}

async fn receive_ws_stream<S>(
    stream: &mut WebSocketStream<S>,
    peer: String,
    options: RecvOptions,
    route_label: &'static str,
) -> anyhow::Result<crate::direct::ReceivedTransfer>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let intro = match ws_read_wire(stream).await? {
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
            let _ = ws_write_wire(
                stream,
                &WireFrame::Error {
                    message: message.clone(),
                },
            )
            .await;
            anyhow::bail!(message)
        }
        _ => anyhow::bail!("unexpected public tunnel client handshake frame"),
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
            peer: peer.clone(),
            files: intro.1.files,
            bytes: intro.1.archive_bytes,
            resume_offset: resume_state.offset,
        },
    );
    emit_event(
        &options.events,
        PeerlineEvent::StageChanged(TransferStage::Authenticating),
    );
    let transcript = direct_transcript_for_route(intro.0.as_ref(), route_label);

    let record = create_server_record(
        options.code.as_str().as_bytes(),
        options.name.as_str().as_bytes(),
    )?;
    let opaque_server = start_server_login(&record, &intro.2)?;
    let server_handshake = peerline_crypto::handshake::ServerHandshake::start(&intro.3)?;
    ws_write_wire(
        stream,
        &WireFrame::ServerIntro {
            version: PROTOCOL_VERSION,
            resume_offset: resume_state.offset,
            opaque_response: opaque_server.response.clone(),
            server_hello: server_handshake.hello.clone(),
        },
    )
    .await?;

    let finish = match ws_read_wire(stream).await? {
        WireFrame::ClientFinish {
            opaque_finalization,
            client_kem,
        } => (opaque_finalization, client_kem),
        _ => anyhow::bail!("unexpected public tunnel client finish frame"),
    };
    let opaque_key = opaque_server.finish(&finish.0)?;
    let keys = server_handshake.finish(&intro.3, &finish.1, &opaque_key, &transcript)?;
    let aead = ChunkAead::new(keys.recv_key, *b"pl01");

    let mut expected_sequence = 0u64;
    let header = ws_read_secure(stream, &aead, &mut expected_sequence).await?;
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
        match ws_read_secure(stream, &aead, &mut expected_sequence).await? {
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
    .map(|manifest| crate::direct::ReceivedTransfer {
        peer: peer.clone(),
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
            ws_write_wire(stream, &WireFrame::Ack).await?;
            emit_event(
                &options.events,
                PeerlineEvent::StageChanged(TransferStage::Complete),
            );
            Ok(received)
        }
        Err(error) => {
            let _ = resume::remove_partial(&resume_state);
            let message = error.to_string();
            let _ = ws_write_wire(stream, &WireFrame::Error { message }).await;
            Err(TransferError::fatal(error).into())
        }
    }
}

async fn ws_write_wire<S>(stream: &mut WebSocketStream<S>, frame: &WireFrame) -> anyhow::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let payload = postcard::to_allocvec(frame)?;
    stream.send(Message::Binary(payload.into())).await?;
    Ok(())
}

async fn ws_read_wire<S>(stream: &mut WebSocketStream<S>) -> anyhow::Result<WireFrame>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        match stream.next().await {
            Some(Ok(Message::Binary(payload))) => return Ok(postcard::from_bytes(&payload)?),
            Some(Ok(Message::Text(_))) => anyhow::bail!("expected binary websocket frame"),
            Some(Ok(Message::Ping(payload))) => {
                stream.send(Message::Pong(payload)).await?;
            }
            Some(Ok(Message::Pong(_))) => {}
            Some(Ok(Message::Close(_))) => anyhow::bail!("websocket closed"),
            Some(Ok(Message::Frame(_))) => {}
            Some(Err(error)) => return Err(error.into()),
            None => anyhow::bail!("websocket closed"),
        }
    }
}

async fn ws_write_secure<S>(
    stream: &mut WebSocketStream<S>,
    aead: &ChunkAead,
    sequence: &mut u64,
    frame: &SecureFrame,
) -> anyhow::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let encrypted = encrypt_secure(aead, sequence, frame)?;
    ws_write_wire(stream, &WireFrame::Secure(encrypted)).await
}

async fn ws_read_secure<S>(
    stream: &mut WebSocketStream<S>,
    aead: &ChunkAead,
    expected_sequence: &mut u64,
) -> anyhow::Result<SecureFrame>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let encrypted = match ws_read_wire(stream).await? {
        WireFrame::Secure(encrypted) => encrypted,
        _ => anyhow::bail!("expected secure frame"),
    };
    decrypt_secure(aead, expected_sequence, encrypted)
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

#[cfg(test)]
mod tests {
    use super::{normalize_public_tunnel_url, normalize_tor_onion_url};

    #[test]
    fn normalizes_public_tunnel_urls_to_websocket_schemes() {
        assert_eq!(
            normalize_public_tunnel_url("http://example.com:8080/bridge?x=1").unwrap(),
            "ws://example.com:8080/bridge?x=1"
        );
        assert_eq!(
            normalize_public_tunnel_url("https://example.com/path").unwrap(),
            "wss://example.com/path"
        );
        assert_eq!(
            normalize_public_tunnel_url("ws://example.com/socket").unwrap(),
            "ws://example.com/socket"
        );
    }

    #[test]
    fn rejects_loopback_public_tunnel_urls() {
        assert!(normalize_public_tunnel_url("https://127.0.0.1:8080").is_err());
        assert!(normalize_public_tunnel_url("ws://localhost:8080").is_err());
    }

    #[test]
    fn normalizes_tor_onion_urls_to_websocket_schemes() {
        assert_eq!(
            normalize_tor_onion_url("abcdefghijklmnopqrstuvwxyzabcdefghijklmnop.onion").unwrap(),
            "ws://abcdefghijklmnopqrstuvwxyzabcdefghijklmnop.onion/"
        );
        assert_eq!(
            normalize_tor_onion_url("http://abcdefghijklmnopqrstuvwxyzabcdefghijklmnop.onion/x")
                .unwrap(),
            "ws://abcdefghijklmnopqrstuvwxyzabcdefghijklmnop.onion/x"
        );
    }

    #[test]
    fn rejects_non_onion_tor_urls() {
        assert!(normalize_tor_onion_url("https://example.com").is_err());
        assert!(normalize_tor_onion_url("ws://127.0.0.1:8080").is_err());
        assert!(
            normalize_tor_onion_url("wss://abcdefghijklmnopqrstuvwxyzabcdefghijklmnop.onion")
                .is_err()
        );
    }
}
