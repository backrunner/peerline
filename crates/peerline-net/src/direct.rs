use crate::protocol::{
    PROTOCOL_VERSION, SecureFrame, WireFrame, direct_transcript, read_secure, read_wire,
    write_secure, write_wire,
};
use peerline_core::{
    Compression, ConnectionRoute, DEFAULT_DIRECT_PORT, DEFAULT_DIRECT_PORT_WINDOW, HumanCode,
    HumanName, PeerlineEvent, TransferId, TransferStage, direct_port_candidates,
};
use peerline_crypto::{
    ChunkAead, ClientHandshake, OpaqueClientStart, ServerHello, Transcript, create_server_record,
    start_client_login, start_server_login,
};
use peerline_transfer::{archive::Archive, create_archive, unpack_archive_from_reader};
use std::{
    io::Write,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};
use tempfile::NamedTempFile;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time,
};

const DIRECT_PORT_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

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
) {
    emit_event(
        events,
        PeerlineEvent::TransferStarted {
            id,
            peer: peer.into(),
            files,
            bytes,
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
}

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
    files: usize,
    bytes: u64,
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
            files,
            bytes,
            opaque_request: opaque_client.request.clone(),
            client_hello: client_handshake.hello.clone(),
        },
    )
    .await?;

    let server_intro = match read_wire(&mut stream).await? {
        WireFrame::ServerIntro {
            version,
            opaque_response,
            server_hello,
        } if version == PROTOCOL_VERSION => (opaque_response, server_hello),
        _ => anyhow::bail!("unexpected server handshake frame"),
    };

    Ok(DirectSession {
        endpoint,
        stream,
        opaque_client,
        server_response: server_intro.0,
        client_handshake,
        server_hello: server_intro.1,
        transcript,
    })
}

async fn complete_direct_transfer(
    mut session: DirectSession,
    archive: &Archive,
    options: &SendOptions,
    transfer_id: TransferId,
    files: usize,
    wire_bytes: u64,
) -> anyhow::Result<SentTransfer> {
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
        files,
        wire_bytes,
    );
    emit_stage(&options.events, TransferStage::Transferring);
    let mut sequence = 0u64;
    let mut bytes_sent = 0u64;
    let mut archive_reader = tokio::fs::File::from_std(archive.reader()?);
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
        emit_progress(&options.events, transfer_id, bytes_sent, wire_bytes);
    }
    write_secure(
        &mut session.stream,
        &aead,
        &mut sequence,
        &SecureFrame::Done,
    )
    .await?;
    session.stream.shutdown().await?;
    emit_stage(&options.events, TransferStage::Complete);

    Ok(SentTransfer {
        endpoint: session.endpoint.to_string(),
        files,
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
    let files = archive
        .manifest
        .entries
        .iter()
        .filter(|entry| entry.blake3.is_some())
        .count();
    let wire_bytes = archive.len();
    emit_stage(
        &options.events,
        TransferStage::Connecting(connection_route_from_endpoint(&options.endpoint)),
    );
    emit_message(&options.events, format!("dialing {}", options.endpoint));
    let session = open_direct_session(
        options.endpoint,
        options.name.as_ref(),
        &options.code,
        files,
        wire_bytes,
    )
    .await?;
    complete_direct_transfer(session, &archive, &options, transfer_id, files, wire_bytes).await
}

pub async fn send_direct_probe(options: SendOptions) -> anyhow::Result<SentTransfer> {
    emit_event(
        &options.events,
        PeerlineEvent::Message("building archive from selected paths".into()),
    );
    let archive = create_archive(&options.paths, options.compression)?;
    let transfer_id = TransferId::random();
    let files = archive
        .manifest
        .entries
        .iter()
        .filter(|entry| entry.blake3.is_some())
        .count();
    let wire_bytes = archive.len();
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
                files,
                wire_bytes,
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
                    files,
                    wire_bytes,
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

async fn receive_stream(
    mut stream: TcpStream,
    peer: SocketAddr,
    options: RecvOptions,
) -> anyhow::Result<ReceivedTransfer> {
    let intro = match read_wire(&mut stream).await? {
        WireFrame::ClientIntro {
            version,
            name,
            files,
            bytes,
            opaque_request,
            client_hello,
        } if version == PROTOCOL_VERSION => (name, files, bytes, opaque_request, client_hello),
        _ => anyhow::bail!("unexpected client handshake frame"),
    };
    let transfer_id = TransferId::random();
    emit_event(
        &options.events,
        PeerlineEvent::TransferStarted {
            id: transfer_id,
            peer: peer.to_string(),
            files: intro.1,
            bytes: intro.2,
        },
    );
    if let Some(name) = intro.0.as_ref()
        && name != &options.name
    {
        anyhow::bail!("receiver name mismatch");
    }
    emit_event(
        &options.events,
        PeerlineEvent::StageChanged(TransferStage::Authenticating),
    );
    let transcript = direct_transcript(intro.0.as_ref());

    let record = create_server_record(
        options.code.as_str().as_bytes(),
        options.name.as_str().as_bytes(),
    )?;
    let opaque_server = start_server_login(&record, &intro.3)?;
    let server_handshake = peerline_crypto::handshake::ServerHandshake::start(&intro.4)?;
    write_wire(
        &mut stream,
        &WireFrame::ServerIntro {
            version: PROTOCOL_VERSION,
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
    let keys = server_handshake.finish(&intro.4, &finish.1, &opaque_key, &transcript)?;
    let aead = ChunkAead::new(keys.recv_key, *b"pl01");

    let mut expected_sequence = 0u64;
    let header = read_secure(&mut stream, &aead, &mut expected_sequence).await?;
    let compression = match header {
        SecureFrame::Header { compression } => compression,
        _ => anyhow::bail!("secure stream must start with header"),
    };
    emit_event(
        &options.events,
        PeerlineEvent::StageChanged(TransferStage::ReceivingManifest),
    );

    std::fs::create_dir_all(&options.destination)?;
    let mut archive = NamedTempFile::new_in(&options.destination)?;
    let mut bytes_received = 0u64;
    loop {
        match read_secure(&mut stream, &aead, &mut expected_sequence).await? {
            SecureFrame::ArchiveChunk { bytes } => {
                bytes_received += bytes.len() as u64;
                archive.as_file_mut().write_all(&bytes)?;
                emit_event(
                    &options.events,
                    PeerlineEvent::Progress {
                        id: transfer_id,
                        bytes_done: bytes_received,
                        bytes_total: intro.2,
                    },
                );
            }
            SecureFrame::Done => break,
            SecureFrame::Header { .. } => anyhow::bail!("duplicate secure header"),
        }
    }
    archive.as_file_mut().flush()?;
    emit_event(
        &options.events,
        PeerlineEvent::StageChanged(TransferStage::Verifying),
    );

    let manifest = unpack_archive_from_reader(
        &options.destination,
        compression,
        archive.reopen()?,
        options.overwrite,
    )?;
    emit_event(
        &options.events,
        PeerlineEvent::StageChanged(TransferStage::Complete),
    );
    Ok(ReceivedTransfer {
        peer: peer.to_string(),
        files: manifest
            .entries
            .iter()
            .filter(|entry| entry.blake3.is_some())
            .count(),
        bytes: manifest.total_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use peerline_core::{ConnectionRoute, PeerlineEvent, TransferStage};
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
