use crate::protocol::{
    PROTOCOL_VERSION, SecureFrame, WireFrame, direct_transcript, read_secure, read_wire,
    write_secure, write_wire,
};
use peerline_core::{
    Compression, ConnectionRoute, DEFAULT_DIRECT_PORT, HumanCode, HumanName, PeerlineEvent,
    TransferId, TransferStage,
};
use peerline_crypto::{ChunkAead, create_server_record, start_client_login, start_server_login};
use peerline_transfer::{create_archive, unpack_archive_from_reader};
use std::{
    io::Write,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};
use tempfile::NamedTempFile;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

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

pub async fn recv_once(options: RecvOptions) -> anyhow::Result<ReceivedTransfer> {
    emit_event(
        &options.events,
        PeerlineEvent::StageChanged(TransferStage::Discovering),
    );
    let listener = TcpListener::bind(options.bind).await?;
    recv_once_bound(listener, options).await
}

pub async fn recv_once_bound(
    listener: TcpListener,
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
                emit_event(&options.events, PeerlineEvent::Message(error.to_string()));
            }
        }
    }
}

pub async fn send_direct(options: SendOptions) -> anyhow::Result<SentTransfer> {
    let archive = create_archive(&options.paths, options.compression)?;
    let mut stream = TcpStream::connect(options.endpoint).await?;
    let transcript = direct_transcript(options.name.as_ref());

    let opaque_client = start_client_login(options.code.as_str().as_bytes())?;
    let client_handshake = peerline_crypto::handshake::ClientHandshake::start();
    write_wire(
        &mut stream,
        &WireFrame::ClientIntro {
            version: PROTOCOL_VERSION,
            name: options.name.clone(),
            files: archive
                .manifest
                .entries
                .iter()
                .filter(|entry| entry.blake3.is_some())
                .count(),
            bytes: archive.len(),
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

    let opaque_finish = opaque_client.finish(options.code.as_str().as_bytes(), &server_intro.0)?;
    let (client_kem, keys) =
        client_handshake.finish(&server_intro.1, &opaque_finish.session_key, &transcript)?;
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
    let mut archive_reader = tokio::fs::File::from_std(archive.reader()?);
    write_secure(
        &mut stream,
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
    write_secure(&mut stream, &aead, &mut sequence, &SecureFrame::Done).await?;
    stream.shutdown().await?;

    Ok(SentTransfer {
        endpoint: options.endpoint.to_string(),
        files: archive
            .manifest
            .entries
            .iter()
            .filter(|entry| entry.blake3.is_some())
            .count(),
        bytes: archive.manifest.total_bytes,
    })
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

    #[tokio::test]
    async fn sends_file_over_loopback() {
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
    async fn named_transfer_rejects_receiver_name_mismatch() {
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
        }));

        assert!(send_task.await.unwrap().is_err());
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(!recv_task.is_finished());
        recv_task.abort();
        let _ = recv_task.await;
        assert!(!dst_dir.join("secret.txt").exists());
    }
}
