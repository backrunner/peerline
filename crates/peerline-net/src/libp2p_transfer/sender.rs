use super::{
    LIBP2P_ROUTE_LABEL, Libp2pSendOptions,
    behaviour::{TransferBehaviour, TransferBehaviourEvent, build_sender_swarm},
};
use crate::direct::descriptor_for_archive;
use crate::protocol::{
    PROTOCOL_VERSION, SecureFrame, WireFrame, encrypt_secure, libp2p_transcript,
};
use futures::StreamExt;
use libp2p::{
    PeerId, Swarm,
    request_response::{Event as RequestResponseEvent, Message as RequestResponseMessage},
    swarm::SwarmEvent,
};
use peerline_core::{ConnectionRoute, PeerlineEvent, TransferId, TransferStage};
use peerline_crypto::{ChunkAead, ClientHandshake, start_client_login};
use peerline_transfer::{Archive, create_archive};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    time,
};

const LIBP2P_REQUEST_ROUND_TRIP_TIMEOUT: Duration = Duration::from_secs(45);

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

fn route_label(route: &ConnectionRoute) -> &'static str {
    match route {
        ConnectionRoute::LanDirect => "lan-direct",
        ConnectionRoute::PublicDirect => "public-direct",
        ConnectionRoute::PublicTunnel => "public-tunnel",
        ConnectionRoute::TorOnion => "tor-onion",
        ConnectionRoute::Libp2pQuic => "libp2p-quic",
        ConnectionRoute::Libp2pDcutr => "libp2p-dcutr",
        ConnectionRoute::Libp2pRelay => "libp2p-relay",
        ConnectionRoute::WebRtcDirect => "webrtc-direct",
        ConnectionRoute::WebRtcTurn => "webrtc-turn",
    }
}

pub(crate) async fn send_libp2p(
    options: Libp2pSendOptions,
) -> anyhow::Result<crate::direct::SentTransfer> {
    emit_message(&options.events, "building archive from selected paths");
    let archive = create_archive(&options.paths, options.compression)?;
    let transfer_id = TransferId::random();
    send_prebuilt_libp2p(options, archive, transfer_id).await
}

pub(crate) async fn send_prebuilt_libp2p(
    options: Libp2pSendOptions,
    archive: Archive,
    transfer_id: TransferId,
) -> anyhow::Result<crate::direct::SentTransfer> {
    let descriptor = descriptor_for_archive(
        &crate::direct::SendOptions {
            endpoint: "127.0.0.1:0".parse().expect("static endpoint"),
            name: Some(options.name.clone()),
            code: options.code.clone(),
            source_id: options.source_id,
            paths: Vec::new(),
            compression: options.compression,
            events: options.events.clone(),
        },
        &archive,
    );
    emit_stage(
        &options.events,
        TransferStage::Connecting(options.route.clone()),
    );
    emit_message(
        &options.events,
        format!(
            "dialing {} via {}",
            options.peer_id,
            route_label(&options.route)
        ),
    );

    let lookup_key =
        peerline_core::NameCode::new(options.name.clone(), options.code.clone()).lookup_key();
    let mut swarm =
        build_sender_swarm(false, options.enable_upnp, &options.webrtc_ice_servers).await?;
    let route_name = route_label(&options.route);
    let transcript = libp2p_transcript(
        &options.name,
        &lookup_key,
        &options.peer_id.to_string(),
        LIBP2P_ROUTE_LABEL,
    );

    emit_stage(&options.events, TransferStage::Authenticating);
    let opaque_client = start_client_login(options.code.as_str().as_bytes())?;
    let client_handshake = ClientHandshake::start();

    let server_intro = match request_round_trip(
        &mut swarm,
        options.peer_id,
        options.addresses.clone(),
        WireFrame::ClientIntro {
            version: PROTOCOL_VERSION,
            name: Some(options.name.clone()),
            descriptor: descriptor.clone(),
            opaque_request: opaque_client.request.clone(),
            client_hello: client_handshake.hello.clone(),
        },
    )
    .await?
    {
        WireFrame::ServerIntro {
            version,
            resume_offset,
            opaque_response,
            server_hello,
        } if version == PROTOCOL_VERSION => (resume_offset, opaque_response, server_hello),
        WireFrame::ServerIntro { version, .. } => {
            anyhow::bail!(
                "incompatible peerline protocol version {version}; expected {PROTOCOL_VERSION}"
            )
        }
        WireFrame::Error { message } => anyhow::bail!("{message}"),
        other => anyhow::bail!("unexpected libp2p handshake response: {other:?}"),
    };
    if server_intro.0 > descriptor.archive_bytes {
        anyhow::bail!("receiver requested resume offset beyond archive size");
    }
    let opaque_finish = opaque_client.finish(options.code.as_str().as_bytes(), &server_intro.1)?;
    let (client_kem, session_keys) =
        client_handshake.finish(&server_intro.2, &opaque_finish.session_key, &transcript)?;

    match request_round_trip(
        &mut swarm,
        options.peer_id,
        options.addresses.clone(),
        WireFrame::ClientFinish {
            opaque_finalization: opaque_finish.finalization,
            client_kem,
        },
    )
    .await?
    {
        WireFrame::Ack => {}
        WireFrame::Error { message } => anyhow::bail!("{message}"),
        other => anyhow::bail!("unexpected libp2p finish response: {other:?}"),
    }
    let mut aead = ChunkAead::new(session_keys.send_key, *b"pl01");
    emit_transfer_started(
        &options.events,
        transfer_id,
        options.peer_id.to_string(),
        descriptor.files,
        descriptor.archive_bytes,
        server_intro.0,
    );
    if server_intro.0 > 0 {
        emit_message(
            &options.events,
            format!("resuming at {} bytes", server_intro.0),
        );
    }
    emit_stage(&options.events, TransferStage::Transferring);
    let mut sequence = 0u64;
    let mut archive_reader = tokio::fs::File::from_std(archive.reader()?);
    archive_reader
        .seek(std::io::SeekFrom::Start(server_intro.0))
        .await?;
    let mut bytes_sent = server_intro.0;
    send_secure_request(
        &mut swarm,
        &options,
        &mut aead,
        &mut sequence,
        SecureFrame::Header {
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
        send_secure_request(
            &mut swarm,
            &options,
            &mut aead,
            &mut sequence,
            SecureFrame::ArchiveChunk {
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

    send_secure_request(
        &mut swarm,
        &options,
        &mut aead,
        &mut sequence,
        SecureFrame::Done,
    )
    .await?;
    emit_stage(&options.events, TransferStage::Complete);

    Ok(crate::direct::SentTransfer {
        endpoint: format!("{} via {}", options.peer_id, route_name),
        files: descriptor.files,
        bytes: archive.manifest.total_bytes,
    })
}

async fn send_secure_request(
    swarm: &mut Swarm<TransferBehaviour>,
    options: &Libp2pSendOptions,
    aead: &mut ChunkAead,
    sequence: &mut u64,
    frame: SecureFrame,
) -> anyhow::Result<()> {
    let encrypted = encrypt_secure(aead, sequence, &frame)?;
    match request_round_trip(
        swarm,
        options.peer_id,
        options.addresses.clone(),
        WireFrame::Secure(encrypted),
    )
    .await?
    {
        WireFrame::Ack => Ok(()),
        WireFrame::Error { message } => anyhow::bail!("{message}"),
        other => anyhow::bail!("unexpected libp2p secure response: {other:?}"),
    }
}

async fn request_round_trip(
    swarm: &mut Swarm<TransferBehaviour>,
    peer: PeerId,
    addresses: Vec<libp2p::Multiaddr>,
    request: WireFrame,
) -> anyhow::Result<WireFrame> {
    let request_id = swarm
        .behaviour_mut()
        .transfer
        .send_request_with_addresses(&peer, request, addresses);

    match time::timeout(LIBP2P_REQUEST_ROUND_TRIP_TIMEOUT, async {
        loop {
            match swarm.select_next_some().await {
                SwarmEvent::Behaviour(TransferBehaviourEvent::Transfer(
                    RequestResponseEvent::Message {
                        peer: message_peer,
                        message:
                            RequestResponseMessage::Response {
                                request_id: response_id,
                                response,
                            },
                        ..
                    },
                )) if message_peer == peer && response_id == request_id => {
                    return Ok(response);
                }
                SwarmEvent::Behaviour(TransferBehaviourEvent::Transfer(
                    RequestResponseEvent::OutboundFailure {
                        peer: failed_peer,
                        request_id: failed_id,
                        error,
                        ..
                    },
                )) if failed_peer == peer && failed_id == request_id => {
                    anyhow::bail!("libp2p request failed: {error}");
                }
                SwarmEvent::Behaviour(TransferBehaviourEvent::Transfer(
                    RequestResponseEvent::InboundFailure { .. }
                    | RequestResponseEvent::ResponseSent { .. }
                    | RequestResponseEvent::Message {
                        message: RequestResponseMessage::Request { .. },
                        ..
                    },
                )) => {}
                SwarmEvent::Behaviour(TransferBehaviourEvent::Dcutr(event)) => {
                    tracing::debug!(?event, "dcutr event while waiting for response");
                }
                SwarmEvent::Behaviour(TransferBehaviourEvent::Relay(event)) => {
                    tracing::debug!(?event, "relay event while waiting for response");
                }
                SwarmEvent::Behaviour(TransferBehaviourEvent::Rendezvous(event)) => {
                    tracing::debug!(?event, "rendezvous event while waiting for response");
                }
                _ => {}
            }
        }
    })
    .await
    {
        Ok(result) => result,
        Err(_) => {
            anyhow::bail!(
                "libp2p request timed out after {} seconds",
                LIBP2P_REQUEST_ROUND_TRIP_TIMEOUT.as_secs()
            )
        }
    }
}
