use super::{
    LIBP2P_ROUTE_LABEL, Libp2pSendOptions,
    behaviour::{TransferBehaviour, TransferBehaviourEvent, build_sender_swarm},
};
use crate::protocol::{
    PROTOCOL_VERSION, SecureFrame, WireFrame, encrypt_secure, libp2p_transcript,
};
use futures::StreamExt;
use libp2p::{
    PeerId, Swarm,
    request_response::{Event as RequestResponseEvent, Message as RequestResponseMessage},
    swarm::SwarmEvent,
};
use peerline_crypto::{ChunkAead, ClientHandshake, start_client_login};
use peerline_transfer::create_archive;
use tokio::io::AsyncReadExt;

pub(crate) async fn send_libp2p(
    options: Libp2pSendOptions,
) -> anyhow::Result<crate::direct::SentTransfer> {
    let archive = create_archive(&options.paths, options.compression)?;
    let lookup_key =
        peerline_core::NameCode::new(options.name.clone(), options.code.clone()).lookup_key();
    let mut swarm = build_sender_swarm(false).await?;
    let transcript = libp2p_transcript(
        &options.name,
        &lookup_key,
        &options.peer_id.to_string(),
        LIBP2P_ROUTE_LABEL,
    );

    let opaque_client = start_client_login(options.code.as_str().as_bytes())?;
    let client_handshake = ClientHandshake::start();

    let server_intro = match request_round_trip(
        &mut swarm,
        options.peer_id,
        options.addresses.clone(),
        WireFrame::ClientIntro {
            version: PROTOCOL_VERSION,
            name: Some(options.name.clone()),
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
    .await?
    {
        WireFrame::ServerIntro {
            version,
            opaque_response,
            server_hello,
        } if version == PROTOCOL_VERSION => (opaque_response, server_hello),
        WireFrame::Error { message } => anyhow::bail!("{message}"),
        other => anyhow::bail!("unexpected libp2p handshake response: {other:?}"),
    };
    let opaque_finish = opaque_client.finish(options.code.as_str().as_bytes(), &server_intro.0)?;
    let (client_kem, session_keys) =
        client_handshake.finish(&server_intro.1, &opaque_finish.session_key, &transcript)?;

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
    let aead = ChunkAead::new(session_keys.send_key, *b"pl01");
    let mut sequence = 0u64;
    let mut archive_reader = tokio::fs::File::from_std(archive.reader()?);
    send_secure_request(
        &mut swarm,
        &options,
        &aead,
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
        send_secure_request(
            &mut swarm,
            &options,
            &aead,
            &mut sequence,
            SecureFrame::ArchiveChunk {
                bytes: buffer[..read].to_vec(),
            },
        )
        .await?;
    }

    send_secure_request(
        &mut swarm,
        &options,
        &aead,
        &mut sequence,
        SecureFrame::Done,
    )
    .await?;

    Ok(crate::direct::SentTransfer {
        endpoint: format!("{} via {}", options.peer_id, options.route_label),
        files: archive
            .manifest
            .entries
            .iter()
            .filter(|entry| entry.blake3.is_some())
            .count(),
        bytes: archive.manifest.total_bytes,
    })
}

async fn send_secure_request(
    swarm: &mut Swarm<TransferBehaviour>,
    options: &Libp2pSendOptions,
    aead: &ChunkAead,
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
            _ => {}
        }
    }
}
