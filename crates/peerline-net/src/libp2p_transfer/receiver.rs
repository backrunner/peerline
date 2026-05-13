use super::{
    LIBP2P_ROUTE_LABEL, Libp2pRecvOptions,
    behaviour::{
        TransferBehaviour, TransferBehaviourEvent, apply_bootstrap, build_receiver_swarm,
        maybe_enable_relay_listeners,
    },
    descriptor::publish_receiver_descriptor,
    session::ReceiverSession,
};
use crate::{
    discovery::{descriptor_record_key, provider_record_key},
    protocol::{PROTOCOL_VERSION, SecureFrame, WireFrame, decrypt_secure, libp2p_transcript},
    rendezvous,
};
use futures::StreamExt;
use libp2p::{
    PeerId, Swarm, identify, kad, mdns, relay,
    request_response::{self, Event as RequestResponseEvent, Message as RequestResponseMessage},
    swarm::SwarmEvent,
};
use peerline_core::{ConnectionRoute, LookupKey, PeerlineEvent, TransferId, TransferStage};
use peerline_crypto::{ChunkAead, ServerHandshake, create_server_record, start_server_login};
use peerline_transfer::unpack_archive_from_reader;
use std::{collections::HashMap, io::Write, time::Duration};
use tempfile::NamedTempFile;
use tokio::time;

pub(crate) async fn recv_libp2p(
    options: Libp2pRecvOptions,
) -> anyhow::Result<crate::direct::ReceivedTransfer> {
    emit_event(
        &options.events,
        PeerlineEvent::StageChanged(TransferStage::Discovering),
    );
    let mut swarm = build_receiver_swarm(options.discovery.enable_mdns).await?;
    let lookup_key =
        peerline_core::NameCode::new(options.name.clone(), options.code.clone()).lookup_key();
    let record_key = descriptor_record_key(&lookup_key);
    let provider_key = provider_record_key(&lookup_key);

    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
    let _ = swarm.listen_on("/ip4/0.0.0.0/udp/0/webrtc-direct".parse()?);
    apply_bootstrap(&mut swarm, &options.discovery);
    maybe_enable_relay_listeners(&mut swarm, &options.discovery);
    emit_event(
        &options.events,
        PeerlineEvent::StageChanged(TransferStage::Connecting(ConnectionRoute::Libp2pDcutr)),
    );

    let mut sessions: HashMap<PeerId, ReceiverSession> = HashMap::new();
    let descriptor_publish_interval = Duration::from_secs(10);
    let mut descriptor_interval = time::interval_at(
        time::Instant::now() + descriptor_publish_interval,
        descriptor_publish_interval,
    );

    let descriptor = publish_receiver_descriptor(
        &mut swarm,
        record_key.clone(),
        provider_key.clone(),
        &options,
    )?;
    let mut rendezvous_registration = rendezvous::RendezvousRegistrationGuard::new(
        options.name.clone(),
        options.code.clone(),
        descriptor.peer_id.clone(),
        options.discovery.rendezvous.clone(),
    );
    rendezvous::publish_peer_descriptor_background(
        options.name.clone(),
        options.code.clone(),
        descriptor,
        options.discovery.rendezvous.clone(),
    );

    loop {
        tokio::select! {
            _ = descriptor_interval.tick() => {
                if let Ok(descriptor) = publish_receiver_descriptor(
                    &mut swarm,
                    record_key.clone(),
                    provider_key.clone(),
                    &options,
                ) {
                    rendezvous::publish_peer_descriptor_background(
                        options.name.clone(),
                        options.code.clone(),
                        descriptor,
                        options.discovery.rendezvous.clone(),
                    )
                }
            }
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::Behaviour(TransferBehaviourEvent::Transfer(event)) => {
                        let local_peer_id = *swarm.local_peer_id();
                        if let Some(done) = handle_transfer_event(
                            &mut swarm,
                            &mut sessions,
                            &local_peer_id,
                            event,
                            &options,
                            &lookup_key,
                        )? {
                            time::sleep(Duration::from_millis(100)).await;
                            emit_event(
                                &options.events,
                                PeerlineEvent::StageChanged(TransferStage::Complete),
                            );
                            rendezvous_registration.unregister().await;
                            return Ok(done);
                        }
                    }
                    SwarmEvent::Behaviour(TransferBehaviourEvent::Identify(identify::Event::Received { peer_id, info, .. })) => {
                        for addr in info.listen_addrs {
                            swarm.behaviour_mut().kad.add_address(&peer_id, addr);
                        }
                        if let Ok(descriptor) = publish_receiver_descriptor(
                            &mut swarm,
                            record_key.clone(),
                            provider_key.clone(),
                            &options,
                        ) {
                            rendezvous::publish_peer_descriptor_background(
                                options.name.clone(),
                                options.code.clone(),
                                descriptor,
                                options.discovery.rendezvous.clone(),
                            )
                        }
                    }
                    SwarmEvent::Behaviour(TransferBehaviourEvent::Mdns(mdns::Event::Discovered(peers))) => {
                        for (peer, addr) in peers {
                            swarm.behaviour_mut().kad.add_address(&peer, addr);
                        }
                    }
                    SwarmEvent::Behaviour(TransferBehaviourEvent::Relay(event)) => {
                        if matches!(
                            event,
                            relay::client::Event::ReservationReqAccepted { .. }
                                | relay::client::Event::OutboundCircuitEstablished { .. }
                                | relay::client::Event::InboundCircuitEstablished { .. }
                        ) && let Ok(descriptor) = publish_receiver_descriptor(
                                &mut swarm,
                                record_key.clone(),
                                provider_key.clone(),
                                &options,
                            )
                        {
                            rendezvous::publish_peer_descriptor_background(
                                options.name.clone(),
                                options.code.clone(),
                                descriptor,
                                options.discovery.rendezvous.clone(),
                            )
                        }
                    }
                    SwarmEvent::Behaviour(TransferBehaviourEvent::Kad(kad::Event::OutboundQueryProgressed { result, .. })) => {
                        log_dht_publish_result(&result);
                    }
                    SwarmEvent::NewListenAddr { .. }
                    | SwarmEvent::ConnectionEstablished { .. }
                    | SwarmEvent::ExternalAddrConfirmed { .. }
                    | SwarmEvent::NewExternalAddrCandidate { .. } => {
                        if let Ok(descriptor) = publish_receiver_descriptor(
                            &mut swarm,
                            record_key.clone(),
                            provider_key.clone(),
                            &options,
                        ) {
                            rendezvous::publish_peer_descriptor_background(
                                options.name.clone(),
                                options.code.clone(),
                                descriptor,
                                options.discovery.rendezvous.clone(),
                            )
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

fn log_dht_publish_result(result: &kad::QueryResult) {
    match result {
        kad::QueryResult::PutRecord(Ok(ok)) => {
            tracing::debug!(key = ?ok.key, "DHT descriptor published");
        }
        kad::QueryResult::PutRecord(Err(kad::PutRecordError::QuorumFailed {
            success,
            quorum,
            ..
        })) if success.is_empty() => {
            tracing::debug!(
                stored = success.len(),
                needed = %quorum,
                "DHT descriptor publish waiting for peers"
            );
        }
        kad::QueryResult::PutRecord(Err(error)) => {
            tracing::warn!(%error, "DHT descriptor publish failed");
        }
        kad::QueryResult::StartProviding(Ok(ok)) => {
            tracing::debug!(key = ?ok.key, "DHT provider record published");
        }
        kad::QueryResult::StartProviding(Err(error)) => {
            tracing::warn!(%error, "DHT provider record publish failed");
        }
        kad::QueryResult::Bootstrap(Ok(ok)) => {
            tracing::debug!(peer = %ok.peer, remaining = ok.num_remaining, "DHT bootstrap progressed");
        }
        kad::QueryResult::Bootstrap(Err(error)) => {
            tracing::debug!(%error, "DHT bootstrap failed");
        }
        _ => {}
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

fn handle_transfer_event(
    swarm: &mut Swarm<TransferBehaviour>,
    sessions: &mut HashMap<PeerId, ReceiverSession>,
    receiver_peer_id: &PeerId,
    event: RequestResponseEvent<WireFrame, WireFrame>,
    options: &Libp2pRecvOptions,
    lookup_key: &LookupKey,
) -> anyhow::Result<Option<crate::direct::ReceivedTransfer>> {
    match event {
        RequestResponseEvent::Message {
            peer,
            message:
                RequestResponseMessage::Request {
                    request_id,
                    request,
                    channel,
                },
            ..
        } => {
            let response = handle_inbound_request(
                sessions,
                receiver_peer_id,
                peer,
                request_id,
                request,
                options,
                lookup_key,
            )?;
            match response {
                InboundResponse::Frame(frame) => {
                    let _ = swarm.behaviour_mut().transfer.send_response(channel, frame);
                    Ok(None)
                }
            }
        }
        RequestResponseEvent::Message {
            message: RequestResponseMessage::Response { .. },
            ..
        } => Ok(None),
        RequestResponseEvent::ResponseSent {
            peer, request_id, ..
        } => {
            let completed = if let Some(session) = sessions.get_mut(&peer) {
                if let Some((pending_request_id, result)) = session.pending_result.take() {
                    if pending_request_id == request_id {
                        Some(result)
                    } else {
                        session.pending_result = Some((pending_request_id, result));
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(result) = completed {
                sessions.remove(&peer);
                Ok(Some(result))
            } else {
                Ok(None)
            }
        }
        RequestResponseEvent::OutboundFailure { .. }
        | RequestResponseEvent::InboundFailure { .. } => Ok(None),
    }
}

enum InboundResponse {
    Frame(WireFrame),
}

fn handle_inbound_request(
    sessions: &mut HashMap<PeerId, ReceiverSession>,
    receiver_peer_id: &PeerId,
    peer: PeerId,
    request_id: request_response::InboundRequestId,
    request: WireFrame,
    options: &Libp2pRecvOptions,
    lookup_key: &LookupKey,
) -> anyhow::Result<InboundResponse> {
    match request {
        WireFrame::ClientIntro {
            version,
            name,
            files,
            bytes,
            opaque_request,
            client_hello,
        } if version == PROTOCOL_VERSION => {
            if let Some(name) = name.as_ref()
                && name != &options.name
            {
                return Ok(InboundResponse::Frame(WireFrame::Error {
                    message: "receiver name mismatch".into(),
                }));
            }

            let negotiated_name = name.clone().unwrap_or_else(|| options.name.clone());
            let transfer_id = TransferId::random();
            emit_event(
                &options.events,
                PeerlineEvent::TransferStarted {
                    id: transfer_id,
                    peer: peer.to_string(),
                    files,
                    bytes,
                },
            );
            emit_event(
                &options.events,
                PeerlineEvent::StageChanged(TransferStage::Authenticating),
            );
            let transcript = libp2p_transcript(
                &negotiated_name,
                lookup_key,
                &receiver_peer_id.to_string(),
                LIBP2P_ROUTE_LABEL,
            );
            let record = create_server_record(
                options.code.as_str().as_bytes(),
                options.name.as_str().as_bytes(),
            )?;
            let opaque_server = start_server_login(&record, &opaque_request)?;
            let server_handshake = ServerHandshake::start(&client_hello)?;
            let response = opaque_server.response.clone();
            let hello = server_handshake.hello.clone();

            sessions.insert(
                peer,
                ReceiverSession::new(
                    client_hello,
                    transcript,
                    opaque_server,
                    server_handshake,
                    transfer_id,
                    bytes,
                ),
            );

            Ok(InboundResponse::Frame(WireFrame::ServerIntro {
                version: PROTOCOL_VERSION,
                opaque_response: response,
                server_hello: hello,
            }))
        }
        WireFrame::ClientFinish {
            opaque_finalization,
            client_kem,
        } => {
            let session = sessions
                .get_mut(&peer)
                .ok_or_else(|| anyhow::anyhow!("libp2p session not initialized"))?;
            let opaque_server = session
                .opaque_server
                .take()
                .ok_or_else(|| anyhow::anyhow!("missing opaque server state"))?;
            let server_handshake = session
                .server_handshake
                .take()
                .ok_or_else(|| anyhow::anyhow!("missing server handshake"))?;
            let opaque_key = opaque_server.finish(&opaque_finalization)?;
            let keys = server_handshake.finish(
                &session.client_hello,
                &client_kem,
                &opaque_key,
                &session.transcript,
            )?;
            session.aead = Some(ChunkAead::new(keys.recv_key, *b"pl01"));
            Ok(InboundResponse::Frame(WireFrame::Ack))
        }
        WireFrame::Secure(encrypted) => {
            let session = sessions
                .get_mut(&peer)
                .ok_or_else(|| anyhow::anyhow!("libp2p session not initialized"))?;
            let aead = session
                .aead
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("secure channel not ready"))?;
            let frame = decrypt_secure(aead, &mut session.expected_sequence, encrypted)?;
            match frame {
                SecureFrame::Header { compression } => {
                    if session.compression.is_some() {
                        return Ok(InboundResponse::Frame(WireFrame::Error {
                            message: "duplicate secure header".into(),
                        }));
                    }
                    session.compression = Some(compression);
                    emit_event(
                        &options.events,
                        PeerlineEvent::StageChanged(TransferStage::ReceivingManifest),
                    );
                    std::fs::create_dir_all(&options.destination)?;
                    session.archive = Some(NamedTempFile::new_in(&options.destination)?);
                    Ok(InboundResponse::Frame(WireFrame::Ack))
                }
                SecureFrame::ArchiveChunk { bytes } => {
                    if session.compression.is_none() {
                        return Ok(InboundResponse::Frame(WireFrame::Error {
                            message: "secure stream must start with header".into(),
                        }));
                    }
                    let archive = session
                        .archive
                        .as_mut()
                        .ok_or_else(|| anyhow::anyhow!("secure archive sink not ready"))?;
                    archive.as_file_mut().write_all(&bytes)?;
                    emit_event(
                        &options.events,
                        PeerlineEvent::Progress {
                            id: session.transfer_id,
                            bytes_done: archive.as_file().metadata()?.len(),
                            bytes_total: session.total_bytes,
                        },
                    );
                    Ok(InboundResponse::Frame(WireFrame::Ack))
                }
                SecureFrame::Done => {
                    let compression = session
                        .compression
                        .ok_or_else(|| anyhow::anyhow!("secure stream must start with header"))?;
                    let mut archive = session
                        .archive
                        .take()
                        .ok_or_else(|| anyhow::anyhow!("secure archive sink not ready"))?;
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
                    let result = crate::direct::ReceivedTransfer {
                        peer: peer.to_string(),
                        files: manifest
                            .entries
                            .iter()
                            .filter(|entry| entry.blake3.is_some())
                            .count(),
                        bytes: manifest.total_bytes,
                    };
                    session.pending_result = Some((request_id, result));
                    Ok(InboundResponse::Frame(WireFrame::Ack))
                }
            }
        }
        WireFrame::Ack | WireFrame::Error { .. } => Ok(InboundResponse::Frame(WireFrame::Error {
            message: "unexpected frame".into(),
        })),
        _ => Ok(InboundResponse::Frame(WireFrame::Error {
            message: "unexpected libp2p frame".into(),
        })),
    }
}
