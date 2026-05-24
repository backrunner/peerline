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
    rendezvous, resume,
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
use std::{collections::HashMap, future, net::SocketAddr, time::Duration};
use tokio::time;

pub(crate) async fn recv_libp2p(
    options: Libp2pRecvOptions,
) -> anyhow::Result<crate::direct::ReceivedTransfer> {
    emit_event(
        &options.events,
        PeerlineEvent::StageChanged(TransferStage::Discovering),
    );
    let mut swarm = build_receiver_swarm(
        options.discovery.enable_mdns,
        options.discovery.enable_upnp,
        &options.discovery.webrtc_ice_servers,
    )
    .await?;
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
    emit_event(
        &options.events,
        PeerlineEvent::Message(
            "listening on direct TCP, libp2p TCP/QUIC/WebRTC-direct, and relay fallback".into(),
        ),
    );

    let mut sessions: HashMap<PeerId, ReceiverSession> = HashMap::new();
    let descriptor_publish_interval = Duration::from_secs(10);
    let mut descriptor_interval = time::interval_at(
        time::Instant::now() + descriptor_publish_interval,
        descriptor_publish_interval,
    );
    let direct_mapping = options
        .discovery
        .enable_upnp
        .then(|| crate::direct::spawn_direct_port_mapping(options.direct_bind));
    let mut direct_mapping_rx = direct_mapping
        .as_ref()
        .map(crate::direct::DirectPortMapping::subscribe);
    let mut mapped_direct_endpoints = direct_mapping
        .as_ref()
        .map(crate::direct::DirectPortMapping::endpoints)
        .unwrap_or_default();

    let descriptor = publish_receiver_descriptor(
        &mut swarm,
        record_key.clone(),
        provider_key.clone(),
        &options,
        &mapped_direct_endpoints,
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
                republish_receiver_descriptor(
                    &mut swarm,
                    record_key.clone(),
                    provider_key.clone(),
                    &options,
                    &mapped_direct_endpoints,
                );
            }
            endpoints = wait_for_direct_mapping_change(&mut direct_mapping_rx) => {
                if let Some(endpoints) = endpoints {
                    mapped_direct_endpoints = endpoints;
                    republish_receiver_descriptor(
                        &mut swarm,
                        record_key.clone(),
                        provider_key.clone(),
                        &options,
                        &mapped_direct_endpoints,
                    );
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
                        republish_receiver_descriptor(
                            &mut swarm,
                            record_key.clone(),
                            provider_key.clone(),
                            &options,
                            &mapped_direct_endpoints,
                        );
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
                        ) {
                            republish_receiver_descriptor(
                                &mut swarm,
                                record_key.clone(),
                                provider_key.clone(),
                                &options,
                                &mapped_direct_endpoints,
                            );
                        }
                    }
                    SwarmEvent::Behaviour(TransferBehaviourEvent::Kad(kad::Event::OutboundQueryProgressed { result, .. })) => {
                        log_dht_publish_result(&result);
                    }
                    SwarmEvent::NewListenAddr { .. }
                    | SwarmEvent::ConnectionEstablished { .. }
                    | SwarmEvent::ExternalAddrConfirmed { .. }
                    | SwarmEvent::ExternalAddrExpired { .. }
                    | SwarmEvent::NewExternalAddrCandidate { .. } => {
                        republish_receiver_descriptor(
                            &mut swarm,
                            record_key.clone(),
                            provider_key.clone(),
                            &options,
                            &mapped_direct_endpoints,
                        );
                    }
                    _ => {}
                }
            }
        }
    }
}

fn republish_receiver_descriptor(
    swarm: &mut Swarm<TransferBehaviour>,
    record_key: kad::RecordKey,
    provider_key: kad::RecordKey,
    options: &Libp2pRecvOptions,
    extra_direct_endpoints: &[SocketAddr],
) {
    if let Ok(descriptor) = publish_receiver_descriptor(
        swarm,
        record_key,
        provider_key,
        options,
        extra_direct_endpoints,
    ) {
        rendezvous::publish_peer_descriptor_background(
            options.name.clone(),
            options.code.clone(),
            descriptor,
            options.discovery.rendezvous.clone(),
        )
    }
}

async fn wait_for_direct_mapping_change(
    receiver: &mut Option<tokio::sync::watch::Receiver<Vec<SocketAddr>>>,
) -> Option<Vec<SocketAddr>> {
    let Some(receiver) = receiver.as_mut() else {
        future::pending::<()>().await;
        return None;
    };
    receiver.changed().await.ok()?;
    Some(receiver.borrow().clone())
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
            let result = if let Some(session) = sessions.get_mut(&peer) {
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
            let should_remove_session = result.is_some()
                || sessions
                    .get(&peer)
                    .is_some_and(|session| session.pending_error == Some(request_id));
            if should_remove_session {
                sessions.remove(&peer);
            }
            if let Some(result) = result {
                Ok(Some(result))
            } else {
                Ok(None)
            }
        }
        RequestResponseEvent::OutboundFailure { .. }
        | RequestResponseEvent::InboundFailure { .. } => {
            emit_event(
                &options.events,
                PeerlineEvent::StageChanged(TransferStage::Failed(
                    "libp2p transfer request failed".into(),
                )),
            );
            Ok(None)
        }
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
            descriptor,
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
            let resume_state = resume::resume_state(&options.destination, &descriptor)?;
            let resume_offset = resume_state.offset;
            emit_event(
                &options.events,
                PeerlineEvent::TransferStarted {
                    id: transfer_id,
                    peer: peer.to_string(),
                    files: descriptor.files,
                    bytes: descriptor.archive_bytes,
                    resume_offset,
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
                    descriptor,
                    resume_state,
                ),
            );

            Ok(InboundResponse::Frame(WireFrame::ServerIntro {
                version: PROTOCOL_VERSION,
                resume_offset,
                opaque_response: response,
                server_hello: hello,
            }))
        }
        WireFrame::ClientIntro { version, .. } => Ok(InboundResponse::Frame(WireFrame::Error {
            message: format!(
                "incompatible peerline protocol version {version}; expected {PROTOCOL_VERSION}"
            ),
        })),
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
                    if compression != session.descriptor.compression {
                        return Ok(InboundResponse::Frame(WireFrame::Error {
                            message: "secure header compression mismatch".into(),
                        }));
                    }
                    session.compression = Some(compression);
                    emit_event(
                        &options.events,
                        PeerlineEvent::StageChanged(TransferStage::ReceivingManifest),
                    );
                    Ok(InboundResponse::Frame(WireFrame::Ack))
                }
                SecureFrame::ArchiveChunk { bytes } => {
                    if session.compression.is_none() {
                        return Ok(InboundResponse::Frame(WireFrame::Error {
                            message: "secure stream must start with header".into(),
                        }));
                    }
                    let bytes_done = resume::append_chunk(
                        &mut session.resume_state,
                        &session.descriptor,
                        &bytes,
                    )?;
                    emit_event(
                        &options.events,
                        PeerlineEvent::Progress {
                            id: session.transfer_id,
                            bytes_done,
                            bytes_total: session.descriptor.archive_bytes,
                        },
                    );
                    Ok(InboundResponse::Frame(WireFrame::Ack))
                }
                SecureFrame::Done => {
                    let compression = session
                        .compression
                        .ok_or_else(|| anyhow::anyhow!("secure stream must start with header"))?;
                    emit_event(
                        &options.events,
                        PeerlineEvent::StageChanged(TransferStage::Verifying),
                    );
                    let archive =
                        resume::complete_partial(&session.resume_state, &session.descriptor)?;
                    let result = unpack_archive_from_reader(
                        &options.destination,
                        compression,
                        archive,
                        options.overwrite,
                    )
                    .map(|manifest| crate::direct::ReceivedTransfer {
                        peer: peer.to_string(),
                        files: manifest
                            .entries
                            .iter()
                            .filter(|entry| entry.blake3.is_some())
                            .count(),
                        bytes: manifest.total_bytes,
                    });

                    match result {
                        Ok(result) => {
                            resume::remove_partial(&session.resume_state)?;
                            emit_event(
                                &options.events,
                                PeerlineEvent::StageChanged(TransferStage::Complete),
                            );
                            session.pending_result = Some((request_id, result));
                            Ok(InboundResponse::Frame(WireFrame::Ack))
                        }
                        Err(error) => {
                            let _ = resume::remove_partial(&session.resume_state);
                            let message = error.to_string();
                            emit_event(
                                &options.events,
                                PeerlineEvent::StageChanged(TransferStage::Failed(message.clone())),
                            );
                            session.pending_error = Some(request_id);
                            Ok(InboundResponse::Frame(WireFrame::Error { message }))
                        }
                    }
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
