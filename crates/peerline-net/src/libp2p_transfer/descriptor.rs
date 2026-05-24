use super::{Libp2pRecvOptions, behaviour::TransferBehaviour};
use crate::discovery::{direct_endpoints_with_extra, make_peer_descriptor};
use libp2p::{Swarm, kad};
use peerline_rendezvous_model::PeerDescriptor;
use std::net::SocketAddr;

pub(crate) fn publish_receiver_descriptor(
    swarm: &mut Swarm<TransferBehaviour>,
    record_key: kad::RecordKey,
    provider_key: kad::RecordKey,
    options: &Libp2pRecvOptions,
    extra_direct_endpoints: &[SocketAddr],
) -> anyhow::Result<PeerDescriptor> {
    let descriptor = make_peer_descriptor(
        swarm.local_peer_id().to_string(),
        direct_endpoints_with_extra(
            options.direct_bind,
            options.discovery.allow_loopback_endpoints,
            extra_direct_endpoints,
        )
        .into_iter()
        .map(|endpoint| endpoint.to_string())
        .collect(),
        swarm
            .listeners()
            .chain(swarm.external_addresses())
            .map(ToString::to_string)
            .collect(),
        options.public_tunnel_endpoints.clone(),
        options.tor_onion_endpoints.clone(),
    );
    tracing::debug!(
        peer_id = %descriptor.peer_id,
        direct_endpoints = descriptor.direct_endpoints.len(),
        libp2p_endpoints = descriptor.libp2p_endpoints.len(),
        "publishing receiver descriptor through DHT and rendezvous"
    );
    let record = kad::Record::new(record_key, postcard::to_allocvec(&descriptor)?);
    if let Err(error) = swarm
        .behaviour_mut()
        .kad
        .put_record(record, kad::Quorum::One)
    {
        tracing::warn!(%error, "could not start DHT descriptor publish");
    }
    if let Err(error) = swarm.behaviour_mut().kad.start_providing(provider_key) {
        tracing::warn!(%error, "could not start DHT provider publish");
    }
    Ok(descriptor)
}
