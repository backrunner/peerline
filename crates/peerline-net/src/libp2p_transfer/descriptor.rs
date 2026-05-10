use super::{Libp2pRecvOptions, behaviour::TransferBehaviour};
use crate::{
    discovery::{direct_endpoints, make_peer_descriptor},
    protocol::PROTOCOL_VERSION,
};
use libp2p::{Swarm, kad};
use peerline_rendezvous_model::PeerDescriptor;

pub(crate) fn publish_receiver_descriptor(
    swarm: &mut Swarm<TransferBehaviour>,
    record_key: kad::RecordKey,
    provider_key: kad::RecordKey,
    options: &Libp2pRecvOptions,
) -> anyhow::Result<PeerDescriptor> {
    let descriptor = make_peer_descriptor(
        PROTOCOL_VERSION,
        swarm.local_peer_id().to_string(),
        direct_endpoints(
            options.direct_bind,
            options.discovery.allow_loopback_endpoints,
        )
        .into_iter()
        .map(|endpoint| endpoint.to_string())
        .collect(),
        swarm
            .listeners()
            .chain(swarm.external_addresses())
            .map(ToString::to_string)
            .collect(),
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
