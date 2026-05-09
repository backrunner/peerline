mod behaviour;
mod codec;
mod descriptor;
mod receiver;
mod sender;
mod session;

#[cfg(test)]
mod tests;

use crate::discovery::DiscoveryConfig;
use peerline_core::{Compression, ConnectionRoute, HumanCode, HumanName, PeerlineEvent};
use std::{net::SocketAddr, path::PathBuf};

pub(crate) const LIBP2P_ROUTE_LABEL: &str = "libp2p-request-response";

#[derive(Clone, Debug)]
pub struct Libp2pSendOptions {
    pub peer_id: libp2p::PeerId,
    pub addresses: Vec<libp2p::Multiaddr>,
    pub name: HumanName,
    pub code: HumanCode,
    pub paths: Vec<PathBuf>,
    pub compression: Compression,
    pub route: ConnectionRoute,
    pub events: Option<tokio::sync::mpsc::UnboundedSender<PeerlineEvent>>,
}

#[derive(Clone, Debug)]
pub struct Libp2pRecvOptions {
    pub name: HumanName,
    pub code: HumanCode,
    pub direct_bind: SocketAddr,
    pub destination: PathBuf,
    pub overwrite: bool,
    pub discovery: DiscoveryConfig,
    pub events: Option<tokio::sync::mpsc::UnboundedSender<PeerlineEvent>>,
}

pub async fn send_libp2p(
    options: Libp2pSendOptions,
) -> anyhow::Result<crate::direct::SentTransfer> {
    sender::send_libp2p(options).await
}

pub async fn recv_libp2p(
    options: Libp2pRecvOptions,
) -> anyhow::Result<crate::direct::ReceivedTransfer> {
    receiver::recv_libp2p(options).await
}
