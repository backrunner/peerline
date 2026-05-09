use crate::manifest::TransferId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionRoute {
    LanDirect,
    PublicDirect,
    Libp2pDcutr,
    Libp2pRelay,
    WebRtcTurn,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferStage {
    Discovering,
    Connecting(ConnectionRoute),
    Authenticating,
    ReceivingManifest,
    Transferring,
    Verifying,
    Complete,
    Failed(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerlineEvent {
    StageChanged(TransferStage),
    TransferStarted {
        id: TransferId,
        peer: String,
        files: usize,
        bytes: u64,
    },
    Progress {
        id: TransferId,
        bytes_done: u64,
        bytes_total: u64,
    },
    Message(String),
}
