use crate::manifest::TransferId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionRoute {
    LanDirect,
    PublicDirect,
    PublicTunnel,
    TorOnion,
    Libp2pQuic,
    Libp2pDcutr,
    Libp2pRelay,
    WebRtcDirect,
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
pub enum PeerlineLogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerlineEvent {
    Shutdown,
    StageChanged(TransferStage),
    TransferStarted {
        id: TransferId,
        peer: String,
        files: usize,
        bytes: u64,
        resume_offset: u64,
    },
    Progress {
        id: TransferId,
        bytes_done: u64,
        bytes_total: u64,
    },
    Message(String),
    Log {
        level: PeerlineLogLevel,
        target: String,
        message: String,
    },
}
