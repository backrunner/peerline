use crate::direct::ReceivedTransfer;
use crate::resume::ResumeState;
use libp2p::request_response;
use peerline_core::{Compression, TransferDescriptor, TransferId};
use peerline_crypto::{ChunkAead, ClientHello, OpaqueServerResponse, ServerHandshake, Transcript};

pub(crate) struct ReceiverSession {
    pub(crate) transfer_id: TransferId,
    pub(crate) descriptor: TransferDescriptor,
    pub(crate) client_hello: ClientHello,
    pub(crate) transcript: Transcript,
    pub(crate) opaque_server: Option<OpaqueServerResponse>,
    pub(crate) server_handshake: Option<ServerHandshake>,
    pub(crate) aead: Option<ChunkAead>,
    pub(crate) compression: Option<Compression>,
    pub(crate) expected_sequence: u64,
    pub(crate) resume_state: ResumeState,
    pub(crate) pending_result: Option<(request_response::InboundRequestId, ReceivedTransfer)>,
    pub(crate) pending_error: Option<request_response::InboundRequestId>,
}

impl ReceiverSession {
    pub(crate) fn new(
        client_hello: ClientHello,
        transcript: Transcript,
        opaque_server: OpaqueServerResponse,
        server_handshake: ServerHandshake,
        transfer_id: TransferId,
        descriptor: TransferDescriptor,
        resume_state: ResumeState,
    ) -> Self {
        Self {
            transfer_id,
            descriptor,
            client_hello,
            transcript,
            opaque_server: Some(opaque_server),
            server_handshake: Some(server_handshake),
            aead: None,
            compression: None,
            expected_sequence: 0,
            resume_state,
            pending_result: None,
            pending_error: None,
        }
    }
}
