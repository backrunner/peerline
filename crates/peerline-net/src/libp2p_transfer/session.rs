use crate::direct::ReceivedTransfer;
use libp2p::request_response;
use peerline_core::{Compression, TransferId};
use peerline_crypto::{ChunkAead, ClientHello, OpaqueServerResponse, ServerHandshake, Transcript};
use tempfile::NamedTempFile;

pub(crate) struct ReceiverSession {
    pub(crate) transfer_id: TransferId,
    pub(crate) total_bytes: u64,
    pub(crate) client_hello: ClientHello,
    pub(crate) transcript: Transcript,
    pub(crate) opaque_server: Option<OpaqueServerResponse>,
    pub(crate) server_handshake: Option<ServerHandshake>,
    pub(crate) aead: Option<ChunkAead>,
    pub(crate) compression: Option<Compression>,
    pub(crate) expected_sequence: u64,
    pub(crate) archive: Option<NamedTempFile>,
    pub(crate) pending_result: Option<(request_response::InboundRequestId, ReceivedTransfer)>,
}

impl ReceiverSession {
    pub(crate) fn new(
        client_hello: ClientHello,
        transcript: Transcript,
        opaque_server: OpaqueServerResponse,
        server_handshake: ServerHandshake,
        transfer_id: TransferId,
        total_bytes: u64,
    ) -> Self {
        Self {
            transfer_id,
            total_bytes,
            client_hello,
            transcript,
            opaque_server: Some(opaque_server),
            server_handshake: Some(server_handshake),
            aead: None,
            compression: None,
            expected_sequence: 0,
            archive: None,
            pending_result: None,
        }
    }
}
