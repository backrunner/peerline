use peerline_core::{Compression, HumanName, LookupKey, TransferDescriptor};
use peerline_crypto::{ChunkAead, ClientHello, ClientKem, EncryptedChunk, ServerHello, Transcript};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub(crate) const PROTOCOL_VERSION: u16 = 2;
pub(crate) const MAX_FRAME: usize = 64 * 1024 * 1024;
pub(crate) const SECURE_AAD: &[u8] = b"peerline:secure-frame:v1";
pub(crate) const LIBP2P_TRANSFER_PROTOCOL: &str = "/peerline/transfer/1";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum WireFrame {
    ClientIntro {
        version: u16,
        name: Option<HumanName>,
        descriptor: TransferDescriptor,
        opaque_request: Vec<u8>,
        client_hello: ClientHello,
    },
    ServerIntro {
        version: u16,
        resume_offset: u64,
        opaque_response: Vec<u8>,
        server_hello: ServerHello,
    },
    ClientFinish {
        opaque_finalization: Vec<u8>,
        client_kem: ClientKem,
    },
    Secure(EncryptedChunk),
    Ack,
    Error {
        message: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum SecureFrame {
    Header { compression: Compression },
    ArchiveChunk { bytes: Vec<u8> },
    Done,
}

pub(crate) async fn write_secure<W: AsyncWrite + Unpin>(
    writer: &mut W,
    aead: &ChunkAead,
    sequence: &mut u64,
    frame: &SecureFrame,
) -> anyhow::Result<()> {
    let encrypted = encrypt_secure(aead, sequence, frame)?;
    write_wire(writer, &WireFrame::Secure(encrypted)).await
}

pub(crate) async fn read_secure<R: AsyncRead + Unpin>(
    reader: &mut R,
    aead: &ChunkAead,
    expected_sequence: &mut u64,
) -> anyhow::Result<SecureFrame> {
    let encrypted = match read_wire(reader).await? {
        WireFrame::Secure(encrypted) => encrypted,
        _ => anyhow::bail!("expected secure frame"),
    };
    decrypt_secure(aead, expected_sequence, encrypted)
}

pub(crate) fn encrypt_secure(
    aead: &ChunkAead,
    sequence: &mut u64,
    frame: &SecureFrame,
) -> anyhow::Result<EncryptedChunk> {
    let payload = postcard::to_allocvec(frame)?;
    let encrypted = aead.encrypt(*sequence, SECURE_AAD, &payload)?;
    *sequence += 1;
    Ok(encrypted)
}

pub(crate) fn decrypt_secure(
    aead: &ChunkAead,
    expected_sequence: &mut u64,
    encrypted: EncryptedChunk,
) -> anyhow::Result<SecureFrame> {
    if encrypted.sequence != *expected_sequence {
        anyhow::bail!("secure frame sequence mismatch");
    }
    *expected_sequence += 1;
    let plaintext = aead.decrypt(SECURE_AAD, &encrypted)?;
    Ok(postcard::from_bytes(&plaintext)?)
}

pub(crate) async fn write_wire<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &WireFrame,
) -> anyhow::Result<()> {
    let payload = encode_wire(frame)?;
    writer
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await?;
    writer.write_all(&payload).await?;
    Ok(())
}

pub(crate) async fn read_wire<R: AsyncRead + Unpin>(reader: &mut R) -> anyhow::Result<WireFrame> {
    let mut len = [0u8; 4];
    reader.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME {
        anyhow::bail!("wire frame exceeds max size");
    }
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;
    decode_wire(&payload)
}

pub(crate) fn encode_wire(frame: &WireFrame) -> anyhow::Result<Vec<u8>> {
    Ok(postcard::to_allocvec(frame)?)
}

pub(crate) fn decode_wire(payload: &[u8]) -> anyhow::Result<WireFrame> {
    Ok(postcard::from_bytes(payload)?)
}

pub(crate) fn direct_transcript(name: Option<&HumanName>) -> Transcript {
    let transcript = Transcript::new("peerline:direct:v1")
        .append("version", PROTOCOL_VERSION.to_be_bytes())
        .append("route", b"direct-tcp");
    match name {
        Some(name) => transcript.append("name", name.as_str().as_bytes()),
        None => transcript.append("name", b"direct-ip"),
    }
}

pub(crate) fn libp2p_transcript(
    name: &HumanName,
    lookup_key: &LookupKey,
    receiver_peer_id: &str,
    route_label: &str,
) -> Transcript {
    Transcript::new("peerline:libp2p:v1")
        .append("version", PROTOCOL_VERSION.to_be_bytes())
        .append("route", route_label.as_bytes())
        .append("name", name.as_str().as_bytes())
        .append("lookup-key", lookup_key.bytes())
        .append("receiver-peer-id", receiver_peer_id.as_bytes())
}
