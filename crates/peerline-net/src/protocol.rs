use peerline_core::{Compression, HumanName, LookupKey, TransferDescriptor};
use peerline_crypto::{ChunkAead, ClientHello, ClientKem, EncryptedChunk, ServerHello, Transcript};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

// Version 4 authenticates the byte-stream receiver's completion acknowledgement.
pub(crate) const PROTOCOL_VERSION: u16 = 4;
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
    // Local decoding result, never a wire message. Keep the intro's variant
    // tag and version prefix stable even when a future version changes its body.
    #[serde(skip)]
    UnsupportedIntro {
        version: u16,
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
    aead: &mut ChunkAead,
    sequence: &mut u64,
    frame: &SecureFrame,
) -> anyhow::Result<()> {
    let encrypted = encrypt_secure(aead, sequence, frame)?;
    write_wire(writer, &WireFrame::Secure(encrypted)).await
}

pub(crate) async fn read_secure<R: AsyncRead + Unpin>(
    reader: &mut R,
    aead: &mut ChunkAead,
    expected_sequence: &mut u64,
) -> anyhow::Result<SecureFrame> {
    let encrypted = match read_wire(reader).await? {
        WireFrame::Secure(encrypted) => encrypted,
        _ => anyhow::bail!("expected secure frame"),
    };
    decrypt_secure(aead, expected_sequence, encrypted)
}

pub(crate) fn encrypt_secure(
    aead: &mut ChunkAead,
    sequence: &mut u64,
    frame: &SecureFrame,
) -> anyhow::Result<EncryptedChunk> {
    let payload = postcard::to_allocvec(frame)?;
    let encrypted = aead.encrypt(*sequence, SECURE_AAD, &payload)?;
    *sequence += 1;
    Ok(encrypted)
}

pub(crate) fn decrypt_secure(
    aead: &mut ChunkAead,
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
    anyhow::ensure!(payload.len() <= MAX_FRAME, "wire frame exceeds max size");
    let (tag, rest) = postcard::take_from_bytes::<u32>(payload)?;
    if matches!(tag, 0 | 1) {
        let (version, _) = postcard::take_from_bytes::<u16>(rest)?;
        if version != PROTOCOL_VERSION {
            return Ok(WireFrame::UnsupportedIntro { version });
        }
    }
    Ok(postcard::from_bytes(payload)?)
}

pub(crate) fn version_mismatch_message(version: u16) -> String {
    format!(
        "incompatible peerline protocol version {version}; expected {PROTOCOL_VERSION}; \
         upgrade the installation using the older protocol; automatic downgrade is disabled"
    )
}

pub(crate) fn direct_transcript(name: Option<&HumanName>) -> Transcript {
    direct_transcript_for_route(name, "direct-tcp")
}

pub(crate) fn direct_transcript_for_route(
    name: Option<&HumanName>,
    route_label: &str,
) -> Transcript {
    let transcript = Transcript::new("peerline:direct:v1")
        .append("version", PROTOCOL_VERSION.to_be_bytes())
        .append("route", route_label.as_bytes());
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

#[cfg(test)]
mod tests {
    use super::*;
    use peerline_core::Compression;

    #[test]
    fn incompatible_intro_is_identified_before_decoding_its_body() {
        for tag in [0, 1] {
            for version in [0, 3, 5, u16::MAX] {
                let mut payload = vec![tag];
                payload.extend(postcard::to_allocvec(&version).unwrap());
                payload.extend([0xff; 16]); // Not a valid current handshake body.
                assert!(matches!(
                    decode_wire(&payload).unwrap(),
                    WireFrame::UnsupportedIntro { version: found } if found == version
                ));
            }
        }
        for payload in [
            vec![],
            vec![0],
            vec![1, 0x80],
            vec![0, 4],
            vec![1, 4],
            vec![6],
        ] {
            assert!(decode_wire(&payload).is_err());
        }
    }

    #[test]
    fn local_version_result_does_not_change_wire_error_format() {
        assert!(encode_wire(&WireFrame::UnsupportedIntro { version: 3 }).is_err());
        let client = WireFrame::ClientIntro {
            version: 4,
            name: None,
            descriptor: TransferDescriptor {
                source_id: peerline_core::NodeId::random(),
                resource_id: peerline_core::ResourceId::from_bytes([0; 32]),
                archive_bytes: 0,
                logical_bytes: 0,
                files: 0,
                compression: Compression::None,
            },
            opaque_request: vec![],
            client_hello: ClientHello {
                x25519_public: [0; 32],
            },
        };
        let server = WireFrame::ServerIntro {
            version: 4,
            resume_offset: 0,
            opaque_response: vec![],
            server_hello: ServerHello {
                x25519_public: [0; 32],
                ml_kem_public: vec![],
            },
        };
        assert_eq!(&encode_wire(&client).unwrap()[..2], &[0, 4]);
        assert_eq!(&encode_wire(&server).unwrap()[..2], &[1, 4]);
        assert_eq!(encode_wire(&WireFrame::Ack).unwrap(), [4]);
        let error = WireFrame::Error {
            message: "x".into(),
        };
        // These tags and the Error string are also understood by v3 peers.
        assert_eq!(encode_wire(&error).unwrap(), [5, 1, b'x']);
        assert!(matches!(
            decode_wire(&[5, 1, b'x']).unwrap(),
            WireFrame::Error { .. }
        ));
    }

    #[test]
    fn secure_frames_use_ratcheted_message_keys() {
        let mut sender = ChunkAead::new([7u8; 32], *b"pl01");
        let mut receiver = ChunkAead::new([7u8; 32], *b"pl01");
        let mut sequence = 0;
        let mut expected_sequence = 0;

        let first = encrypt_secure(
            &mut sender,
            &mut sequence,
            &SecureFrame::Header {
                compression: Compression::None,
            },
        )
        .unwrap();
        let second = encrypt_secure(
            &mut sender,
            &mut sequence,
            &SecureFrame::ArchiveChunk {
                bytes: b"hello".to_vec(),
            },
        )
        .unwrap();

        assert!(matches!(
            decrypt_secure(&mut receiver, &mut expected_sequence, first.clone()).unwrap(),
            SecureFrame::Header {
                compression: Compression::None
            }
        ));
        assert!(matches!(
            decrypt_secure(&mut receiver, &mut expected_sequence, second).unwrap(),
            SecureFrame::ArchiveChunk { bytes } if bytes == b"hello"
        ));

        let mut replay_sequence = 0;
        assert!(decrypt_secure(&mut receiver, &mut replay_sequence, first).is_err());
    }
}
