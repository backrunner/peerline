use chacha20poly1305::{
    ChaCha20Poly1305, KeyInit,
    aead::{Aead, Payload},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AeadError {
    #[error("encryption failed")]
    Encrypt,
    #[error("decryption failed")]
    Decrypt,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptedChunk {
    pub sequence: u64,
    pub ciphertext: Vec<u8>,
}

#[derive(Clone)]
pub struct ChunkAead {
    cipher: ChaCha20Poly1305,
    nonce_prefix: [u8; 4],
}

impl ChunkAead {
    pub fn new(key: [u8; 32], nonce_prefix: [u8; 4]) -> Self {
        Self {
            cipher: ChaCha20Poly1305::new(&key.into()),
            nonce_prefix,
        }
    }

    pub fn encrypt(
        &self,
        sequence: u64,
        aad: &[u8],
        plaintext: &[u8],
    ) -> Result<EncryptedChunk, AeadError> {
        let nonce = self.nonce(sequence);
        let ciphertext = self
            .cipher
            .encrypt(
                &nonce.into(),
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|_| AeadError::Encrypt)?;
        Ok(EncryptedChunk {
            sequence,
            ciphertext,
        })
    }

    pub fn decrypt(&self, aad: &[u8], chunk: &EncryptedChunk) -> Result<Vec<u8>, AeadError> {
        let nonce = self.nonce(chunk.sequence);
        self.cipher
            .decrypt(
                &nonce.into(),
                Payload {
                    msg: &chunk.ciphertext,
                    aad,
                },
            )
            .map_err(|_| AeadError::Decrypt)
    }

    fn nonce(&self, sequence: u64) -> [u8; 12] {
        let mut nonce = [0u8; 12];
        nonce[..4].copy_from_slice(&self.nonce_prefix);
        nonce[4..].copy_from_slice(&sequence.to_be_bytes());
        nonce
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_tampering() {
        let aead = ChunkAead::new([7u8; 32], *b"send");
        let mut chunk = aead.encrypt(0, b"aad", b"hello").unwrap();
        chunk.ciphertext[0] ^= 1;
        assert!(aead.decrypt(b"aad", &chunk).is_err());
    }
}
