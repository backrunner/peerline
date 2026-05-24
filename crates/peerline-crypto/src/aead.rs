use chacha20poly1305::{
    ChaCha20Poly1305, KeyInit,
    aead::{Aead, Payload},
};
use hkdf::Hkdf;
use serde::{Deserialize, Serialize};
use sha2::Sha512;
use thiserror::Error;
use zeroize::Zeroize;

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

pub struct ChunkAead {
    chain_key: [u8; 32],
    nonce_prefix: [u8; 4],
}

impl ChunkAead {
    pub fn new(key: [u8; 32], nonce_prefix: [u8; 4]) -> Self {
        Self {
            chain_key: key,
            nonce_prefix,
        }
    }

    pub fn encrypt(
        &mut self,
        sequence: u64,
        aad: &[u8],
        plaintext: &[u8],
    ) -> Result<EncryptedChunk, AeadError> {
        let (mut message_key, next_chain_key) = self.derive_next(sequence);
        let cipher = ChaCha20Poly1305::new(&message_key.into());
        let nonce = self.nonce(sequence);
        let ciphertext = match cipher.encrypt(
            &nonce.into(),
            Payload {
                msg: plaintext,
                aad,
            },
        ) {
            Ok(ciphertext) => ciphertext,
            Err(_) => {
                message_key.zeroize();
                return Err(AeadError::Encrypt);
            }
        };
        message_key.zeroize();
        self.advance(next_chain_key);
        Ok(EncryptedChunk {
            sequence,
            ciphertext,
        })
    }

    pub fn decrypt(&mut self, aad: &[u8], chunk: &EncryptedChunk) -> Result<Vec<u8>, AeadError> {
        let (mut message_key, next_chain_key) = self.derive_next(chunk.sequence);
        let cipher = ChaCha20Poly1305::new(&message_key.into());
        let nonce = self.nonce(chunk.sequence);
        let plaintext = match cipher.decrypt(
            &nonce.into(),
            Payload {
                msg: &chunk.ciphertext,
                aad,
            },
        ) {
            Ok(plaintext) => plaintext,
            Err(_) => {
                message_key.zeroize();
                return Err(AeadError::Decrypt);
            }
        };
        message_key.zeroize();
        self.advance(next_chain_key);
        Ok(plaintext)
    }

    fn nonce(&self, sequence: u64) -> [u8; 12] {
        let mut nonce = [0u8; 12];
        nonce[..4].copy_from_slice(&self.nonce_prefix);
        nonce[4..].copy_from_slice(&sequence.to_be_bytes());
        nonce
    }

    fn derive_next(&self, sequence: u64) -> ([u8; 32], [u8; 32]) {
        let hk = Hkdf::<Sha512>::new(Some(b"peerline:chunk-ratchet:v1"), &self.chain_key);
        let mut message_key = [0u8; 32];
        let mut next_chain_key = [0u8; 32];
        let sequence = sequence.to_be_bytes();
        hk.expand_multi_info(
            &[
                b"message-key",
                self.nonce_prefix.as_slice(),
                sequence.as_slice(),
            ],
            &mut message_key,
        )
        .expect("fixed HKDF message key length is valid");
        hk.expand_multi_info(
            &[
                b"chain-key",
                self.nonce_prefix.as_slice(),
                sequence.as_slice(),
            ],
            &mut next_chain_key,
        )
        .expect("fixed HKDF chain key length is valid");
        (message_key, next_chain_key)
    }

    fn advance(&mut self, next_chain_key: [u8; 32]) {
        self.chain_key.zeroize();
        self.chain_key = next_chain_key;
    }
}

impl Drop for ChunkAead {
    fn drop(&mut self) {
        self.chain_key.zeroize();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_tampering() {
        let mut aead = ChunkAead::new([7u8; 32], *b"send");
        let mut chunk = aead.encrypt(0, b"aad", b"hello").unwrap();
        chunk.ciphertext[0] ^= 1;
        assert!(aead.decrypt(b"aad", &chunk).is_err());
    }

    #[test]
    fn matching_ratchets_decrypt_in_order() {
        let mut sender = ChunkAead::new([7u8; 32], *b"send");
        let mut receiver = ChunkAead::new([7u8; 32], *b"send");

        let first = sender.encrypt(0, b"aad", b"first").unwrap();
        let second = sender.encrypt(1, b"aad", b"second").unwrap();

        assert_eq!(receiver.decrypt(b"aad", &first).unwrap(), b"first");
        assert_eq!(receiver.decrypt(b"aad", &second).unwrap(), b"second");
    }

    #[test]
    fn ratchet_forgets_previous_message_keys() {
        let mut sender = ChunkAead::new([7u8; 32], *b"send");
        let mut receiver = ChunkAead::new([7u8; 32], *b"send");

        let first = sender.encrypt(0, b"aad", b"first").unwrap();
        let _second = sender.encrypt(1, b"aad", b"second").unwrap();
        assert_eq!(receiver.decrypt(b"aad", &first).unwrap(), b"first");
        let replayed = EncryptedChunk {
            sequence: 0,
            ciphertext: first.ciphertext,
        };
        assert!(receiver.decrypt(b"aad", &replayed).is_err());
    }

    #[test]
    fn failed_decrypt_does_not_advance_ratchet() {
        let mut sender = ChunkAead::new([7u8; 32], *b"send");
        let mut receiver = ChunkAead::new([7u8; 32], *b"send");

        let first = sender.encrypt(0, b"aad", b"first").unwrap();
        let mut tampered = first.clone();
        tampered.ciphertext[0] ^= 1;

        assert!(receiver.decrypt(b"aad", &tampered).is_err());
        assert_eq!(receiver.decrypt(b"aad", &first).unwrap(), b"first");
    }
}
