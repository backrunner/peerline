use crate::transcript::Transcript;
use hkdf::Hkdf;
use ml_kem::{
    EncapsulationKey768, KeyExport, MlKem768, TryKeyInit,
    kem::{Decapsulate, Encapsulate, Kem},
};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use sha2::Sha512;
use x25519_dalek::{PublicKey, StaticSecret};
use zeroize::{Zeroize, ZeroizeOnDrop};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandshakeRole {
    Sender,
    Receiver,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientHello {
    pub x25519_public: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerHello {
    pub x25519_public: [u8; 32],
    pub ml_kem_public: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientKem {
    pub ml_kem_ciphertext: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Zeroize, ZeroizeOnDrop)]
pub struct SessionKeys {
    pub send_key: [u8; 32],
    pub recv_key: [u8; 32],
}

pub struct ClientHandshake {
    secret: StaticSecret,
    pub hello: ClientHello,
}

pub struct ServerHandshake {
    secret: StaticSecret,
    ml_kem_decapsulation: <MlKem768 as Kem>::DecapsulationKey,
    pub hello: ServerHello,
}

impl ClientHandshake {
    pub fn start() -> Self {
        let secret = StaticSecret::random_from_rng(OsRng);
        let public = PublicKey::from(&secret);
        Self {
            secret,
            hello: ClientHello {
                x25519_public: public.to_bytes(),
            },
        }
    }

    pub fn finish(
        self,
        server: &ServerHello,
        opaque_key: &[u8],
        transcript: &Transcript,
    ) -> anyhow::Result<(ClientKem, SessionKeys)> {
        let server_public = PublicKey::from(server.x25519_public);
        let x_shared = self.secret.diffie_hellman(&server_public);
        let ml_public = EncapsulationKey768::new_from_slice(&server.ml_kem_public)?;
        let (ciphertext, ml_shared) = ml_public.encapsulate();
        let keys = derive_session_keys(
            HandshakeRole::Sender,
            opaque_key,
            x_shared.as_bytes(),
            ml_shared.as_ref(),
            transcript,
        );
        Ok((
            ClientKem {
                ml_kem_ciphertext: ciphertext.as_slice().to_vec(),
            },
            keys,
        ))
    }
}

impl ServerHandshake {
    pub fn start(client: &ClientHello) -> anyhow::Result<Self> {
        let _client_public = PublicKey::from(client.x25519_public);
        let secret = StaticSecret::random_from_rng(OsRng);
        let public = PublicKey::from(&secret);
        let (dk, ek) = MlKem768::generate_keypair();
        let ek_bytes = ek.to_bytes();
        Ok(Self {
            secret,
            ml_kem_decapsulation: dk,
            hello: ServerHello {
                x25519_public: public.to_bytes(),
                ml_kem_public: ek_bytes.as_slice().to_vec(),
            },
        })
    }

    pub fn finish(
        self,
        client: &ClientHello,
        client_kem: &ClientKem,
        opaque_key: &[u8],
        transcript: &Transcript,
    ) -> anyhow::Result<SessionKeys> {
        let client_public = PublicKey::from(client.x25519_public);
        let x_shared = self.secret.diffie_hellman(&client_public);
        let ciphertext: ml_kem::Ciphertext<MlKem768> =
            client_kem.ml_kem_ciphertext.as_slice().try_into()?;
        let ml_shared = self.ml_kem_decapsulation.decapsulate(&ciphertext);
        Ok(derive_session_keys(
            HandshakeRole::Receiver,
            opaque_key,
            x_shared.as_bytes(),
            ml_shared.as_ref(),
            transcript,
        ))
    }
}

fn derive_session_keys(
    role: HandshakeRole,
    opaque_key: &[u8],
    x25519_shared: &[u8],
    ml_kem_shared: &[u8],
    transcript: &Transcript,
) -> SessionKeys {
    let mut input =
        Vec::with_capacity(opaque_key.len() + x25519_shared.len() + ml_kem_shared.len() + 32);
    input.extend_from_slice(opaque_key);
    input.extend_from_slice(x25519_shared);
    input.extend_from_slice(ml_kem_shared);
    input.extend_from_slice(&transcript.hash());

    let hk = Hkdf::<Sha512>::new(Some(b"peerline:session:v1"), &input);
    let mut sender = [0u8; 32];
    let mut receiver = [0u8; 32];
    hk.expand(b"sender-to-receiver", &mut sender)
        .expect("fixed HKDF output length is valid");
    hk.expand(b"receiver-to-sender", &mut receiver)
        .expect("fixed HKDF output length is valid");

    match role {
        HandshakeRole::Sender => SessionKeys {
            send_key: sender,
            recv_key: receiver,
        },
        HandshakeRole::Receiver => SessionKeys {
            send_key: receiver,
            recv_key: sender,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hybrid_handshake_matches_keys() {
        let transcript = Transcript::new("peerline-test");
        let client = ClientHandshake::start();
        let client_hello = client.hello.clone();
        let server = ServerHandshake::start(&client_hello).unwrap();
        let server_hello = server.hello.clone();
        let (kem, client_keys) = client
            .finish(&server_hello, b"opaque-session-key", &transcript)
            .unwrap();
        let server_keys = server
            .finish(&client_hello, &kem, b"opaque-session-key", &transcript)
            .unwrap();
        assert_eq!(client_keys.send_key, server_keys.recv_key);
        assert_eq!(client_keys.recv_key, server_keys.send_key);
    }
}
