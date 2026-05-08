use opaque_ke::argon2::Argon2;
use opaque_ke::ciphersuite::CipherSuite;
use opaque_ke::rand::rngs::OsRng;
use opaque_ke::{
    ClientLogin, ClientLoginFinishParameters, ClientRegistration,
    ClientRegistrationFinishParameters, CredentialFinalization, CredentialRequest,
    CredentialResponse, RegistrationRequest, RegistrationResponse, RegistrationUpload, ServerLogin,
    ServerLoginParameters, ServerRegistration, ServerSetup,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zeroize::Zeroizing;

#[derive(Debug, Error)]
pub enum OpaqueError {
    #[error("opaque protocol error: {0}")]
    Protocol(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpaqueServerRecord {
    pub server_setup: Vec<u8>,
    pub password_file: Vec<u8>,
    pub identifier: Vec<u8>,
}

#[derive(Debug)]
pub struct OpaqueClientStart {
    pub request: Vec<u8>,
    state: ClientLogin<PeerlineOpaqueSuite>,
}

#[derive(Debug)]
pub struct OpaqueServerResponse {
    pub response: Vec<u8>,
    state: ServerLogin<PeerlineOpaqueSuite>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpaqueClientFinish {
    pub finalization: Vec<u8>,
    pub session_key: Vec<u8>,
}

pub struct PeerlineOpaqueSuite;

impl CipherSuite for PeerlineOpaqueSuite {
    type OprfCs = opaque_ke::Ristretto255;
    type KeyExchange = opaque_ke::TripleDh<opaque_ke::Ristretto255, sha2::Sha512>;
    type Ksf = Argon2<'static>;
}

pub fn create_server_record(
    password: impl AsRef<[u8]>,
    identifier: impl AsRef<[u8]>,
) -> Result<OpaqueServerRecord, OpaqueError> {
    let password = Zeroizing::new(password.as_ref().to_vec());
    let identifier = identifier.as_ref().to_vec();
    let mut rng = OsRng;
    let server_setup = ServerSetup::<PeerlineOpaqueSuite>::new(&mut rng);

    let client_start = ClientRegistration::<PeerlineOpaqueSuite>::start(&mut rng, &password)
        .map_err(|err| OpaqueError::Protocol(err.to_string()))?;
    let server_start = ServerRegistration::<PeerlineOpaqueSuite>::start(
        &server_setup,
        RegistrationRequest::deserialize(&client_start.message.serialize())
            .map_err(|err| OpaqueError::Protocol(err.to_string()))?,
        &identifier,
    )
    .map_err(|err| OpaqueError::Protocol(err.to_string()))?;
    let client_finish = client_start
        .state
        .finish(
            &mut rng,
            &password,
            RegistrationResponse::deserialize(&server_start.message.serialize())
                .map_err(|err| OpaqueError::Protocol(err.to_string()))?,
            ClientRegistrationFinishParameters::default(),
        )
        .map_err(|err| OpaqueError::Protocol(err.to_string()))?;
    let password_file = ServerRegistration::<PeerlineOpaqueSuite>::finish(
        RegistrationUpload::deserialize(&client_finish.message.serialize())
            .map_err(|err| OpaqueError::Protocol(err.to_string()))?,
    );

    Ok(OpaqueServerRecord {
        server_setup: server_setup.serialize().to_vec(),
        password_file: password_file.serialize().to_vec(),
        identifier,
    })
}

pub fn start_client_login(password: impl AsRef<[u8]>) -> Result<OpaqueClientStart, OpaqueError> {
    let password = Zeroizing::new(password.as_ref().to_vec());
    let mut rng = OsRng;
    let start = ClientLogin::<PeerlineOpaqueSuite>::start(&mut rng, &password)
        .map_err(|err| OpaqueError::Protocol(err.to_string()))?;
    Ok(OpaqueClientStart {
        request: start.message.serialize().to_vec(),
        state: start.state,
    })
}

pub fn start_server_login(
    record: &OpaqueServerRecord,
    request: &[u8],
) -> Result<OpaqueServerResponse, OpaqueError> {
    let mut rng = OsRng;
    let setup = ServerSetup::<PeerlineOpaqueSuite>::deserialize(&record.server_setup)
        .map_err(|err| OpaqueError::Protocol(err.to_string()))?;
    let password_file =
        ServerRegistration::<PeerlineOpaqueSuite>::deserialize(record.password_file.as_slice())
            .map_err(|err| OpaqueError::Protocol(err.to_string()))?;
    let request = CredentialRequest::deserialize(request)
        .map_err(|err| OpaqueError::Protocol(err.to_string()))?;
    let start = ServerLogin::start(
        &mut rng,
        &setup,
        Some(password_file),
        request,
        &record.identifier,
        ServerLoginParameters::default(),
    )
    .map_err(|err| OpaqueError::Protocol(err.to_string()))?;
    Ok(OpaqueServerResponse {
        response: start.message.serialize().to_vec(),
        state: start.state,
    })
}

impl OpaqueClientStart {
    pub fn finish(
        self,
        password: impl AsRef<[u8]>,
        response: &[u8],
    ) -> Result<OpaqueClientFinish, OpaqueError> {
        let password = Zeroizing::new(password.as_ref().to_vec());
        let mut rng = OsRng;
        let response = CredentialResponse::deserialize(response)
            .map_err(|err| OpaqueError::Protocol(err.to_string()))?;
        let finish = self
            .state
            .finish(
                &mut rng,
                &password,
                response,
                ClientLoginFinishParameters::default(),
            )
            .map_err(|err| OpaqueError::Protocol(err.to_string()))?;
        Ok(OpaqueClientFinish {
            finalization: finish.message.serialize().to_vec(),
            session_key: finish.session_key.to_vec(),
        })
    }
}

impl OpaqueServerResponse {
    pub fn finish(self, finalization: &[u8]) -> Result<Vec<u8>, OpaqueError> {
        let finalization = CredentialFinalization::deserialize(finalization)
            .map_err(|err| OpaqueError::Protocol(err.to_string()))?;
        let finish = self
            .state
            .finish(finalization, ServerLoginParameters::default())
            .map_err(|err| OpaqueError::Protocol(err.to_string()))?;
        Ok(finish.session_key.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opaque_login_matches_for_correct_code() {
        let record = create_server_record(b"rose-lime-iris-jade-1234", b"alice").unwrap();
        let client = start_client_login(b"rose-lime-iris-jade-1234").unwrap();
        let server = start_server_login(&record, &client.request).unwrap();
        let client_finish = client
            .finish(b"rose-lime-iris-jade-1234", &server.response)
            .unwrap();
        let server_key = server.finish(&client_finish.finalization).unwrap();
        assert_eq!(client_finish.session_key, server_key);
    }

    #[test]
    fn opaque_login_rejects_wrong_code() {
        let record = create_server_record(b"rose-lime-iris-jade-1234", b"alice").unwrap();
        let client = start_client_login(b"wrong-code").unwrap();
        let server = start_server_login(&record, &client.request).unwrap();
        assert!(client.finish(b"wrong-code", &server.response).is_err());
    }
}
