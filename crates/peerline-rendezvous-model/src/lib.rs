use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

pub const HEADER_TIMESTAMP: &str = "x-peerline-timestamp";
pub const HEADER_SIGNATURE: &str = "x-peerline-signature";
pub const HEADER_VERSION: &str = "x-peerline-version";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerDescriptor {
    pub protocol_version: u16,
    pub peer_id: String,
    pub direct_endpoints: Vec<String>,
    pub libp2p_endpoints: Vec<String>,
    pub published_unix_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RendezvousRecord {
    pub sequence: u64,
    pub expires_unix_ms: u64,
    pub descriptor: PeerDescriptor,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RendezvousRegisterRequest {
    pub ttl_seconds: u32,
    pub descriptor: PeerDescriptor,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RendezvousRegisterResponse {
    pub cookie: u64,
    pub record: RendezvousRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RendezvousUnregisterResponse {
    pub removed: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RendezvousDiscoverRequest {
    pub after_cookie: Option<u64>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RendezvousDiscoverResponse {
    pub cookie: u64,
    pub records: Vec<RendezvousRecord>,
}

pub fn request_path_and_query(path: &str, query: Option<&str>) -> String {
    match query {
        Some(query) if !query.is_empty() => format!("{path}?{query}"),
        _ => path.to_string(),
    }
}

pub fn sign_peerline_request(
    token: &str,
    timestamp_ms: i64,
    method: &str,
    path_and_query: &str,
    body: &[u8],
) -> String {
    let key = blake3::hash(token.as_bytes());
    let mut hasher = blake3::Hasher::new_keyed(key.as_bytes());
    hasher.update(timestamp_ms.to_string().as_bytes());
    hasher.update(b"\n");
    hasher.update(method.as_bytes());
    hasher.update(b"\n");
    hasher.update(path_and_query.as_bytes());
    hasher.update(b"\n");
    hasher.update(body);
    URL_SAFE_NO_PAD.encode(hasher.finalize().as_bytes())
}

pub fn verify_peerline_request_signature(
    token: &str,
    timestamp_ms: i64,
    method: &str,
    path_and_query: &str,
    body: &[u8],
    signature: &str,
) -> bool {
    let expected = sign_peerline_request(token, timestamp_ms, method, path_and_query, body);
    constant_time_eq(expected.as_bytes(), signature.as_bytes())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    left.iter()
        .zip(right)
        .fold(0u8, |diff, (left, right)| diff | (left ^ right))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peerline_request_signatures_roundtrip() {
        let signature = sign_peerline_request(
            "secret",
            1_778_342_400_000,
            "POST",
            "/v1/namespaces/abc/registrations?limit=10",
            br#"{"ok":true}"#,
        );

        assert!(verify_peerline_request_signature(
            "secret",
            1_778_342_400_000,
            "POST",
            "/v1/namespaces/abc/registrations?limit=10",
            br#"{"ok":true}"#,
            &signature,
        ));
        assert!(!verify_peerline_request_signature(
            "secret",
            1_778_342_400_000,
            "POST",
            "/v1/namespaces/abc/registrations?limit=10",
            br#"{"ok":false}"#,
            &signature,
        ));
    }
}
