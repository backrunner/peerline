use peerline_core::{HumanCode, HumanName, NameCode};
use peerline_rendezvous_model::{
    HEADER_SIGNATURE, HEADER_TIMESTAMP, HEADER_VERSION, PeerDescriptor, RendezvousDiscoverResponse,
    RendezvousRegisterRequest, request_path_and_query, sign_peerline_request,
};
use reqwest::Url;
use std::{
    fmt, fs,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub const DEFAULT_RENDEZVOUS_ENDPOINT: &str = "https://peerline.pwp.sh";
const DEFAULT_RENDEZVOUS_TTL_SECS: u32 = 120;
const DEFAULT_RENDEZVOUS_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, PartialEq, Eq)]
pub struct RendezvousConfig {
    pub endpoints: Vec<Url>,
    pub auth_token: Option<String>,
    pub client_identity: Option<RendezvousClientIdentity>,
    pub request_timeout: Duration,
    pub registration_ttl: Duration,
}

#[derive(Clone, PartialEq, Eq)]
pub enum RendezvousClientIdentity {
    PemBundle(Vec<u8>),
    PemPath(PathBuf),
}

impl fmt::Debug for RendezvousClientIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PemBundle(_) => f.write_str("PemBundle(<redacted>)"),
            Self::PemPath(path) => f.debug_tuple("PemPath").field(path).finish(),
        }
    }
}

impl RendezvousConfig {
    pub fn disabled() -> Self {
        Self {
            endpoints: Vec::new(),
            auth_token: None,
            client_identity: None,
            request_timeout: DEFAULT_RENDEZVOUS_TIMEOUT,
            registration_ttl: Duration::from_secs(DEFAULT_RENDEZVOUS_TTL_SECS.into()),
        }
    }
}

impl Default for RendezvousConfig {
    fn default() -> Self {
        let endpoints = rendezvous_endpoints_from_env().unwrap_or_else(|| {
            vec![Url::parse(DEFAULT_RENDEZVOUS_ENDPOINT).expect("default rendezvous URL is valid")]
        });
        let client_identity = client_identity_from_env();
        if endpoints.iter().any(is_default_private_endpoint) && client_identity.is_none() {
            tracing::warn!(
                "default private rendezvous endpoint requires PEERLINE_RENDEZVOUS_CLIENT_IDENTITY_PEM or PEERLINE_RENDEZVOUS_CLIENT_IDENTITY_PATH"
            );
        }

        Self {
            endpoints,
            auth_token: auth_token_from_env(),
            client_identity,
            request_timeout: env_duration_ms("PEERLINE_RENDEZVOUS_TIMEOUT_MS")
                .unwrap_or(DEFAULT_RENDEZVOUS_TIMEOUT),
            registration_ttl: env_duration_secs("PEERLINE_RENDEZVOUS_TTL_SECS")
                .unwrap_or_else(|| Duration::from_secs(DEFAULT_RENDEZVOUS_TTL_SECS.into())),
        }
    }
}

impl fmt::Debug for RendezvousConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RendezvousConfig")
            .field("endpoints", &self.endpoints)
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "client_identity",
                &self.client_identity.as_ref().map(|_| "<redacted>"),
            )
            .field("request_timeout", &self.request_timeout)
            .field("registration_ttl", &self.registration_ttl)
            .finish()
    }
}

pub fn rendezvous_namespace(name: &HumanName, code: &HumanCode) -> String {
    NameCode::new(name.clone(), code.clone()).lookup_key().hex()
}

pub async fn publish_peer_descriptor(
    name: &HumanName,
    code: &HumanCode,
    descriptor: &PeerDescriptor,
    config: &RendezvousConfig,
) -> anyhow::Result<()> {
    let usable_endpoints = usable_endpoints(config);
    if usable_endpoints.is_empty() {
        if config.endpoints.iter().any(is_default_private_endpoint)
            && config.client_identity.is_none()
        {
            tracing::debug!(
                "skipping private rendezvous endpoint because no client identity is configured"
            );
        }
        return Ok(());
    }

    let namespace = rendezvous_namespace(name, code);
    let request = RendezvousRegisterRequest {
        ttl_seconds: config
            .registration_ttl
            .as_secs()
            .clamp(1, u64::from(u32::MAX)) as u32,
        descriptor: descriptor.clone(),
    };
    let body = serde_json::to_vec(&request)?;
    let client = client(config)?;
    let mut published = 0usize;

    for endpoint in usable_endpoints {
        let url = namespace_url(endpoint, &namespace)?;
        match signed_request(&client, config, "POST", url, &body)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                published += 1;
            }
            Ok(response) => {
                tracing::debug!(
                    status = %response.status(),
                    endpoint = %endpoint,
                    "rendezvous publish rejected"
                );
            }
            Err(error) => {
                tracing::debug!(%error, endpoint = %endpoint, "rendezvous publish failed");
            }
        }
    }

    if published == 0 {
        tracing::warn!(namespace = %namespace, "no rendezvous endpoint accepted the registration");
    }

    Ok(())
}

pub async fn discover_peer_descriptors(
    name: &HumanName,
    code: &HumanCode,
    config: &RendezvousConfig,
) -> anyhow::Result<Vec<PeerDescriptor>> {
    let usable_endpoints = usable_endpoints(config);
    if usable_endpoints.is_empty() {
        if config.endpoints.iter().any(is_default_private_endpoint)
            && config.client_identity.is_none()
        {
            tracing::debug!(
                "skipping private rendezvous endpoint because no client identity is configured"
            );
        }
        return Ok(Vec::new());
    }

    let namespace = rendezvous_namespace(name, code);
    let client = client(config)?;
    let mut descriptors = Vec::new();

    for endpoint in usable_endpoints {
        let url = namespace_url(endpoint, &namespace)?;
        let response = match signed_request(&client, config, "GET", url, &[])
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => response,
            Ok(response) => {
                tracing::debug!(
                    status = %response.status(),
                    endpoint = %endpoint,
                    "rendezvous discovery rejected"
                );
                continue;
            }
            Err(error) => {
                tracing::debug!(%error, endpoint = %endpoint, "rendezvous discovery failed");
                continue;
            }
        };

        let payload = match response.json::<RendezvousDiscoverResponse>().await {
            Ok(payload) => payload,
            Err(error) => {
                tracing::debug!(%error, endpoint = %endpoint, "invalid rendezvous discovery payload");
                continue;
            }
        };

        descriptors.extend(payload.records.into_iter().map(|record| record.descriptor));
    }

    descriptors.sort_by_key(|descriptor| {
        (
            descriptor.published_unix_ms,
            descriptor.libp2p_endpoints.len(),
            descriptor.peer_id.clone(),
        )
    });
    descriptors.reverse();
    descriptors.dedup_by(|left, right| left.peer_id == right.peer_id);
    Ok(descriptors)
}

fn client(config: &RendezvousConfig) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .user_agent(format!("peerline/{}", env!("CARGO_PKG_VERSION")))
        .timeout(config.request_timeout);
    if let Some(identity) = &config.client_identity {
        builder = builder.identity(identity.load()?);
    }
    builder.build().map_err(Into::into)
}

fn namespace_url(base: &Url, namespace: &str) -> anyhow::Result<Url> {
    base.join(&format!("/v1/namespaces/{namespace}/registrations"))
        .map_err(Into::into)
}

fn signed_request(
    client: &reqwest::Client,
    config: &RendezvousConfig,
    method: &str,
    url: Url,
    body: &[u8],
) -> reqwest::RequestBuilder {
    let mut builder = match method {
        "POST" => client
            .post(url.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::ACCEPT, "application/json")
            .body(body.to_vec()),
        "GET" => client
            .get(url.clone())
            .header(reqwest::header::ACCEPT, "application/json"),
        "DELETE" => client.delete(url.clone()),
        _ => client.request(method.parse().expect("valid method"), url.clone()),
    };

    builder = builder.header(HEADER_VERSION, env!("CARGO_PKG_VERSION"));

    if let Some(token) = auth_token_for_request(config, &url) {
        let timestamp = now_unix_ms();
        let signature = sign_peerline_request(token, timestamp, method, &request_path(&url), body);
        builder = builder
            .header(HEADER_TIMESTAMP, timestamp.to_string())
            .header(HEADER_SIGNATURE, signature);
    }

    builder
}

fn request_path(url: &Url) -> String {
    request_path_and_query(url.path(), url.query())
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn rendezvous_endpoints_from_env() -> Option<Vec<Url>> {
    let raw = std::env::var("PEERLINE_RENDEZVOUS_URLS")
        .ok()
        .or_else(|| std::env::var("PEERLINE_RENDEZVOUS_URL").ok())?;
    let endpoints = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter_map(|value| match Url::parse(value) {
            Ok(url) => Some(url),
            Err(error) => {
                tracing::warn!(%error, value, "ignoring invalid rendezvous URL");
                None
            }
        })
        .collect::<Vec<_>>();
    if endpoints.is_empty() {
        tracing::warn!(value = %raw, "rendezvous URL configuration produced no valid endpoints");
        None
    } else {
        tracing::debug!(
            count = endpoints.len(),
            "using configured rendezvous endpoints"
        );
        Some(endpoints)
    }
}

fn auth_token_from_env() -> Option<String> {
    std::env::var("PEERLINE_RENDEZVOUS_TOKEN")
        .ok()
        .filter(|token| !token.trim().is_empty())
}

fn auth_token_for_request<'a>(config: &'a RendezvousConfig, _url: &Url) -> Option<&'a str> {
    config.auth_token.as_deref()
}

fn client_identity_from_env() -> Option<RendezvousClientIdentity> {
    if let Ok(raw) = std::env::var("PEERLINE_RENDEZVOUS_CLIENT_IDENTITY_PEM") {
        let pem = raw.trim();
        if !pem.is_empty() {
            return Some(RendezvousClientIdentity::PemBundle(pem.as_bytes().to_vec()));
        }
    }
    if let Ok(raw) = std::env::var("PEERLINE_RENDEZVOUS_CLIENT_IDENTITY_PATH") {
        let path = PathBuf::from(raw.trim());
        if !path.as_os_str().is_empty() {
            return Some(RendezvousClientIdentity::PemPath(path));
        }
    }
    None
}

fn is_default_private_endpoint(url: &Url) -> bool {
    url.scheme() == "https"
        && url.host_str() == Some("peerline.pwp.sh")
        && url.port_or_known_default() == Some(443)
}

fn usable_endpoints(config: &RendezvousConfig) -> Vec<&Url> {
    config
        .endpoints
        .iter()
        .filter(|endpoint| {
            !is_default_private_endpoint(endpoint) || config.client_identity.is_some()
        })
        .collect()
}

impl RendezvousClientIdentity {
    fn load(&self) -> anyhow::Result<reqwest::Identity> {
        let pem = match self {
            Self::PemBundle(bytes) => bytes.clone(),
            Self::PemPath(path) => fs::read(path).map_err(|error| {
                anyhow::anyhow!("could not read rendezvous client identity: {error}")
            })?,
        };
        reqwest::Identity::from_pem(&pem)
            .map_err(|error| anyhow::anyhow!("invalid rendezvous client identity PEM: {error}"))
    }
}

fn env_duration_ms(name: &str) -> Option<Duration> {
    let raw = std::env::var(name).ok()?;
    let millis = raw.parse::<u64>().ok()?;
    Some(Duration::from_millis(millis))
}

fn env_duration_secs(name: &str) -> Option<Duration> {
    let raw = std::env::var(name).ok()?;
    let seconds = raw.parse::<u64>().ok()?;
    Some(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_private_endpoint_is_identified_by_origin() {
        let url = Url::parse("https://peerline.pwp.sh/v1/namespaces/abc/registrations")
            .expect("valid rendezvous URL");

        assert!(is_default_private_endpoint(&url));
        assert!(!is_default_private_endpoint(
            &Url::parse("https://example.com/v1/namespaces/abc/registrations")
                .expect("valid rendezvous URL")
        ));
    }

    #[test]
    fn default_private_endpoint_is_skipped_without_client_identity() {
        let private = Url::parse(DEFAULT_RENDEZVOUS_ENDPOINT).expect("valid rendezvous URL");
        let public = Url::parse("https://example.com").expect("valid rendezvous URL");
        let config = RendezvousConfig {
            endpoints: vec![private, public.clone()],
            auth_token: None,
            client_identity: None,
            request_timeout: DEFAULT_RENDEZVOUS_TIMEOUT,
            registration_ttl: Duration::from_secs(DEFAULT_RENDEZVOUS_TTL_SECS.into()),
        };

        assert_eq!(usable_endpoints(&config), vec![&public]);
    }
}
