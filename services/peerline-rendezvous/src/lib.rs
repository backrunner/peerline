use peerline_rendezvous_model::{
    HEADER_SIGNATURE, HEADER_TIMESTAMP, HEADER_VERSION, PeerDescriptor, RendezvousDiscoverRequest,
    RendezvousDiscoverResponse, RendezvousRecord, RendezvousRegisterRequest,
    RendezvousRegisterResponse, request_path_and_query, verify_peerline_request_signature,
};
use serde::Deserialize;
use std::collections::HashSet;
use worker::{
    Context, Date, DurableObject, Env, Method, Request, Response, Result, SqlStorage,
    SqlStorageValue, State, TlsClientAuth, durable_object, event,
};

const DO_BINDING: &str = "RENDEZVOUS";
const RATE_LIMIT_BINDING: &str = "RATE_LIMITER";
const AUTH_TOKEN_BINDING: &str = "PEERLINE_RENDEZVOUS_TOKEN";
const REQUIRE_MTLS_BINDING: &str = "PEERLINE_RENDEZVOUS_REQUIRE_MTLS";
const CLIENT_CERT_FINGERPRINTS_BINDING: &str =
    "PEERLINE_RENDEZVOUS_ALLOWED_CLIENT_CERT_FINGERPRINTS";

const DESCRIPTOR_PROTOCOL_VERSION: u16 = 1;
const DEFAULT_MAX_TTL_SECS: u32 = 180;
const DEFAULT_DISCOVER_LIMIT: u32 = 32;
const MAX_DISCOVER_LIMIT: u32 = 128;
const MAX_REQUEST_BYTES: usize = 32 * 1024;
const MAX_DIRECT_ENDPOINTS: usize = 8;
const MAX_LIBP2P_ENDPOINTS: usize = 24;
const MAX_ENDPOINT_LEN: usize = 256;
const RATE_WINDOW_MS: i64 = 60_000;
const REGISTER_RATE_LIMIT: u32 = 30;
const DISCOVER_RATE_LIMIT: u32 = 120;
const SIGNATURE_SKEW_MS: i64 = 5 * 60 * 1000;

#[event(fetch, respond_with_errors)]
pub async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    console_error_panic_hook::set_once();

    if matches!(req.method(), Method::Get) && req.path() == "/healthz" {
        return Response::ok("ok");
    }

    let path = req.path();
    let Some(namespace) = namespace_from_path(&path) else {
        return Response::error("not found", 404);
    };
    if !is_valid_namespace(namespace) {
        return Response::error("invalid namespace", 400);
    }
    if !matches!(req.method(), Method::Get | Method::Post) {
        return Response::error("method not allowed", 405);
    }

    if let Ok(limiter) = env.rate_limiter(RATE_LIMIT_BINDING) {
        let ip = client_ip(&req)?;
        let key = format!("peerline:rendezvous:{namespace}:{}:{ip}", req.method());
        if !limiter.limit(key).await?.success {
            return Response::error("rate limit exceeded", 429);
        }
    }

    let stub = env.durable_object(DO_BINDING)?.get_by_name(namespace)?;
    stub.fetch_with_request(req).await
}

#[durable_object(fetch)]
pub struct RendezvousShard {
    sql: SqlStorage,
    auth_token: Option<String>,
    require_mtls: bool,
    allowed_client_cert_fingerprints: Option<HashSet<String>>,
    max_ttl_secs: u32,
}

impl DurableObject for RendezvousShard {
    fn new(state: State, env: Env) -> Self {
        let sql = state.storage().sql();
        install_schema(&sql);
        let allowed_client_cert_fingerprints =
            env_fingerprint_set(&env, CLIENT_CERT_FINGERPRINTS_BINDING);
        let require_mtls = env_bool(&env, REQUIRE_MTLS_BINDING)
            .unwrap_or_else(|| allowed_client_cert_fingerprints.is_some());

        Self {
            sql,
            auth_token: env_string(&env, AUTH_TOKEN_BINDING),
            require_mtls,
            allowed_client_cert_fingerprints,
            max_ttl_secs: env_u32(&env, "PEERLINE_RENDEZVOUS_MAX_TTL_SECS")
                .unwrap_or(DEFAULT_MAX_TTL_SECS),
        }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        let path = req.path();
        let Some(namespace) = namespace_from_path(&path) else {
            return Response::error("not found", 404);
        };
        if !is_valid_namespace(namespace) {
            return Response::error("invalid namespace", 400);
        }

        match req.method() {
            Method::Post => self.register(namespace, &mut req).await,
            Method::Get => self.discover(namespace, &mut req).await,
            _ => Response::error("method not allowed", 405),
        }
    }
}

impl RendezvousShard {
    async fn register(&self, namespace: &str, req: &mut Request) -> Result<Response> {
        if !is_json_request(req)? {
            return Response::error("content type must be application/json", 415);
        }

        let body = req.bytes().await?;
        if body.len() > MAX_REQUEST_BYTES {
            return Response::error("payload too large", 413);
        }
        if let Err(response) = self.verify_request(req, &body) {
            return Ok(response);
        }
        if !self.enforce_rate_limit(namespace, req, REGISTER_RATE_LIMIT)? {
            return Response::error("rate limit exceeded", 429);
        }

        let request = match serde_json::from_slice::<RendezvousRegisterRequest>(&body) {
            Ok(request) => request,
            Err(_) => return Response::error("invalid registration payload", 400),
        };
        if let Err(response) = validate_descriptor(&request.descriptor) {
            return Ok(response);
        }

        let now = now_unix_ms();
        let ttl_seconds = request.ttl_seconds.clamp(1, self.max_ttl_secs);
        let expires_unix_ms = now + i64::from(ttl_seconds) * 1000;
        self.cleanup_expired(now)?;

        let mut descriptor = request.descriptor;
        descriptor.published_unix_ms = now as u64;
        let peer_id = descriptor.peer_id.clone();
        let descriptor_json = serde_json::to_string(&descriptor)?;
        self.sql.exec(
            "DELETE FROM registrations WHERE peer_id = ?;",
            vec![peer_id.clone().into()],
        )?;
        let row: CookieRow = self
            .sql
            .exec(
                "INSERT INTO registrations(peer_id, expires_unix_ms, descriptor_json)
                 VALUES (?, ?, ?)
                 RETURNING cookie;",
                vec![
                    peer_id.into(),
                    expires_unix_ms.into(),
                    descriptor_json.into(),
                ],
            )?
            .one()?;
        let record = RendezvousRecord {
            sequence: row.cookie as u64,
            expires_unix_ms: expires_unix_ms as u64,
            descriptor,
        };

        Ok(Response::from_json(&RendezvousRegisterResponse {
            cookie: row.cookie as u64,
            record,
        })?
        .with_status(201))
    }

    async fn discover(&self, namespace: &str, req: &mut Request) -> Result<Response> {
        if let Err(response) = self.verify_request(req, &[]) {
            return Ok(response);
        }
        if !self.enforce_rate_limit(namespace, req, DISCOVER_RATE_LIMIT)? {
            return Response::error("rate limit exceeded", 429);
        }

        let query = req
            .query::<RendezvousDiscoverRequest>()
            .unwrap_or(RendezvousDiscoverRequest {
                after_cookie: None,
                limit: None,
            });
        let after_cookie = query.after_cookie.unwrap_or_default();
        let limit = query
            .limit
            .unwrap_or(DEFAULT_DISCOVER_LIMIT)
            .clamp(1, MAX_DISCOVER_LIMIT);

        let now = now_unix_ms();
        self.cleanup_expired(now)?;

        let rows: Vec<RegistrationRow> = self
            .sql
            .exec(
                "SELECT cookie, expires_unix_ms, descriptor_json
                 FROM registrations
                 WHERE expires_unix_ms > ? AND cookie > ?
                 ORDER BY cookie ASC
                 LIMIT ?;",
                vec![
                    now.into(),
                    (after_cookie as i64).into(),
                    i64::from(limit).into(),
                ],
            )?
            .to_array()?;
        let records = rows
            .into_iter()
            .filter_map(|row| {
                serde_json::from_str::<PeerDescriptor>(&row.descriptor_json)
                    .ok()
                    .map(|descriptor| RendezvousRecord {
                        sequence: row.cookie as u64,
                        expires_unix_ms: row.expires_unix_ms as u64,
                        descriptor,
                    })
            })
            .collect::<Vec<_>>();
        let cookie: CookieRow = self
            .sql
            .exec(
                "SELECT COALESCE(MAX(cookie), 0) AS cookie
                 FROM registrations
                 WHERE expires_unix_ms > ?;",
                vec![now.into()],
            )?
            .one()?;

        Response::from_json(&RendezvousDiscoverResponse {
            cookie: cookie.cookie as u64,
            records,
        })
    }

    fn verify_request(&self, req: &Request, body: &[u8]) -> std::result::Result<(), Response> {
        self.verify_peerline_version(req)?;
        if let Some(auth) = req.cf().and_then(|cf| cf.tls_client_auth()) {
            return self.verify_tls_client_auth(&auth);
        }
        if self.require_mtls {
            return Err(error_response("client certificate required", 401));
        }
        self.verify_hmac_request(req, body)
    }

    fn verify_peerline_version(&self, req: &Request) -> std::result::Result<(), Response> {
        if req
            .headers()
            .get(HEADER_VERSION)
            .map_err(|_| error_response("invalid version header", 400))?
            .filter(|version| !version.trim().is_empty())
            .is_none()
        {
            return Err(error_response("missing peerline version", 401));
        }
        Ok(())
    }

    fn verify_tls_client_auth(
        &self,
        auth: &TlsClientAuth,
    ) -> std::result::Result<(), Response> {
        if auth.cert_presented() != "1" {
            return Err(error_response("client certificate required", 401));
        }
        if !auth.cert_verified().eq_ignore_ascii_case("SUCCESS") {
            return Err(error_response("invalid client certificate", 401));
        }
        if let Some(allowed) = &self.allowed_client_cert_fingerprints {
            let fingerprint = normalize_cert_fingerprint(&auth.cert_fingerprint_sha256());
            if !allowed.contains(&fingerprint) {
                return Err(error_response("unauthorized client certificate", 403));
            }
        }
        Ok(())
    }

    fn verify_hmac_request(
        &self,
        req: &Request,
        body: &[u8],
    ) -> std::result::Result<(), Response> {
        let Some(token) = self.auth_token.as_deref() else {
            return Err(error_response(
                "rendezvous auth secret is not configured",
                500,
            ));
        };

        let timestamp = match req
            .headers()
            .get(HEADER_TIMESTAMP)
            .map_err(|_| error_response("invalid timestamp header", 400))?
            .and_then(|value| value.parse::<i64>().ok())
        {
            Some(timestamp) => timestamp,
            None => return Err(error_response("missing request timestamp", 401)),
        };
        if now_unix_ms().abs_diff(timestamp) > SIGNATURE_SKEW_MS as u64 {
            return Err(error_response("stale request timestamp", 401));
        }

        let signature = match req
            .headers()
            .get(HEADER_SIGNATURE)
            .map_err(|_| error_response("invalid signature header", 400))?
        {
            Some(signature) => signature,
            None => return Err(error_response("missing request signature", 401)),
        };

        let url = match req.url() {
            Ok(url) => url,
            Err(_) => return Err(error_response("invalid request URL", 400)),
        };
        let path = request_path_and_query(url.path(), url.query());
        let method = req.method().to_string();
        if !verify_peerline_request_signature(token, timestamp, &method, &path, body, &signature) {
            return Err(error_response("invalid request signature", 401));
        }

        Ok(())
    }

    fn enforce_rate_limit(&self, namespace: &str, req: &Request, limit: u32) -> Result<bool> {
        let ip = client_ip(req)?;
        let key = format!("{namespace}:{}:{ip}", req.method());
        let now = now_unix_ms();
        let window_start = now - now.rem_euclid(RATE_WINDOW_MS);
        let limit_plus_one = limit.saturating_add(1);
        let sql = format!(
            "INSERT INTO rate_limits(key, window_start_ms, count)
             VALUES (?, ?, 1)
             ON CONFLICT(key) DO UPDATE SET
                window_start_ms = CASE
                    WHEN rate_limits.window_start_ms <> excluded.window_start_ms
                    THEN excluded.window_start_ms
                    ELSE rate_limits.window_start_ms
                END,
                count = CASE
                    WHEN rate_limits.window_start_ms <> excluded.window_start_ms THEN 1
                    WHEN rate_limits.count + 1 > {limit_plus_one} THEN {limit_plus_one}
                    ELSE rate_limits.count + 1
                END
             RETURNING count;"
        );
        let row: CountRow = self
            .sql
            .exec(&sql, vec![key.into(), window_start.into()])?
            .one()?;
        Ok(row.count <= i64::from(limit))
    }

    fn cleanup_expired(&self, now_unix_ms: i64) -> Result<()> {
        self.sql.exec(
            "DELETE FROM registrations WHERE expires_unix_ms <= ?;",
            vec![now_unix_ms.into()],
        )?;
        self.sql.exec(
            "DELETE FROM rate_limits WHERE window_start_ms < ?;",
            vec![(now_unix_ms - RATE_WINDOW_MS * 2).into()],
        )?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct CookieRow {
    cookie: i64,
}

#[derive(Debug, Deserialize)]
struct CountRow {
    count: i64,
}

#[derive(Debug, Deserialize)]
struct RegistrationRow {
    cookie: i64,
    expires_unix_ms: i64,
    descriptor_json: String,
}

fn install_schema(sql: &SqlStorage) {
    sql.exec(
        "CREATE TABLE IF NOT EXISTS registrations(
            cookie INTEGER PRIMARY KEY AUTOINCREMENT,
            peer_id TEXT NOT NULL,
            expires_unix_ms INTEGER NOT NULL,
            descriptor_json TEXT NOT NULL
        );",
        Vec::<SqlStorageValue>::new(),
    )
    .expect("install rendezvous registrations table");
    sql.exec(
        "CREATE INDEX IF NOT EXISTS registrations_expires_idx
            ON registrations(expires_unix_ms);",
        Vec::<SqlStorageValue>::new(),
    )
    .expect("install rendezvous registrations expiry index");
    sql.exec(
        "CREATE INDEX IF NOT EXISTS registrations_peer_id_idx
            ON registrations(peer_id);",
        Vec::<SqlStorageValue>::new(),
    )
    .expect("install rendezvous registrations peer index");
    sql.exec(
        "CREATE TABLE IF NOT EXISTS rate_limits(
            key TEXT PRIMARY KEY,
            window_start_ms INTEGER NOT NULL,
            count INTEGER NOT NULL
        );",
        Vec::<SqlStorageValue>::new(),
    )
    .expect("install rendezvous rate limit table");
}

fn error_response(message: &str, status: u16) -> Response {
    Response::error(message, status).expect("valid error response")
}

fn validate_descriptor(descriptor: &PeerDescriptor) -> std::result::Result<(), Response> {
    if descriptor.protocol_version != DESCRIPTOR_PROTOCOL_VERSION {
        return Err(error_response(
            "unsupported descriptor protocol version",
            400,
        ));
    }
    if descriptor.peer_id.is_empty() || descriptor.peer_id.len() > 128 {
        return Err(error_response("invalid peer id", 400));
    }
    if descriptor.direct_endpoints.is_empty() && descriptor.libp2p_endpoints.is_empty() {
        return Err(error_response("no advertised endpoints", 400));
    }
    if descriptor.direct_endpoints.len() > MAX_DIRECT_ENDPOINTS
        || descriptor.libp2p_endpoints.len() > MAX_LIBP2P_ENDPOINTS
    {
        return Err(error_response("too many advertised endpoints", 400));
    }
    if descriptor
        .direct_endpoints
        .iter()
        .chain(descriptor.libp2p_endpoints.iter())
        .any(|endpoint| endpoint.is_empty() || endpoint.len() > MAX_ENDPOINT_LEN)
    {
        return Err(error_response("invalid advertised endpoint", 400));
    }

    Ok(())
}

fn namespace_from_path(path: &str) -> Option<&str> {
    let mut parts = path.trim_matches('/').split('/');
    match (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) {
        (Some("v1"), Some("namespaces"), Some(namespace), Some("registrations"), None) => {
            Some(namespace)
        }
        _ => None,
    }
}

fn is_valid_namespace(namespace: &str) -> bool {
    namespace.len() == 64
        && namespace
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_json_request(req: &Request) -> Result<bool> {
    Ok(req
        .headers()
        .get("content-type")?
        .map(|content_type| {
            content_type
                .to_ascii_lowercase()
                .contains("application/json")
        })
        .unwrap_or(false))
}

fn client_ip(req: &Request) -> Result<String> {
    Ok(req
        .headers()
        .get("cf-connecting-ip")?
        .and_then(|value| value.split(',').next().map(str::trim).map(str::to_owned))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string()))
}

fn env_string(env: &Env, name: &str) -> Option<String> {
    env.secret(name)
        .map(|value| value.to_string())
        .or_else(|_| env.var(name).map(|value| value.to_string()))
        .ok()
}

fn env_u32(env: &Env, name: &str) -> Option<u32> {
    env_string(env, name)?.parse().ok()
}

fn env_bool(env: &Env, name: &str) -> Option<bool> {
    match env_string(env, name)?.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn env_fingerprint_set(env: &Env, name: &str) -> Option<HashSet<String>> {
    let raw = env_string(env, name)?;
    Some(fingerprints_from_raw(&raw))
}

fn normalize_cert_fingerprint(raw: &str) -> String {
    raw.chars()
        .filter(|ch| ch.is_ascii_hexdigit())
        .map(|ch| ch.to_ascii_lowercase())
        .collect()
}

fn fingerprints_from_raw(raw: &str) -> HashSet<String> {
    raw
        .split(|ch: char| ch == ',' || ch.is_whitespace())
        .map(normalize_cert_fingerprint)
        .filter(|fingerprint| fingerprint.len() == 64)
        .collect::<HashSet<_>>()
}

fn now_unix_ms() -> i64 {
    Date::now().as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_certificate_fingerprints() {
        assert_eq!(
            normalize_cert_fingerprint("AA:bb cc-dd"),
            "aabbccdd"
        );
    }

    #[test]
    fn rejects_non_sha256_fingerprint_entries() {
        let valid = "ab".repeat(32);
        let fingerprints = fingerprints_from_raw(&format!("{valid}, short"));
        assert_eq!(fingerprints.len(), 1);
        assert!(fingerprints.contains(&valid));
        assert!(fingerprints_from_raw("short").is_empty());
    }
}
