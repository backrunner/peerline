use peerline_rendezvous_model::{
    HEADER_SIGNATURE, HEADER_TIMESTAMP, HEADER_VERSION, PeerDescriptor, RendezvousDiscoverRequest,
    RendezvousDiscoverResponse, RendezvousRecord, RendezvousRegisterRequest,
    RendezvousRegisterResponse, RendezvousUnregisterResponse, request_path_and_query,
    verify_peerline_request_signature,
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
const UNREGISTER_RATE_LIMIT: u32 = 60;
const DISCOVER_RATE_LIMIT: u32 = 120;
const SIGNATURE_SKEW_MS: i64 = 5 * 60 * 1000;

#[event(fetch, respond_with_errors)]
pub async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    console_error_panic_hook::set_once();

    let telemetry = RequestTelemetry::from_request(&req)?;

    if matches!(req.method(), Method::Get) && telemetry.path == "/healthz" {
        let response = Response::ok("ok")?;
        log_request_complete("rendezvous_healthz", &telemetry, response.status_code());
        return with_observability_headers(response, &telemetry);
    }

    let Some(namespace) = namespace_from_path(&telemetry.path) else {
        return observed_error_response(
            "rendezvous_edge_rejected",
            &telemetry,
            None,
            "not found",
            404,
            serde_json::json!({ "reason": "route_not_found" }),
        );
    };
    if !is_valid_namespace(namespace) {
        return observed_error_response(
            "rendezvous_edge_rejected",
            &telemetry,
            Some(namespace),
            "invalid namespace",
            400,
            serde_json::json!({ "reason": "invalid_namespace" }),
        );
    }
    if !matches!(req.method(), Method::Get | Method::Post | Method::Delete) {
        return observed_error_response(
            "rendezvous_edge_rejected",
            &telemetry,
            Some(namespace),
            "method not allowed",
            405,
            serde_json::json!({ "reason": "method_not_allowed" }),
        );
    }

    if let Ok(limiter) = env.rate_limiter(RATE_LIMIT_BINDING) {
        let ip = client_ip(&req)?;
        let key = format!("peerline:rendezvous:{namespace}:{}:{ip}", req.method());
        if !limiter.limit(key).await?.success {
            return observed_error_response(
                "rendezvous_edge_rate_limited",
                &telemetry,
                Some(namespace),
                "rate limit exceeded",
                429,
                serde_json::json!({ "reason": "edge_rate_limit" }),
            );
        }
    }

    let stub = env.durable_object(DO_BINDING)?.get_by_name(namespace)?;
    log_request_event(
        LogLevel::Info,
        "rendezvous_edge_forward",
        &telemetry,
        Some(namespace),
        serde_json::json!({ "durable_object": DO_BINDING }),
    );
    let response = stub.fetch_with_request(req).await?;
    log_request_complete(
        "rendezvous_request_complete",
        &telemetry,
        response.status_code(),
    );
    with_observability_headers(response, &telemetry)
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
        let telemetry = RequestTelemetry::from_request(&req)?;
        let Some(namespace) = namespace_from_path(&telemetry.path) else {
            return observed_error_response(
                "rendezvous_do_rejected",
                &telemetry,
                None,
                "not found",
                404,
                serde_json::json!({ "reason": "route_not_found" }),
            );
        };
        if !is_valid_namespace(namespace) {
            return observed_error_response(
                "rendezvous_do_rejected",
                &telemetry,
                Some(namespace),
                "invalid namespace",
                400,
                serde_json::json!({ "reason": "invalid_namespace" }),
            );
        }

        match req.method() {
            Method::Post => self.register(namespace, &mut req, &telemetry).await,
            Method::Get => self.discover(namespace, &mut req, &telemetry).await,
            Method::Delete => self.unregister(namespace, &req, &telemetry).await,
            _ => observed_error_response(
                "rendezvous_do_rejected",
                &telemetry,
                Some(namespace),
                "method not allowed",
                405,
                serde_json::json!({ "reason": "method_not_allowed" }),
            ),
        }
    }
}

impl RendezvousShard {
    async fn register(
        &self,
        namespace: &str,
        req: &mut Request,
        telemetry: &RequestTelemetry,
    ) -> Result<Response> {
        if !is_json_request(req)? {
            return observed_error_response(
                "rendezvous_register_rejected",
                telemetry,
                Some(namespace),
                "content type must be application/json",
                415,
                serde_json::json!({ "reason": "content_type" }),
            );
        }

        let body = req.bytes().await?;
        if body.len() > MAX_REQUEST_BYTES {
            return observed_error_response(
                "rendezvous_register_rejected",
                telemetry,
                Some(namespace),
                "payload too large",
                413,
                serde_json::json!({
                    "reason": "payload_too_large",
                    "body_bytes": body.len(),
                    "max_body_bytes": MAX_REQUEST_BYTES,
                }),
            );
        }
        if let Err(rejection) = self.verify_request(req, &body) {
            return observed_rejection_response(
                "rendezvous_auth_rejected",
                telemetry,
                Some(namespace),
                rejection,
            );
        }
        if !self.enforce_rate_limit(namespace, req, REGISTER_RATE_LIMIT)? {
            return observed_error_response(
                "rendezvous_do_rate_limited",
                telemetry,
                Some(namespace),
                "rate limit exceeded",
                429,
                serde_json::json!({
                    "operation": "register",
                    "limit": REGISTER_RATE_LIMIT,
                }),
            );
        }

        let request = match serde_json::from_slice::<RendezvousRegisterRequest>(&body) {
            Ok(request) => request,
            Err(_) => {
                return observed_error_response(
                    "rendezvous_register_rejected",
                    telemetry,
                    Some(namespace),
                    "invalid registration payload",
                    400,
                    serde_json::json!({ "reason": "invalid_json" }),
                );
            }
        };
        if let Err(rejection) = validate_descriptor(&request.descriptor) {
            return observed_rejection_response(
                "rendezvous_descriptor_rejected",
                telemetry,
                Some(namespace),
                rejection,
            );
        }

        let now = now_unix_ms();
        let ttl_seconds = request.ttl_seconds.clamp(1, self.max_ttl_secs);
        let expires_unix_ms = now + i64::from(ttl_seconds) * 1000;
        self.cleanup_expired(now)?;

        let mut descriptor = request.descriptor;
        descriptor.published_unix_ms = now as u64;
        let peer_id = descriptor.peer_id.clone();
        let direct_endpoint_count = descriptor.direct_endpoints.len();
        let libp2p_endpoint_count = descriptor.libp2p_endpoints.len();
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
                    peer_id.clone().into(),
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

        log_request_event(
            LogLevel::Info,
            "rendezvous_register_ok",
            telemetry,
            Some(namespace),
            serde_json::json!({
                "peer_id": peer_id,
                "cookie": row.cookie,
                "ttl_seconds": ttl_seconds,
                "direct_endpoint_count": direct_endpoint_count,
                "libp2p_endpoint_count": libp2p_endpoint_count,
                "expires_in_ms": expires_unix_ms.saturating_sub(now),
            }),
        );

        Ok(Response::from_json(&RendezvousRegisterResponse {
            cookie: row.cookie as u64,
            record,
        })?
        .with_status(201))
    }

    async fn discover(
        &self,
        namespace: &str,
        req: &mut Request,
        telemetry: &RequestTelemetry,
    ) -> Result<Response> {
        if let Err(rejection) = self.verify_request(req, &[]) {
            return observed_rejection_response(
                "rendezvous_auth_rejected",
                telemetry,
                Some(namespace),
                rejection,
            );
        }
        if !self.enforce_rate_limit(namespace, req, DISCOVER_RATE_LIMIT)? {
            return observed_error_response(
                "rendezvous_do_rate_limited",
                telemetry,
                Some(namespace),
                "rate limit exceeded",
                429,
                serde_json::json!({
                    "operation": "discover",
                    "limit": DISCOVER_RATE_LIMIT,
                }),
            );
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

        log_request_event(
            LogLevel::Info,
            "rendezvous_discover_ok",
            telemetry,
            Some(namespace),
            serde_json::json!({
                "record_count": records.len(),
                "after_cookie": after_cookie,
                "limit": limit,
                "latest_cookie": cookie.cookie,
            }),
        );

        Response::from_json(&RendezvousDiscoverResponse {
            cookie: cookie.cookie as u64,
            records,
        })
    }

    async fn unregister(
        &self,
        namespace: &str,
        req: &Request,
        telemetry: &RequestTelemetry,
    ) -> Result<Response> {
        if let Err(rejection) = self.verify_request(req, &[]) {
            return observed_rejection_response(
                "rendezvous_auth_rejected",
                telemetry,
                Some(namespace),
                rejection,
            );
        }
        if !self.enforce_rate_limit(namespace, req, UNREGISTER_RATE_LIMIT)? {
            return observed_error_response(
                "rendezvous_do_rate_limited",
                telemetry,
                Some(namespace),
                "rate limit exceeded",
                429,
                serde_json::json!({
                    "operation": "unregister",
                    "limit": UNREGISTER_RATE_LIMIT,
                }),
            );
        }

        let peer_id = match unregister_peer_id(req) {
            Some(peer_id) if is_valid_peer_id(&peer_id) => peer_id,
            _ => {
                return observed_error_response(
                    "rendezvous_unregister_rejected",
                    telemetry,
                    Some(namespace),
                    "invalid peer id",
                    400,
                    serde_json::json!({ "reason": "invalid_peer_id" }),
                );
            }
        };

        let now = now_unix_ms();
        self.cleanup_expired(now)?;
        let existing: CountRow = self
            .sql
            .exec(
                "SELECT COUNT(*) AS count FROM registrations WHERE peer_id = ?;",
                vec![peer_id.clone().into()],
            )?
            .one()?;
        self.sql.exec(
            "DELETE FROM registrations WHERE peer_id = ?;",
            vec![peer_id.clone().into()],
        )?;

        log_request_event(
            LogLevel::Info,
            "rendezvous_unregister_ok",
            telemetry,
            Some(namespace),
            serde_json::json!({
                "peer_id": peer_id,
                "removed": existing.count,
            }),
        );

        Response::from_json(&RendezvousUnregisterResponse {
            removed: existing.count.max(0) as u32,
        })
    }

    fn verify_request(&self, req: &Request, body: &[u8]) -> std::result::Result<(), Rejection> {
        self.verify_peerline_version(req)?;
        if let Some(auth) = req.cf().and_then(|cf| cf.tls_client_auth()) {
            return self.verify_tls_client_auth(&auth);
        }
        if self.require_mtls {
            return Err(Rejection::new("client certificate required", 401));
        }
        self.verify_hmac_request(req, body)
    }

    fn verify_peerline_version(&self, req: &Request) -> std::result::Result<(), Rejection> {
        if req
            .headers()
            .get(HEADER_VERSION)
            .map_err(|_| Rejection::new("invalid version header", 400))?
            .filter(|version| !version.trim().is_empty())
            .is_none()
        {
            return Err(Rejection::new("missing peerline version", 401));
        }
        Ok(())
    }

    fn verify_tls_client_auth(&self, auth: &TlsClientAuth) -> std::result::Result<(), Rejection> {
        if auth.cert_presented() != "1" {
            return Err(Rejection::new("client certificate required", 401));
        }
        if !auth.cert_verified().eq_ignore_ascii_case("SUCCESS") {
            return Err(Rejection::new("invalid client certificate", 401));
        }
        if let Some(allowed) = &self.allowed_client_cert_fingerprints {
            let fingerprint = normalize_cert_fingerprint(&auth.cert_fingerprint_sha256());
            if !allowed.contains(&fingerprint) {
                return Err(Rejection::new("unauthorized client certificate", 403));
            }
        }
        Ok(())
    }

    fn verify_hmac_request(
        &self,
        req: &Request,
        body: &[u8],
    ) -> std::result::Result<(), Rejection> {
        let Some(token) = self.auth_token.as_deref() else {
            return Err(Rejection::new(
                "rendezvous auth secret is not configured",
                500,
            ));
        };

        let timestamp = match req
            .headers()
            .get(HEADER_TIMESTAMP)
            .map_err(|_| Rejection::new("invalid timestamp header", 400))?
            .and_then(|value| value.parse::<i64>().ok())
        {
            Some(timestamp) => timestamp,
            None => return Err(Rejection::new("missing request timestamp", 401)),
        };
        if now_unix_ms().abs_diff(timestamp) > SIGNATURE_SKEW_MS as u64 {
            return Err(Rejection::new("stale request timestamp", 401));
        }

        let signature = match req
            .headers()
            .get(HEADER_SIGNATURE)
            .map_err(|_| Rejection::new("invalid signature header", 400))?
        {
            Some(signature) => signature,
            None => return Err(Rejection::new("missing request signature", 401)),
        };

        let url = match req.url() {
            Ok(url) => url,
            Err(_) => return Err(Rejection::new("invalid request URL", 400)),
        };
        let path = request_path_and_query(url.path(), url.query());
        let method = req.method().to_string();
        if !verify_peerline_request_signature(token, timestamp, &method, &path, body, &signature) {
            return Err(Rejection::new("invalid request signature", 401));
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

#[derive(Clone, Debug)]
struct RequestTelemetry {
    request_id: String,
    method: String,
    path: String,
    namespace: Option<String>,
    started_unix_ms: i64,
}

#[derive(Clone, Copy, Debug)]
enum LogLevel {
    Info,
    Warn,
    Error,
}

#[derive(Clone, Copy, Debug)]
struct Rejection {
    message: &'static str,
    status: u16,
}

impl RequestTelemetry {
    fn from_request(req: &Request) -> Result<Self> {
        let path = req.path();
        Ok(Self {
            request_id: request_id(req)?,
            method: req.method().to_string(),
            namespace: namespace_from_path(&path).map(str::to_owned),
            path,
            started_unix_ms: now_unix_ms(),
        })
    }

    fn elapsed_ms(&self) -> i64 {
        now_unix_ms().saturating_sub(self.started_unix_ms)
    }
}

impl Rejection {
    fn new(message: &'static str, status: u16) -> Self {
        Self { message, status }
    }

    fn response(self) -> Result<Response> {
        Response::error(self.message, self.status)
    }
}

fn request_id(req: &Request) -> Result<String> {
    for header in ["x-request-id", "cf-ray", "traceparent"] {
        if let Some(value) = req.headers().get(header)? {
            let value = value.trim();
            if !value.is_empty() {
                return Ok(value.to_string());
            }
        }
    }
    Ok(format!("peerline-{}", now_unix_ms()))
}

fn with_observability_headers(
    mut response: Response,
    telemetry: &RequestTelemetry,
) -> Result<Response> {
    let duration = telemetry.elapsed_ms();
    response
        .headers_mut()
        .set("x-peerline-request-id", &telemetry.request_id)?;
    response
        .headers_mut()
        .set("server-timing", &format!("peerline;dur={duration}"))?;
    Ok(response)
}

fn observed_error_response(
    event: &str,
    telemetry: &RequestTelemetry,
    namespace: Option<&str>,
    message: &'static str,
    status: u16,
    details: serde_json::Value,
) -> Result<Response> {
    log_request_event(
        level_for_status(status),
        event,
        telemetry,
        namespace,
        serde_json::json!({
            "status": status,
            "message": message,
            "details": details,
        }),
    );
    with_observability_headers(Response::error(message, status)?, telemetry)
}

fn observed_rejection_response(
    event: &str,
    telemetry: &RequestTelemetry,
    namespace: Option<&str>,
    rejection: Rejection,
) -> Result<Response> {
    log_request_event(
        level_for_status(rejection.status),
        event,
        telemetry,
        namespace,
        serde_json::json!({
            "status": rejection.status,
            "message": rejection.message,
        }),
    );
    with_observability_headers(rejection.response()?, telemetry)
}

fn log_request_complete(event: &str, telemetry: &RequestTelemetry, status: u16) {
    log_request_event(
        level_for_status(status),
        event,
        telemetry,
        None,
        serde_json::json!({ "status": status }),
    );
}

fn log_request_event(
    level: LogLevel,
    event: &str,
    telemetry: &RequestTelemetry,
    namespace: Option<&str>,
    fields: serde_json::Value,
) {
    let namespace = namespace
        .map(str::to_owned)
        .or_else(|| telemetry.namespace.clone());
    let payload = serde_json::json!({
        "service": "peerline-rendezvous",
        "event": event,
        "request_id": &telemetry.request_id,
        "method": &telemetry.method,
        "path": &telemetry.path,
        "namespace": namespace,
        "duration_ms": telemetry.elapsed_ms(),
        "fields": fields,
    });
    let line = serde_json::to_string(&payload).unwrap_or_else(|_| {
        format!(
            "{{\"service\":\"peerline-rendezvous\",\"event\":\"{event}\",\"request_id\":\"{}\"}}",
            telemetry.request_id
        )
    });

    match level {
        LogLevel::Info => worker::console_log!("{line}"),
        LogLevel::Warn => worker::console_warn!("{line}"),
        LogLevel::Error => worker::console_error!("{line}"),
    }
}

fn level_for_status(status: u16) -> LogLevel {
    match status {
        500..=599 => LogLevel::Error,
        400..=499 => LogLevel::Warn,
        _ => LogLevel::Info,
    }
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

fn validate_descriptor(descriptor: &PeerDescriptor) -> std::result::Result<(), Rejection> {
    if descriptor.protocol_version != DESCRIPTOR_PROTOCOL_VERSION {
        return Err(Rejection::new(
            "unsupported descriptor protocol version",
            400,
        ));
    }
    if !is_valid_peer_id(&descriptor.peer_id) {
        return Err(Rejection::new("invalid peer id", 400));
    }
    if descriptor.direct_endpoints.is_empty() && descriptor.libp2p_endpoints.is_empty() {
        return Err(Rejection::new("no advertised endpoints", 400));
    }
    if descriptor.direct_endpoints.len() > MAX_DIRECT_ENDPOINTS
        || descriptor.libp2p_endpoints.len() > MAX_LIBP2P_ENDPOINTS
    {
        return Err(Rejection::new("too many advertised endpoints", 400));
    }
    if descriptor
        .direct_endpoints
        .iter()
        .chain(descriptor.libp2p_endpoints.iter())
        .any(|endpoint| endpoint.is_empty() || endpoint.len() > MAX_ENDPOINT_LEN)
    {
        return Err(Rejection::new("invalid advertised endpoint", 400));
    }

    Ok(())
}

fn unregister_peer_id(req: &Request) -> Option<String> {
    req.url()
        .ok()?
        .query_pairs()
        .find(|(key, _)| key == "peer_id")
        .map(|(_, value)| value.into_owned())
}

fn is_valid_peer_id(peer_id: &str) -> bool {
    !peer_id.is_empty()
        && peer_id.len() <= 128
        && peer_id.bytes().all(|byte| byte.is_ascii_alphanumeric())
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
    raw.split(|ch: char| ch == ',' || ch.is_whitespace())
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
        assert_eq!(normalize_cert_fingerprint("AA:bb cc-dd"), "aabbccdd");
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
