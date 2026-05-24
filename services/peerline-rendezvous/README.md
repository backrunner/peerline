# Peerline Rendezvous Worker

Private HTTP rendezvous service for Peerline named transfers. It runs on Cloudflare Workers with a Durable Object shard per rendezvous namespace.

## API

- `GET /healthz`
- `POST /v1/namespaces/:namespace/registrations`
- `DELETE /v1/namespaces/:namespace/registrations?peer_id=...`
- `GET /v1/namespaces/:namespace/registrations?after_cookie=...&limit=...`

## Observability

The Worker emits structured JSON logs to Cloudflare Logs/Tail for request completion,
edge and Durable Object rate limits, authentication failures, descriptor validation
failures, edge-to-Durable-Object forwarding, successful registrations, explicit
unregistrations, and successful discovery lookups.

Responses include:

- `x-peerline-request-id`: copied from `x-request-id`, `cf-ray`, or `traceparent` when present.
- `server-timing`: total Worker handling time in milliseconds.

Clients always send:

- `x-peerline-version`

Shared-secret deployments also send:

- `x-peerline-timestamp`
- `x-peerline-signature`

Private deployments should authenticate the client with Cloudflare mTLS and can optionally pin allowed client certificate fingerprints by setting `PEERLINE_RENDEZVOUS_ALLOWED_CLIENT_CERT_FINGERPRINTS` as a Worker var. The value is a comma-separated list of SHA-256 fingerprints.

Set `PEERLINE_RENDEZVOUS_REQUIRE_MTLS=1` for the private deployment. The client should point at a PEM bundle that contains the certificate chain and private key:

```bash
export PEERLINE_RENDEZVOUS_CLIENT_IDENTITY_PATH=/path/to/peerline-client.pem
```

You can also supply the bundle inline with `PEERLINE_RENDEZVOUS_CLIENT_IDENTITY_PEM`.

Official release builds can embed the same PEM from GitHub Actions secrets by
setting `PEERLINE_RENDEZVOUS_CLIENT_IDENTITY_PEM` or
`PEERLINE_RENDEZVOUS_CLIENT_IDENTITY_PEM_B64` in the repository secret store.
The workflow forwards those values to Cargo as
`PEERLINE_BUILD_RENDEZVOUS_CLIENT_IDENTITY_PEM` and
`PEERLINE_BUILD_RENDEZVOUS_CLIENT_IDENTITY_PEM_B64`.

Embedding a client certificate private key keeps it out of source control and
CI logs, but it is not a true secret once distributed in a public binary. Use
short-lived certificates, fingerprint allow-lists, rate limits, and rotation for
the private default rendezvous deployment.

If you are wiring Peerline to another rendezvous service that still expects a shared secret, set `PEERLINE_RENDEZVOUS_TOKEN` in the client environment instead.

The Worker caps registration TTLs with `PEERLINE_RENDEZVOUS_MAX_TTL_SECS`; the checked-in deployment config sets it to `180`. The client default registration TTL is `120` seconds with a `60` second keepalive.

## Deploy

```bash
cd services/peerline-rendezvous
npx wrangler deploy
```

The default production route is the custom domain `rendezvous.peerline.pwp.sh`.
Prerelease deployments use isolated subdomains:

- `alpha` -> `alpha.rendezvous.peerline.pwp.sh`
- `beta` -> `beta.rendezvous.peerline.pwp.sh`

Deploy them with `npx wrangler deploy --env alpha` or `npx wrangler deploy --env beta`.

The Worker also uses an internal Durable Object rate window. If your Cloudflare account has the Rate Limiting binding enabled, bind it as `RATE_LIMITER`; the Worker will use it before Durable Object routing and continue to enforce the internal limit as a backstop.
