# Peerline Rendezvous Worker

Private HTTP rendezvous service for Peerline named transfers. It runs on Cloudflare Workers with a Durable Object shard per rendezvous namespace.

## API

- `GET /healthz`
- `POST /v1/namespaces/:namespace/registrations`
- `GET /v1/namespaces/:namespace/registrations?after_cookie=...&limit=...`

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

If you are wiring Peerline to another rendezvous service that still expects a shared secret, set `PEERLINE_RENDEZVOUS_TOKEN` in the client environment instead.

## Deploy

```bash
cd services/peerline-rendezvous
npx wrangler deploy
```

The default production route is the custom domain `peerline.pwp.sh`.
Prerelease deployments use isolated subdomains:

- `alpha` -> `alpha.peerline.pwp.sh`
- `beta` -> `beta.peerline.pwp.sh`

Deploy them with `npx wrangler deploy --env alpha` or `npx wrangler deploy --env beta`.

The Worker also uses an internal Durable Object rate window. If your Cloudflare account has the Rate Limiting binding enabled, bind it as `RATE_LIMITER`; the Worker will use it before Durable Object routing and continue to enforce the internal limit as a backstop.
