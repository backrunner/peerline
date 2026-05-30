# Peerline

Peerline is a terminal-first peer-to-peer file transfer tool.

It is built for the moments when you want to move files, folders, logs, build artifacts, or project snapshots directly between two machines without first uploading them to a cloud drive. One side opens a receive session, shares a human-friendly name and code, and the other side sends one or more paths. Peerline then tries to find the best route between the two peers and transfers the data with application-layer end-to-end encryption.

Peerline is not a sync service and it does not keep a permanent hosted copy of your files. It is closer to a terminal copy workflow: start a receiver, send one or more batches of paths, verify the transfer, and exit when you are done.

## Why Peerline Exists

- Direct handoff: transfer files and folders from one machine to another without using shared storage as the normal path.
- Human-friendly pairing: use readable names and codes instead of asking people to exchange long keys or machine identifiers.
- Flexible networking: publish compact discovery records to pkarr/mainline, discover peers through rendezvous and libp2p, and use fallback routes only when better paths are unavailable.
- Encrypted by default: authenticate the transfer with the shared code and encrypt file contents at the application layer.
- Safe file handling: archive multiple inputs with safe relative paths, BLAKE3 integrity checks, and non-overwriting output by default.

## How It Works

Peerline has two roles:

- Receiver: runs `peerline recv`, keeps listening for incoming transfers until you quit or the idle timeout expires, and prints a `name`, `code`, and direct endpoint.
- Sender: runs `peerline send`, points at the receiver by name/code or IP/code, and provides the files or folders to send.

For named transfers, the receiver publishes a short-lived descriptor keyed by the shared name and code. Peerline publishes that descriptor to multiple backends at once: a compact raw-binary pkarr announcement on the mainline DHT, the HTTP rendezvous service, and libp2p discovery records. The pkarr payload is budgeted to keep the signed DNS packet under 1000 bytes and retains the highest-value endpoints first when space is tight.

On send, Peerline does a fast pkarr/mainline probe, queries HTTP rendezvous in parallel, and keeps gathering descriptors from Kademlia, optional libp2p Rendezvous peers, and mDNS for a short grace window when that can improve route diversity. Direct TCP and other non-relay routes are preferred ahead of relay fallback, and Tor candidates are delayed when another usable route already exists.

The built-in official rendezvous endpoint follows the release channel: alpha builds use `https://alpha.rendezvous.peerline.pwp.sh`, beta builds use `https://beta.rendezvous.peerline.pwp.sh`, and stable builds use `https://rendezvous.peerline.pwp.sh`. Official endpoints are mTLS-protected; official release assets can embed the client identity, while source builds can set `PEERLINE_RENDEZVOUS_CLIENT_IDENTITY_PATH` or `PEERLINE_RENDEZVOUS_CLIENT_IDENTITY_PEM` to a PEM bundle with the client certificate and private key. Set `PEERLINE_RENDEZVOUS_URLS` / `PEERLINE_RENDEZVOUS_URL` to point at another compatible rendezvous service. Use `PEERLINE_RENDEZVOUS_TOKEN` only when the rendezvous service expects a shared secret.

For direct transfers, the sender can skip discovery and dial an IP address or `host:port` directly. This is useful on the same network, over VPNs, or whenever the receiver already knows which address the sender should use. If you only provide an IP, Peerline probes the default direct window `43117-43121`.

During a transfer, Peerline scans the requested paths into a manifest, preserves directories with safe relative paths, compresses when useful, and streams the archive to the receiver. The receiver verifies file sizes and BLAKE3 hashes before writing files into the current directory. Existing files are kept by default; conflicting names receive a non-overwriting suffix unless `--overwrite` is used.

The secure session combines the shared code with OPAQUE PAKE, X25519, ML-KEM, HKDF, ratcheted message keys, and ChaCha20-Poly1305. Discovery and transport routes can vary, but file contents stay encrypted by Peerline itself.

## Install And Run

Use the npm package without installing it globally:

```sh
npx peerline@beta --help
```

Install it globally through npm:

```sh
npm install -g peerline@beta
peerline --help
```

The npm package is a small launcher. It runs a local `target/debug/peerline` or `target/release/peerline` during development; installed packages download the matching macOS, Linux, or Windows binary from the corresponding GitHub Release on first use, cache the current version locally, and prune older cached versions. If that first download takes longer than three seconds, the launcher shows a lightweight terminal progress line with download status and speed.

Run from source:

```sh
cargo run -p peerline-cli -- --help
```

Build a local binary:

```sh
cargo build --release -p peerline-cli
./target/release/peerline --help
```

Before the first transfer, check your local networking helpers:

```sh
peerline doctor
peerline setup
```

`peerline doctor` prints the current platform, Peerline config path, package manager detection, and dependency status. Use `peerline doctor --json` for machine-readable output.

`peerline setup` opens an interactive terminal checklist for missing dependencies, with install actions for macOS Homebrew, common Linux package managers, and Windows Chocolatey. Run `peerline setup --no-tui` to print the same plan without the interactive UI.

Tor is optional, but installing the `tor` command gives Peerline onion routes when direct and relay paths are blocked. pkarr/mainline discovery is built into Peerline; there is no separate `bt` or `mainline` binary to install.

## Basic Usage

Start a receiver:

```sh
peerline recv
```

The receiver prints values like:

```text
peerline recv
name: frost-827
code: fig-mint-rose-123456
direct: 0.0.0.0:43117
waiting for transfers over direct TCP, Tor onion, libp2p TCP, libp2p QUIC, DCUtR, WebRTC/TURN, relay fallback...
idle timeout: 10 min (change with --idle-timeout-minutes)
```

Send a file, multiple files, or a folder by name and code:

```sh
peerline send frost-827 fig-mint-rose-123456 ./file.txt
peerline send frost-827 fig-mint-rose-123456 ./file.txt ./notes.md ./photos
```

Receive with a saved name:

```sh
peerline set name frost-827
peerline recv
```

After a name is saved, you can receive with only a fresh code:

```sh
peerline recv fig-mint-rose-123456
```

Use a direct IP address when discovery is not needed:

```sh
peerline send 192.168.1.23:43117 ./file.txt --code=fig-mint-rose-123456
peerline send 192.168.1.23 ./folder --code=fig-mint-rose-123456
```

When the port is omitted, Peerline probes the default direct window `43117-43121`.

## Common Options

Receiver options:

```sh
peerline recv [NAME_OR_CODE] [CODE] --port 43117
peerline recv [NAME_OR_CODE] [CODE] --overwrite
peerline recv [NAME_OR_CODE] [CODE] --no-tui
peerline recv [NAME_OR_CODE] [CODE] --no-relay-fallback
peerline recv [NAME_OR_CODE] [CODE] --no-upnp
peerline recv [NAME_OR_CODE] [CODE] --no-nat-pmp-pcp
peerline recv [NAME_OR_CODE] [CODE] --no-quic
peerline recv [NAME_OR_CODE] [CODE] --no-dcutr
peerline recv [NAME_OR_CODE] [CODE] --no-turn
peerline recv [NAME_OR_CODE] [CODE] --no-tor
peerline recv [NAME_OR_CODE] [CODE] --idle-timeout-minutes 30
peerline recv [NAME_OR_CODE] [CODE] --tunnel cloudflared
peerline recv [NAME_OR_CODE] [CODE] --tunnel localtunnel
peerline recv [NAME_OR_CODE] [CODE] --tunnel tmole
peerline recv [NAME_OR_CODE] [CODE] --tunnel tunnelmole
```

`--port` starts the 5-port direct window; Peerline will try that port and the next four.
`--idle-timeout-minutes` defaults to `10`; set it to `0` to wait until you quit manually.
Tor onion receive is attempted by default when the `tor` command is available; use `--no-tor` or `PEERLINE_DISABLE_TOR=1` to skip it.
`--tunnel` starts a public tunnel on the receiver side. `cloudflared`, `localtunnel`, and `tmole` are the public values; `tunnelmole` is accepted as an alias for `tmole`.

Sender options:

```sh
peerline send <name> <code> <path...> --compression auto
peerline send <name> <code> <path...> --compression none
peerline send <name> <code> <path...> --compression zstd
peerline send <name> <code> <path...> --compression lzma
peerline send <name> <code> <path...> --no-relay-fallback
peerline send <name> <code> <path...> --no-upnp
peerline send <name> <code> <path...> --no-nat-pmp-pcp
peerline send <name> <code> <path...> --no-quic
peerline send <name> <code> <path...> --no-dcutr
peerline send <name> <code> <path...> --no-turn
peerline send <name> <code> <path...> --no-tor
peerline send <name> <code> <path...> --tor-socks-proxy 127.0.0.1:9050
peerline send <name> <code> <path...> --retry-attempts 5
peerline send --name <name> --code <code> <path...>
```

Relay fallback is enabled by default. Direct and hole-punched routes are attempted before relay fallback; pass `--no-relay-fallback` on both sides when you want to require a direct or hole-punched path.
Send retries default to `5` attempts before the send TUI asks whether to retry the whole send.
In the send TUI, a failed attempt stays on screen and offers `r` to retry the same send or `q`/Esc to quit.

Network environment variables:

```sh
PEERLINE_RENDEZVOUS_URLS=https://rendezvous.example.net
PEERLINE_RENDEZVOUS_TIMEOUT_MS=5000
PEERLINE_RENDEZVOUS_TTL_SECS=120
PEERLINE_RENDEZVOUS_KEEPALIVE_SECS=60
PEERLINE_LIBP2P_RENDEZVOUS_PEERS=/dnsaddr/rzv.example.net/p2p/...
PEERLINE_PKARR_BOOTSTRAP=router1.example.net:6881,router2.example.net:6881
PEERLINE_RELAY_PEERS=/ip4/203.0.113.10/tcp/4001/p2p/...
PEERLINE_DISABLE_RELAY_FALLBACK=1
PEERLINE_DISABLE_MDNS=1
PEERLINE_DISABLE_UPNP=1
PEERLINE_DISABLE_NATPMP_PCP=1
PEERLINE_DISABLE_QUIC=1
PEERLINE_DISABLE_DCUTR=1
PEERLINE_DISABLE_TURN=1
PEERLINE_DISABLE_PUBLIC_TUNNELS=1
PEERLINE_DISABLE_TOR=1
PEERLINE_ALLOW_LOOPBACK_DISCOVERY=1
PEERLINE_BOOTSTRAP=/dnsaddr/bootstrap.libp2p.io/p2p/...
PEERLINE_WEBRTC_ICE_SERVERS='[{"urls":["turn:turn.example.net:3478?transport=udp"],"username":"user","credential":"pass"}]'
```

`PEERLINE_RENDEZVOUS_URLS` should contain comma-separated compatible HTTP rendezvous endpoints. The singular `PEERLINE_RENDEZVOUS_URL` is also supported.
`PEERLINE_PKARR_BOOTSTRAP` should contain comma-separated `host:port` mainline DHT bootstrap nodes for pkarr. Leave it unset to use pkarr's default bootstrap network; setting it is mainly useful for tests, private deployments, or isolated bootstrap pools.
`PEERLINE_LIBP2P_RENDEZVOUS_PEERS` should contain comma-separated libp2p Rendezvous point multiaddrs. This optional backend is disabled unless at least one rendezvous point is configured; discovered registrations are used as libp2p route candidates.
`PEERLINE_RELAY_PEERS` should contain comma-separated libp2p relay-capable peer multiaddrs. If it is not set, Peerline tries the bootstrap peers as relay candidates. UPnP is enabled by default for home-router TCP and libp2p external address discovery; set `PEERLINE_DISABLE_UPNP=1` to turn it off.
WebRTC direct transport ships with default Open Relay Project TURN candidates for `staticauth.openrelay.metered.ca` on ports `80` and `443`, including UDP, TCP, and TLS-style `turns` URLs. Peerline generates short-lived static-auth credentials for those defaults. Set `PEERLINE_WEBRTC_ICE_SERVERS` to a JSON array of WebRTC ICE server objects to replace the defaults; set it to an empty string to disable configured ICE servers entirely.
`PEERLINE_ALLOW_LOOPBACK_DISCOVERY=1` is mainly useful for local development and E2E tests where both peers run on the same machine.

## Current Status

- Direct IP send and receive works.
- Named discovery now publishes and resolves descriptors through pkarr/mainline, HTTP rendezvous, Kademlia records/providers, optional libp2p Rendezvous points, and mDNS.
- Receiver descriptors are packed into a compact binary pkarr announcement that keeps the signed DNS packet within the 1000-byte budget while prioritizing the most useful endpoints first.
- Route candidates include direct TCP, public tunnels, Tor onion, libp2p TCP/QUIC/DCUtR, WebRTC direct/TURN, and relay fallback.
- Files, multiple files, and folders are archived with safe relative paths, BLAKE3 integrity checks, and streaming zstd/lzma compression support.
- Receivers stay open across multiple incoming transfers, with a configurable idle auto-exit.
- Conflicts default to non-overwrite suffixes, with `--overwrite` available when replacement is intended.
- The receive side includes a modern terminal UI for identity, route state, and transfer progress.
- The workspace test suite and E2E coverage are in place.
- The repository also includes a private Cloudflare Worker rendezvous service in `services/peerline-rendezvous`.

## License

Apache-2.0.
