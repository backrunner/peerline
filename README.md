# Peerline

Peerline is a terminal-first peer-to-peer file transfer tool.

It is built for the moments when you want to move files, folders, logs, build artifacts, or project snapshots directly between two machines without first uploading them to a cloud drive. One side opens a receive session, shares a human-friendly name and code, and the other side sends one or more paths. Peerline then tries to find the best route between the two peers and transfers the data with application-layer end-to-end encryption.

Peerline is not a sync service and it does not keep a permanent hosted copy of your files. It is closer to a terminal copy workflow: start a receiver, send one or more batches of paths, verify the transfer, and exit when you are done.

## Why Peerline Exists

- Direct handoff: transfer files and folders from one machine to another without using shared storage as the normal path.
- Human-friendly pairing: use readable names and codes instead of asking people to exchange long keys or machine identifiers.
- Flexible networking: prefer direct TCP, discover peers through libp2p, and use fallback routes when direct paths are unavailable.
- Encrypted by default: authenticate the transfer with the shared code and encrypt file contents at the application layer.
- Safe file handling: archive multiple inputs with safe relative paths, BLAKE3 integrity checks, and non-overwriting output by default.

## How It Works

Peerline has two roles:

- Receiver: runs `peerline recv`, keeps listening for incoming transfers until you quit or the idle timeout expires, and prints a `name`, `code`, and direct endpoint.
- Sender: runs `peerline send`, points at the receiver by name/code or IP/code, and provides the files or folders to send.

For named transfers, the receiver publishes a short-lived descriptor keyed by the shared name and code. The sender derives the same lookup key, discovers candidate routes, and tries them in order. Peerline prefers direct LAN or public TCP endpoints first, then libp2p routes such as DCUtR and WebRTC TURN. Relay data fallback is available only when explicitly enabled.

Peerline also probes HTTP rendezvous first, with a default endpoint at `https://peerline.pwp.sh`, then falls back to DHT and mDNS. Official builds use a private mTLS-protected rendezvous endpoint; set `PEERLINE_RENDEZVOUS_CLIENT_IDENTITY_PATH` or `PEERLINE_RENDEZVOUS_CLIENT_IDENTITY_PEM` to a PEM bundle with the client certificate and private key, or set `PEERLINE_RENDEZVOUS_URLS` / `PEERLINE_RENDEZVOUS_URL` to point at another compatible rendezvous service. Use `PEERLINE_RENDEZVOUS_TOKEN` only when the rendezvous service expects a shared secret.

For direct transfers, the sender can skip discovery and dial an IP address or `host:port` directly. This is useful on the same network, over VPNs, or whenever the receiver already knows which address the sender should use. If you only provide an IP, Peerline probes the default direct window `43117-43121`.

During a transfer, Peerline scans the requested paths into a manifest, preserves directories with safe relative paths, compresses when useful, and streams the archive to the receiver. The receiver verifies file sizes and BLAKE3 hashes before writing files into the current directory. Existing files are kept by default; conflicting names receive a non-overwriting suffix unless `--overwrite` is used.

The secure session combines the shared code with OPAQUE PAKE, X25519, ML-KEM, HKDF, and ChaCha20-Poly1305. Discovery and transport routes can vary, but file contents stay encrypted by Peerline itself.

## Install And Run

Use the npm package without installing it globally:

```sh
npx peerline@alpha --help
```

Install it globally through npm:

```sh
npm install -g peerline@alpha
peerline --help
```

The npm package is a small launcher. It runs a local `target/debug/peerline` or `target/release/peerline` during development; installed packages download the matching macOS, Linux, or Windows binary from the corresponding GitHub Release on first use and cache it locally.

Run from source:

```sh
cargo run -p peerline-cli -- --help
```

Build a local binary:

```sh
cargo build --release -p peerline-cli
./target/release/peerline --help
```

## Basic Usage

Start a receiver:

```sh
peerline recv
```

The receiver prints values like:

```text
peerline recv
name: frost-827
code: fig-mint-1234-5678
direct: 0.0.0.0:43117
waiting for transfers over direct TCP or libp2p...
idle timeout: 10 min (change with --idle-timeout-minutes)
```

Send a file, multiple files, or a folder by name and code:

```sh
peerline send frost-827 fig-mint-1234-5678 ./file.txt
peerline send frost-827 fig-mint-1234-5678 ./file.txt ./notes.md ./photos
```

Receive with a saved name:

```sh
peerline set name frost-827
peerline recv
```

After a name is saved, you can receive with only a fresh code:

```sh
peerline recv fig-mint-1234-5678
```

Use a direct IP address when discovery is not needed:

```sh
peerline send 192.168.1.23:43117 ./file.txt --code=fig-mint-1234-5678
peerline send 192.168.1.23 ./folder --code=fig-mint-1234-5678
```

When the port is omitted, Peerline probes the default direct window `43117-43121`.

## Common Options

Receiver options:

```sh
peerline recv [NAME_OR_CODE] [CODE] --port 43117
peerline recv [NAME_OR_CODE] [CODE] --overwrite
peerline recv [NAME_OR_CODE] [CODE] --no-tui
peerline recv [NAME_OR_CODE] [CODE] --allow-relay-fallback
peerline recv [NAME_OR_CODE] [CODE] --idle-timeout-minutes 30
```

`--port` starts the 5-port direct window; Peerline will try that port and the next four.
`--idle-timeout-minutes` defaults to `10`; set it to `0` to wait until you quit manually.

Sender options:

```sh
peerline send <name> <code> <path...> --compression auto
peerline send <name> <code> <path...> --compression none
peerline send <name> <code> <path...> --compression zstd
peerline send <name> <code> <path...> --compression lzma
peerline send <name> <code> <path...> --allow-relay-fallback
peerline send --name <name> --code <code> <path...>
```

Relay fallback must be enabled on both sides when you want Peerline to use relay data paths. Direct and hole-punched routes are attempted before relay fallback.
In the send TUI, a failed attempt stays on screen and offers `r` to retry the same send or `q`/Esc to quit.

## Current Status

- Direct IP send and receive works.
- Named discovery now uses HTTP rendezvous first, then Kademlia provider records, mDNS, DCUtR, relay fallback, and libp2p-webrtc's built-in ICE servers.
- Files, multiple files, and folders are archived with safe relative paths, BLAKE3 integrity checks, and streaming zstd/lzma compression support.
- Receivers stay open across multiple incoming transfers, with a configurable idle auto-exit.
- Conflicts default to non-overwrite behavior, with TUI-driven handling in the receiver flow.
- The receive side includes a modern terminal UI for identity, route state, and transfer progress.
- The workspace test suite and E2E coverage are in place.
- The repository also includes a private Cloudflare Worker rendezvous service in `services/peerline-rendezvous`.

## License

Apache-2.0.
