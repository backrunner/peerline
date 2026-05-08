# Peerline

Peerline is a Rust CLI/TUI for peer-to-peer file transfer.
It prefers direct TCP first, then libp2p discovery and fallback routes, while keeping file contents end-to-end encrypted at the application layer.

## Current Status

- Direct IP send/recv works.
- Named discovery uses saved names, `code`, OPAQUE PAKE, Kademlia provider records, mDNS, DCUtR, relay fallback, and libp2p-webrtc's built-in ICE servers.
- Files, multiple files, and folders are archived with safe relative paths, BLAKE3 integrity checks, and streaming zstd/lzma compression support.
- Conflicts default to non-overwrite behavior, with TUI-driven handling in the receiver flow.
- The receive side includes a modern terminal UI for identity, route state, and transfer progress.
- The workspace test suite and E2E coverage are in place.

## Install

```sh
npx peerline@alpha --version
```

Or from source:

```sh
cargo run -p peerline-cli -- --help
```

## Usage

```sh
peerline set name river-mango-42
peerline recv rose-lime-iris-jade-1234
peerline send river-mango-42 rose-lime-iris-jade-1234 ./file.txt
peerline send 127.0.0.1:43117 ./file.txt --code=rose-lime-iris-jade-1234
```

## License

Apache-2.0.
