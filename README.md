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

## Release

Releases are published with the bundled npm release script:

```sh
npm run release:alpha -- --otp=123456
npm run release:beta -- --otp=123456
npm run release:stable -- --otp=123456
```

The release script publishes an unscoped platform binary package for the current runner, such as
`peerline-linux-x64-gnu`, `peerline-linux-arm64-musl`, or `peerline-darwin-arm64`, and then
publishes the main `peerline` shim. It runs `npm run lint` and `npm test` before publishing unless
`--skip-tests` is passed.

GitHub Actions has a manual `Release Packages` workflow that does this for native runners:
Linux x64 glibc, Linux arm64 glibc, Linux x64 musl, Linux arm64 musl, macOS arm64, macOS x64, and
Windows x64. Add an npm automation token as the `NPM_TOKEN` repository secret, run the workflow
once with `publish=false` for a dry run, then rerun with `publish=true`. The workflow can publish
`alpha`, `beta`, or `stable`, and can create a GitHub release after npm publish.

If a publish attempt fails after the version bump commit, retry the current version:

```sh
npm run release:alpha -- --current --otp=123456
npm run release:beta -- --current --otp=123456
npm run release:stable -- --current --otp=123456
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
