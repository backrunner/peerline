# Dependency update — 2026-11-13

Updated 39 existing workspace dependency requirements and refreshed both Cargo
lockfiles, including compatible transitive releases. The npm launcher has no
third-party runtime dependencies. Existing prerelease dependencies remain on
their latest available release lines.

| Area | Major updates |
| --- | --- |
| Networking | libp2p 0.57.1, libp2p-webrtc 0.10.0-alpha, pkarr 9.0.0, reqwest 0.13.5, igd-next 0.18.0, crab_nat 0.8.1, tokio-tungstenite 0.30.0 |
| Crypto | chacha20poly1305 0.11.0, hkdf/hmac 0.13.0, sha2 0.11.1, sha1 0.11.0, x25519-dalek 3.0.0, ml-kem 0.3.4 |
| CLI/data | rand 0.10.3, ratatui 0.30.2, toml 1.2.1, zstd 0.14.4, base64 0.23.1 |
| Worker | worker and worker-build 0.9.0, wasm-bindgen 0.2.130 |
| New BLE dependencies | btleplug 0.13.5, ble-peripheral-rust 0.2.0, uuid 1.29.2; vendored dbus 0.9.12 on Linux |
| Optional receive features | arboard 3.7.0, native clipboard generation APIs; hyper 1.12.0, tokio-rustls 0.26.6, rcgen 0.14.10, plist 1.10.3, flate2 1.1.11 |

## Migration decisions

- Rebased the vendored WebRTC transport onto upstream 0.10.0-alpha while
  retaining Peerline's configurable ICE/TURN support. The upstream update also
  moves the underlying WebRTC stack to 0.17.
- libp2p Codec implementations now use native async trait methods. UPnP events
  now distinguish local and external addresses.
- pkarr 9 uses explicit `ResolvePolicy::NetworkOnly`, typed not-found errors,
  DHT-specific timeouts and mutable DHT configuration. Local DHT integration
  tests use mainline 8 directly because pkarr no longer reexports it.
- Migrated rand generation/selection APIs and X25519 RNG borrowing. Updated
  reqwest's Rustls feature name and the HMAC key initialization import.
- The follow-up review found RUSTSEC-2026-0387 in the published opaque-ke
  4.1.0-pre.2 dependency chain. Pin upstream opaque-ke at
  `070eac916fa58c71029a13021c127861f58d11d1`, which migrates to current RustCrypto
  dependencies and voprf 0.6.0-pre.2. This is an unreleased upstream revision,
  not a floating branch. Remove the temporary SHA-2 0.10 compatibility alias;
  the suite now uses SHA-2 0.11 throughout.
- Updated Worker SDK, binding generator and build tool together. The existing
  main-branch workflow automatically deploys rendezvous changes after push.
- Vendor two small ble-peripheral-rust 0.2.0 platform fixes: never power on a
  Linux adapter automatically, and tear down/recreate macOS manager threads.
  See its `PEERLINE_PATCHES.md` and retained upstream license.

## Follow-up security review

The byte-stream transport now authenticates the final success reply with the
receiver's session key. Previously a forged plaintext ACK could claim success.
Protocol version is bumped from 3 to 4, with explicit old-peer rejection; both
peers must upgrade. This applies to TCP and the new BLE stream transport.

Both lockfiles pass vulnerability auditing after the OPAQUE migration. The main
lockfile still contains unmaintained-package notices for `atomic-polyfill`
(target-specific), `paste` and `pin-utils`; these are recorded rather than
suppressed. No vulnerability advisory is ignored.

## Verification scope

Workspace tests include direct TCP, discovery, local pkarr/mainline, transfer,
crypto, CLI integration and simulated BLE. Validation also covers strict
Clippy, formatting, Node launcher tests, macOS CLI compilation, Linux x86_64
musl linking, Windows GNU cross-target checking and the full Worker WASM build.

Bluetooth initialization/advertising, idle exit, and a sender scan that correctly
failed after 30 seconds without a matching receiver were exercised on the local
Mac. Neither cross compilation nor simulated GATT tests prove physical BLE
interoperability. The BLE acceptance procedure is in
[sendme-and-ble.md](sendme-and-ble.md).

Worker bundling emits warnings for its generated optional `queue` and
`scheduled` handlers (the service implements neither), and wasm-bindgen warns
that symbol stripping removes the Wasm feature section. Packaging completes;
runtime deployment remains separate.
