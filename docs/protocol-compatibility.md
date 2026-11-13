# Protocol compatibility

Application dependency upgrades and new transports do not inherently require
a new transfer protocol. Peerline currently uses one exact transfer-protocol
version across TCP/BLE, WebSocket tunnels and libp2p; it does not yet negotiate
a range of versions or optional capabilities. The rendezvous descriptor version
and libp2p stream identifier are separate from this transfer version.

## Why v4 rejects v3

The v4 byte-stream sender requires an authenticated completion reply from the
receiver. The v3 completion ACK has no session-key authentication. Accepting
that ACK as an automatic fallback would restore the false-success risk the
change fixes. Old peers must therefore be rejected under the current security
policy. This does not mean v3 file contents were sent without encryption.

Using a shared version deliberately also rejects v3 on other Peerline routes,
even though their completion exchange did not change. This is a conservative
compatibility policy, not a requirement imposed by BLE. Introducing support for
multiple versions would require per-version codecs, cryptographic transcript
handling and interoperability tests; merely accepting a different number is
not safe. AirDrop uses its separate native protocol and is unaffected.

## Rejection behavior

- The intro keeps a stable Postcard enum tag (client `0`, server `1`), followed
  by a `u16` protocol version. Decoders inspect that prefix before interpreting
  the version-specific body. Future intro formats must preserve this prefix.
- A version other than `4`, including an unknown future version, is rejected.
  The reply uses the existing Error tag (`5`) and string representation that
  v3 peers already understand. It reports both protocol numbers, tells the user
  to upgrade the installation using the older protocol, and never negotiates
  an automatic downgrade.
- An incompatible incoming intro is rejected before authentication, session
  creation, resume-cache cleanup/creation or writing received files. The
  receiver remains available for a compatible connection.
- A sender receiving an incompatible intro or an old peer's Error stops that
  attempt before ClientFinish or file-data frames. Explicit incompatibility is
  non-retryable once candidate discovery/port probing has exhausted its options.
- Other candidates are still tried: a pre-authentication error is not proof of
  the intended peer's identity. If no candidate succeeds, a version/authentication
  failure is preserved instead of being overwritten by a later socket error.
- Malformed/truncated prefixes and oversized frames fail decoding rather than
  being interpreted as another version. They may produce a generic framing or
  transport error; there is no reliable version number to report in that case.

The local `UnsupportedIntro` decoding result is excluded from serialization;
this diagnostic change does not alter valid v4 messages or add a new wire tag.
The selected version remains bound into the authenticated handshake transcript.

## Regression coverage

Tests cover v3, invalid zero and future versions (including `u16::MAX`), changed
or malformed intro bodies, stable error tags, early rejection on byte streams,
WebSockets and actual libp2p request/response streams, preservation of a usable
receiver, no receive-state creation, no sender ClientFinish after rejection,
and port/candidate errors arriving in either order. A compatible later port
still succeeds after an incompatible earlier port.

These tests emulate incompatible peers at the framing boundary; they do not
claim successful transfer interoperability with historical binaries. Future
optional features should use authenticated capability negotiation without
changing this major version when existing semantics remain compatible. Such
capability negotiation is not implemented by this change.
