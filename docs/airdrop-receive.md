# Experimental AirDrop receiver

`peerline recv --airdrop` is a separate receive-only compatibility mode on
macOS, disabled by default. BLE file transport remains `recv --ble`; AirDrop
uses IPv6, Wi-Fi AWDL and HTTPS. No central service, internet access, Apple ID
or persistent change to system AirDrop configuration is used by this receiver.

## Discovery and protocol

Startup checks Bluetooth availability, finds the scoped link-local address of
`awdl0`, and binds only that address. Native DNS-SD advertises `_airdrop._tcp`
with AWDL enabled and the protocol's Everyone/discovery flags. A random service
instance is used; `/Discover` returns `Peerline <short-name>`. An ephemeral
self-signed TLS certificate is generated for each session. The advertisement
child process and connection tasks are scoped to the receiver lifetime.

The original Rust implementation follows protocol observations documented by
[OpenDrop](https://github.com/seemoo-lab/opendrop) and the
[AWDL security research](https://www.usenix.org/system/files/sec19-stute.pdf):
binary plist `/Discover`, `/Ask`, and chunked CPIO `/Upload` over HTTPS.
No OpenDrop code or executable is bundled. Apple does not publish this protocol
as a supported cross-platform API; current Apple release compatibility is not
established by these tests.

## Consent and storage

Sender names are unverified and printed with control characters escaped. Each
batch requires a local `y` confirmation within 40 seconds, defaulting to reject.
There is no Contacts-only or Apple ID authentication and no automatic accept.
The approval is bound to the peer's link-local IP, single use and expires after
60 seconds. It is not a cryptographic sender identity. One pending offer or
upload is allowed; a maximum of eight TLS connections are served concurrently.

Uploads accept gzip or plain ASCII CPIO (odc/newc), with a default 1 GiB cap on
compressed and expanded file data, bounded metadata, 256 approved roots and
10,000 archive entries. HTTP headers, metadata bodies, TLS handshakes and upload
progress have deadlines. An upload may take up to five minutes with at most
15 seconds between body frames. Cancelled extraction stops and removes staging.

Only approved top-level names are accepted. Absolute/traversal paths, links,
duplicate entries, device nodes, unexpected roots and URL shares are rejected.
Archive ownership, permissions and executable bits are not restored. Private
staging prevents partial files becoming final output; a verified batch is moved
into a freshly reserved `AirDrop-*` directory without replacing existing files.
Gzip integrity and archive structure are checked. Unlike Peerline transfers,
AirDrop does not provide the shared-code PAKE or Peerline's manifest BLAKE3
verification; it relies on this separate HTTPS protocol.

## Verification

Automated tests cover consent, rejected/expired approvals, peer binding,
single-use uploads, traversal/link/size rejection, native-libarchive odc/newc
gzip fixtures, corrupt/truncated archives, and an actual local HTTPS
discover/approve/chunked-upload exchange. These tests do not use Apple senders.
Local macOS startup, AWDL service registration, idle exit and advertisement
process cleanup were also exercised.

To repeat the native CLI/AWDL probe after `cargo build -p peerline-cli`, run
`python3 crates/peerline-cli/tests/airdrop_macos.py` on an authorized Mac with
Bluetooth and Wi-Fi enabled. It uses temporary output, tests that pretyped keys
cannot approve an offer, then confirms and checks an actual file upload.

Physical acceptance remains: open the AirDrop picker on a nearby iPhone/iPad
and Mac, confirm Peerline appears, accept/reject photos and Files/Finder folders,
compare file contents, interrupt an upload, and check cleanup with Wi-Fi and
Bluetooth off. Record OS versions. Linux/Windows return an unsupported-platform
error rather than silently starting an incompatible receiver.
