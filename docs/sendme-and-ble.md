# sendme and Peerline BLE

Research date: 2026-11-13.

## Why sendme needs only a ticket

The upstream [sendme implementation](https://github.com/n0-computer/sendme/blob/main/src/main.rs)
uses Iroh and iroh-blobs, not Bluetooth. The sender imports the paths into a
content-addressed collection, starts a provider, and prints a BlobTicket.
The recipient decodes the ticket and downloads that collection.

[BlobTicket](https://docs.rs/iroh-blobs/latest/src/iroh_blobs/ticket.rs.html)
encodes the provider's public endpoint identity, an optional relay URL, direct
socket addresses, blob format and content hash. The identity authenticates the
endpoint; the hash identifies and verifies the requested content. A human or OS
machine name is unnecessary because the ticket carries the needed identifiers.
The ticket is not a short alias that must be looked up in a sendme account database.

There is server infrastructure in the default network path. Iroh attempts NAT
traversal and direct encrypted QUIC connections and can relay encrypted traffic
when direct connectivity fails. sendme exposes default, custom and disabled
relay modes, and configures address lookup using pkarr publishing and DNS.
Thus "peer-to-peer" does not mean "never contacts any server". Supplying usable
direct addresses and disabling relays can avoid the relay, but cannot guarantee
connectivity through arbitrary NATs. See the
[upstream README](https://github.com/n0-computer/sendme#readme).

Peerline's existing name is a human-chosen discovery label, not the OS hostname.
Its name/code lookup and sendme's self-contained ticket are different UX choices.
The ticket is a long `blob`-prefixed base32 encoding, meant to be copied rather
than memorized. Peerline deliberately keeps short names/codes instead; no Iroh
or sendme wire compatibility is added. Explicit `--copy-code` copies expire on
a best-effort 30-second timer; see README for platform/history limitations.

## BLE channel

`recv --ble` prints a short public name. `send --ble NAME PATH... --code CODE`
derives the same 128-bit service UUID from that name with BLAKE3's
`peerline BLE service v1` domain and scans for it. Users do not copy UUIDs.
The name/UUID is a locator, not an authentication secret. The code is never placed
in advertisements or characteristics. Existing OPAQUE authentication is still
required before encrypted file data can be accepted. The code does not influence
the advertised UUID, which avoids creating an offline code-guessing oracle.

Bluetooth is initialized only for an explicit BLE/AirDrop request. Before BLE
scanning or peripheral creation, adapters are enumerated and must report
PoweredOn. No adapters or only powered-off adapters cause immediate failure;
unknown initialization state is bounded to five seconds. No IP fallback starts.

Two fixed characteristic UUIDs identify the byte stream inside that service:

| Characteristic | UUID | Operation |
| --- | --- | --- |
| Input | `952e471c-291b-4dc7-9865-e377b840ac01` | Central writes with response |
| Output | `952e471c-291b-4dc7-9865-e377b840ac02` | Central reads |

Input writes contain 1–19 stream bytes. Output reads return `00` while empty,
or `01` followed by 1–19 stream bytes. These fit the minimum ATT MTU (23).
GATT ordering and acknowledgements provide stream ordering; Peerline's existing
framing, protocol version, authenticated chunk sequence and content hashes
protect the transfer. The application completion ACK, not a GATT write response,
determines transfer success. That ACK is authenticated with the receiver's
session key in protocol version 4; forged plaintext ACKs are rejected. Both
Peerline peers must use this protocol version.

The peripheral binds the active stream to the first central's native client ID.
Other clients, wrong characteristics, offsets and oversized writes are rejected.
Each direction has a bounded 128 KiB stream buffer. Native operations have a
30-second deadline; a session with no data progress expires after 120 seconds.
Empty polling cannot extend that session timeout. Receiver idle exit also uses
the existing `--idle-timeout-minutes` option.

The client uses btleplug, and the server uses ble-peripheral-rust. Both expose
native macOS/CoreBluetooth, Linux/BlueZ and Windows/WinRT implementations.
The CLI embeds macOS Bluetooth usage descriptions; Linux builds vendor libdbus
so musl release assets do not gain a shared-library requirement.
The peripheral dependency has local lifecycle fixes: Linux does not enable a
powered-off default adapter, and macOS closes its manager thread when dropped
and supports a later receive session in the same process.

## Validation boundary

The simulated GATT tests exercise actual input/read request handlers and the
same stream pump used by hardware, including data larger than the bounded
buffer, encryption/authentication, final file equality and client isolation.
They cannot establish radio compatibility, OS permission handling, practical
range, throughput or reliable reconnect on real adapters. Those require a
two-device run with networking disabled. No BLE hardware interoperability claim
is implied by compilation or simulation.

For manual acceptance: send a small file and a directory, compare BLAKE3 hashes,
reject a wrong code, disconnect during a transfer and resend, power Bluetooth
off, and test Ctrl-C/idle exit. Confirm another central cannot consume response
bytes while a session is active. Record both OS versions and adapters.
