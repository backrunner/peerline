# Peerline patches to ble-peripheral-rust 0.2.0

Source: crates.io `ble-peripheral-rust` 0.2.0; upstream MIT license is retained.

- Linux initialization requires the default adapter to be powered already. It
  must not change the user's Bluetooth power setting, including on hosts with
  multiple adapters.
- Each macOS peripheral owns a manager thread. Closing its command channel ends
  that thread and stops advertising, rather than busy-looping on a closed
  channel. A later peripheral instance can initialize normally; the upstream
  global OnceCell made that second instance unusable.

These small platform fixes are separate from Peerline's wire protocol. Native
hardware interoperability remains subject to the acceptance tests in
`docs/sendme-and-ble.md`.
