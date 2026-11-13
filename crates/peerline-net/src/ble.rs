//! Offline BLE GATT transport. No discovery record or file bytes use IP.
#[cfg(any(target_os = "macos", target_os = "linux", target_os = "windows"))]
#[path = "ble/native.rs"]
mod native;
#[cfg(any(target_os = "macos", target_os = "linux", target_os = "windows"))]
pub use native::{BleReceiver, ensure_hardware, send};
