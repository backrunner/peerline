use anyhow::Context;
use ble_peripheral_rust::{
    Peripheral as Server, PeripheralImpl,
    gatt::{
        characteristic::Characteristic,
        peripheral_event::{
            PeripheralEvent, ReadRequestResponse, RequestResponse, WriteRequestResponse,
        },
        properties::{AttributePermission, CharacteristicProperty},
        service::Service,
    },
};
use btleplug::{
    api::{Central, CentralState, Manager as _, Peripheral as _, ScanFilter, WriteType},
    platform::Manager,
};
use peerline_core::{ConnectionRoute, HumanName, PeerlineEvent, TransferStage};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc,
    time::{self, Instant},
};
use uuid::Uuid;

use crate::{ReceivedTransfer, RecvOptions, SendOptions, SentTransfer, direct};

// Fits ATT MTU 23 on every supported adapter. One byte is reserved for read status.
const CHUNK: usize = 19;
const BUFFER: usize = 128 * 1024;
const INPUT: Uuid = Uuid::from_u128(0x952e471c_291b_4dc7_9865_e377b840ac01);
const OUTPUT: Uuid = Uuid::from_u128(0x952e471c_291b_4dc7_9865_e377b840ac02);
const IO_TIMEOUT: Duration = Duration::from_secs(30);
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);

// The short, public receive name selects the service. The code remains secret and
// is never used to derive an advertised value (avoids an offline code oracle).
fn service_for_name(name: &HumanName) -> Uuid {
    let hash = blake3::derive_key("peerline BLE service v1", name.as_str().as_bytes());
    Uuid::from_bytes(hash[..16].try_into().expect("fixed size"))
}

pub async fn ensure_hardware() -> anyhow::Result<()> {
    powered_adapter().await.map(|_| ())
}

fn powered_index(states: &[CentralState]) -> anyhow::Result<Option<usize>> {
    anyhow::ensure!(
        !states.is_empty(),
        "No Bluetooth adapter; BLE was not started"
    );
    if let Some(index) = states.iter().position(|s| *s == CentralState::PoweredOn) {
        return Ok(Some(index));
    }
    anyhow::ensure!(
        states.contains(&CentralState::Unknown),
        "Bluetooth is off; BLE was not started"
    );
    Ok(None)
}

async fn powered_adapter() -> anyhow::Result<btleplug::platform::Adapter> {
    time::timeout(Duration::from_secs(5), async {
        let manager = Manager::new()
            .await
            .context("Bluetooth unavailable; BLE was not started")?;
        let adapters = manager.adapters().await?;
        // Some platforms report Unknown briefly while initializing their manager.
        loop {
            let mut states = Vec::with_capacity(adapters.len());
            for adapter in &adapters {
                states.push(adapter.adapter_state().await?);
            }
            if let Some(index) = powered_index(&states)? {
                return Ok(adapters[index].clone());
            }
            time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .context("No usable Bluetooth adapter or permission; BLE was not started")?
}

/// An explicitly enabled receive session. Hardware is checked before creating the peripheral.
pub struct BleReceiver {
    peripheral: Server,
    events: mpsc::Receiver<PeripheralEvent>,
    service: Uuid,
}

impl BleReceiver {
    pub async fn bind(name: &HumanName) -> anyhow::Result<Self> {
        let _adapter = powered_adapter().await?;
        time::timeout(IO_TIMEOUT, Self::bind_inner(service_for_name(name)))
            .await
            .context("BLE initialization timed out; check Bluetooth permissions and power")?
    }

    async fn bind_inner(service: Uuid) -> anyhow::Result<Self> {
        let (tx, events) = mpsc::channel(64);
        let mut peripheral = Server::new(tx).await?;
        while !peripheral.is_powered().await? {
            time::sleep(Duration::from_millis(100)).await;
        }
        peripheral
            .add_service(&Service {
                uuid: service,
                primary: true,
                characteristics: vec![
                    Characteristic {
                        uuid: INPUT,
                        properties: vec![CharacteristicProperty::Write],
                        permissions: vec![AttributePermission::Writeable],
                        ..Default::default()
                    },
                    Characteristic {
                        uuid: OUTPUT,
                        properties: vec![CharacteristicProperty::Read],
                        permissions: vec![AttributePermission::Readable],
                        ..Default::default()
                    },
                ],
            })
            .await?;
        peripheral.start_advertising("Peerline", &[service]).await?;
        while !peripheral.is_advertising().await? {
            time::sleep(Duration::from_millis(100)).await;
        }
        Ok(Self {
            peripheral,
            events,
            service,
        })
    }

    pub async fn ensure_available(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.events.is_closed(), "BLE adapter event stream closed");
        anyhow::ensure!(
            time::timeout(IO_TIMEOUT, self.peripheral.is_powered()).await??,
            "Bluetooth was powered off"
        );
        Ok(())
    }

    pub fn address(&self) -> String {
        self.service.to_string()
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        time::timeout(IO_TIMEOUT, self.peripheral.stop_advertising()).await??;
        Ok(())
    }

    /// Serve a single peer. Other centrals cannot interleave bytes into its stream.
    pub async fn receive(&mut self, options: RecvOptions) -> anyhow::Result<ReceivedTransfer> {
        serve(&mut self.events, self.service, options).await
    }
}

async fn serve(
    events: &mut mpsc::Receiver<PeripheralEvent>,
    service: Uuid,
    options: RecvOptions,
) -> anyhow::Result<ReceivedTransfer> {
    let (stream, bridge) = tokio::io::duplex(BUFFER);
    let (mut output, mut input) = tokio::io::split(bridge);
    let mut transfer = std::pin::pin!(direct::receive_transport(
        stream,
        format!("ble:{}", service),
        options
    ));
    let mut client: Option<String> = None;
    let mut pending = Vec::new();
    let mut buffer = [0; CHUNK];
    let mut outcome = None;
    let mut eof = false;
    let mut last_data = Instant::now();
    loop {
        if eof
            && pending.is_empty()
            && let Some(result) = outcome.take()
        {
            return result;
        }
        tokio::select! {
            result = &mut transfer, if outcome.is_none() => {
                // Failed authentication must release the session immediately.
                match result { Ok(value) => outcome = Some(Ok(value)), Err(error) => return Err(error) }
            }
            read = output.read(&mut buffer), if pending.is_empty() && !eof => {
                let count = read?;
                eof = count == 0;
                pending.extend_from_slice(&buffer[..count]);
            }
            _ = time::sleep_until(last_data + IDLE_TIMEOUT), if client.is_some() => {
                anyhow::bail!("BLE peer stopped transferring data for 120 seconds");
            }
            event = events.recv() => {
                match event.context("BLE adapter event stream closed")? {
                    PeripheralEvent::StateUpdate { is_powered: false } => anyhow::bail!("Bluetooth was powered off"),
                    PeripheralEvent::WriteRequest { request, value, offset, responder } => {
                        let valid = request.service == service && request.characteristic == INPUT
                            && offset == 0 && !value.is_empty() && value.len() <= CHUNK
                            && client.as_ref().is_none_or(|id| id == &request.client);
                        let response = if valid {
                            client.get_or_insert(request.client);
                            time::timeout(IO_TIMEOUT, input.write_all(&value)).await.context("BLE receive buffer stalled")??;
                            last_data = Instant::now();
                            RequestResponse::Success
                        } else { RequestResponse::RequestNotSupported };
                        let _ = responder.send(WriteRequestResponse { response });
                    }
                    PeripheralEvent::ReadRequest { request, offset, responder } => {
                        let valid = request.service == service && request.characteristic == OUTPUT
                            && offset == 0 && client.as_ref() == Some(&request.client);
                        let (value, response) = if valid {
                            let mut value = vec![u8::from(!pending.is_empty())];
                            value.extend_from_slice(&pending);
                            if !pending.is_empty() { last_data = Instant::now(); }
                            (value, RequestResponse::Success)
                        } else { (vec![], RequestResponse::RequestNotSupported) };
                        if responder.send(ReadRequestResponse { value, response }).is_ok() && valid { pending.clear(); }
                    }
                    _ => {}
                }
            }
        }
    }
}

pub async fn send(address: &str, options: SendOptions) -> anyhow::Result<SentTransfer> {
    let service = service_for_name(&HumanName::parse(address)?);
    let adapter = powered_adapter().await?;
    let _scan_cleanup = ScanCleanup(adapter.clone());
    time::timeout(
        IO_TIMEOUT,
        adapter.start_scan(ScanFilter {
            services: vec![service],
        }),
    )
    .await??;
    let found = time::timeout(IO_TIMEOUT, async {
        loop {
            for device in adapter.peripherals().await? {
                if device
                    .properties()
                    .await?
                    .is_some_and(|p| p.services.contains(&service))
                {
                    return Ok::<_, anyhow::Error>(device);
                }
            }
            time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    let _ = time::timeout(IO_TIMEOUT, adapter.stop_scan()).await;
    let device =
        found.context("BLE receiver not found within 30 seconds; keep both devices nearby")??;
    let _connection_cleanup = ConnectionCleanup(device.clone());
    let result = async {
        time::timeout(IO_TIMEOUT, device.connect()).await??;
        time::timeout(IO_TIMEOUT, device.discover_services()).await??;
        let chars = device.characteristics();
        let input = chars.iter().find(|c| c.service_uuid == service && c.uuid == INPUT).context("BLE input characteristic missing")?;
        let output = chars.iter().find(|c| c.service_uuid == service && c.uuid == OUTPUT).context("BLE output characteristic missing")?;
        if let Some(events) = &options.events {
            let _ = events.send(PeerlineEvent::StageChanged(TransferStage::Connecting(ConnectionRoute::Ble)));
        }
        let (stream, bridge) = tokio::io::duplex(BUFFER);
        let protocol = direct::send_transport(stream, format!("ble:{service}"), options);
        let io = NativeIo { device: &device, input, output };
        let pump = pump(&io, bridge);
        tokio::select! {
            result = protocol => result,
            result = pump => { result?; anyhow::bail!("BLE stream closed before transfer acknowledgement") }
        }
    }.await;
    let _ = time::timeout(IO_TIMEOUT, device.disconnect()).await;
    result
}

struct ScanCleanup(btleplug::platform::Adapter);
impl Drop for ScanCleanup {
    fn drop(&mut self) {
        let adapter = self.0.clone();
        tokio::spawn(async move {
            let _ = time::timeout(IO_TIMEOUT, adapter.stop_scan()).await;
        });
    }
}
struct ConnectionCleanup(btleplug::platform::Peripheral);
impl Drop for ConnectionCleanup {
    fn drop(&mut self) {
        let device = self.0.clone();
        tokio::spawn(async move {
            let _ = time::timeout(IO_TIMEOUT, device.disconnect()).await;
        });
    }
}

trait GattIo {
    async fn write(&self, bytes: &[u8]) -> anyhow::Result<()>;
    async fn read(&self) -> anyhow::Result<Vec<u8>>;
}

struct NativeIo<'a> {
    device: &'a btleplug::platform::Peripheral,
    input: &'a btleplug::api::Characteristic,
    output: &'a btleplug::api::Characteristic,
}
impl GattIo for NativeIo<'_> {
    async fn write(&self, bytes: &[u8]) -> anyhow::Result<()> {
        self.device
            .write(self.input, bytes, WriteType::WithResponse)
            .await?;
        Ok(())
    }
    async fn read(&self) -> anyhow::Result<Vec<u8>> {
        Ok(self.device.read(self.output).await?)
    }
}

async fn pump(io: &impl GattIo, mut bridge: tokio::io::DuplexStream) -> anyhow::Result<()> {
    let mut buffer = [0; CHUNK];
    let mut tick = time::interval(Duration::from_millis(15));
    tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let mut started = false;
    let mut last_data = Instant::now();
    loop {
        tokio::select! {
            read = bridge.read(&mut buffer) => {
                let count = read?;
                if count == 0 { return Ok::<(), anyhow::Error>(()); }
                time::timeout(IO_TIMEOUT, io.write(&buffer[..count])).await??;
                started = true;
                last_data = Instant::now();
            }
            _ = tick.tick(), if started => {
                let packet = time::timeout(IO_TIMEOUT, io.read()).await??;
                match packet.first() {
                    Some(0) if packet.len() == 1 => {}
                    Some(1) if (2..=CHUNK+1).contains(&packet.len()) => {
                        time::timeout(IO_TIMEOUT, bridge.write_all(&packet[1..])).await??;
                        last_data = Instant::now();
                    }
                    _ => anyhow::bail!("Invalid BLE response packet"),
                }
            }
            _ = time::sleep_until(last_data + IDLE_TIMEOUT) => anyhow::bail!("BLE peer stopped transferring data for 120 seconds"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ble_peripheral_rust::gatt::peripheral_event::PeripheralRequest;
    use peerline_core::{Compression, HumanCode, NodeId};
    use tokio::sync::oneshot;

    #[cfg(target_os = "macos")]
    #[tokio::test]
    #[ignore = "requires a powered, authorized local Bluetooth adapter"]
    async fn native_receiver_can_be_recreated_after_drop() {
        for _ in 0..2 {
            let mut receiver = BleReceiver::bind(&HumanName::generate()).await.unwrap();
            receiver.ensure_available().await.unwrap();
            receiver.close().await.unwrap();
            drop(receiver);
        }
    }

    #[test]
    fn missing_or_disabled_adapters_never_pass_startup_gate() {
        use CentralState::*;
        assert!(powered_index(&[]).is_err());
        assert!(powered_index(&[PoweredOff, PoweredOff]).is_err());
        assert_eq!(powered_index(&[Unknown, PoweredOff]).unwrap(), None);
        assert_eq!(powered_index(&[PoweredOff, PoweredOn]).unwrap(), Some(1));
    }

    #[test]
    fn short_public_name_selects_the_ble_service() {
        let name = HumanName::parse("frost-827").unwrap();
        assert_eq!(
            service_for_name(&name),
            service_for_name(&HumanName::parse("frost-827").unwrap())
        );
        assert_ne!(
            service_for_name(&name),
            service_for_name(&HumanName::parse("frost-828").unwrap())
        );
    }

    struct FakeGatt {
        events: mpsc::Sender<PeripheralEvent>,
        service: Uuid,
        client: String,
    }
    impl FakeGatt {
        fn request(&self, characteristic: Uuid) -> PeripheralRequest {
            PeripheralRequest {
                client: self.client.clone(),
                service: self.service,
                characteristic,
            }
        }
    }
    impl GattIo for FakeGatt {
        async fn write(&self, bytes: &[u8]) -> anyhow::Result<()> {
            let (tx, rx) = oneshot::channel();
            self.events
                .send(PeripheralEvent::WriteRequest {
                    request: self.request(INPUT),
                    value: bytes.to_vec(),
                    offset: 0,
                    responder: tx,
                })
                .await?;
            anyhow::ensure!(
                rx.await?.response == RequestResponse::Success,
                "write rejected"
            );
            Ok(())
        }
        async fn read(&self) -> anyhow::Result<Vec<u8>> {
            let (tx, rx) = oneshot::channel();
            self.events
                .send(PeripheralEvent::ReadRequest {
                    request: self.request(OUTPUT),
                    offset: 0,
                    responder: tx,
                })
                .await?;
            let response = rx.await?;
            anyhow::ensure!(
                response.response == RequestResponse::Success,
                "read rejected"
            );
            Ok(response.value)
        }
    }

    #[tokio::test]
    async fn encrypted_transfer_over_gatt_fragments_and_wrong_password() {
        for wrong_code in [false, true] {
            let source = tempfile::tempdir().unwrap();
            let destination = tempfile::tempdir().unwrap();
            // More than the bounded duplex buffer, with incompressible data.
            let bytes: Vec<u8> = (0..180_000).map(|_| rand::random()).collect();
            let file = source.path().join("payload.bin");
            std::fs::write(&file, &bytes).unwrap();
            let code = HumanCode::generate();
            let (tx, mut events) = mpsc::channel(64);
            let service = Uuid::new_v4();
            let io = FakeGatt {
                events: tx,
                service,
                client: "sender".into(),
            };
            let receiver = serve(
                &mut events,
                service,
                RecvOptions {
                    code: code.clone(),
                    destination: destination.path().to_path_buf(),
                    ..Default::default()
                },
            );
            let (stream, bridge) = tokio::io::duplex(BUFFER);
            let sender = direct::send_transport(
                stream,
                "ble:test".into(),
                SendOptions {
                    endpoint: "127.0.0.1:0".parse().unwrap(),
                    name: None,
                    code: if wrong_code {
                        HumanCode::generate()
                    } else {
                        code
                    },
                    source_id: NodeId::random(),
                    paths: vec![file],
                    compression: Compression::None,
                    events: None,
                },
            );
            if wrong_code {
                time::timeout(Duration::from_secs(15), async {
                    tokio::select! {
                        result = receiver => assert!(result.is_err()),
                        result = sender => assert!(result.is_err()),
                        result = pump(&io, bridge) => assert!(result.is_err()),
                    }
                })
                .await
                .unwrap();
                assert!(!destination.path().join("payload.bin").exists());
            } else {
                let (sent, received) = time::timeout(Duration::from_secs(30), async {
                    tokio::select! {
                        results = async { tokio::join!(sender, receiver) } => results,
                        result = pump(&io, bridge) => panic!("pump exited unexpectedly: {result:?}"),
                    }
                }).await.unwrap();
                assert_eq!(sent.unwrap().bytes, bytes.len() as u64);
                assert_eq!(received.unwrap().bytes, bytes.len() as u64);
                assert_eq!(
                    std::fs::read(destination.path().join("payload.bin")).unwrap(),
                    bytes
                );
            }
        }
    }

    #[tokio::test]
    async fn gatt_rejects_other_clients_and_oversized_writes() {
        let destination = tempfile::tempdir().unwrap();
        let (tx, mut events) = mpsc::channel(64);
        let service = Uuid::new_v4();
        let io = FakeGatt {
            events: tx.clone(),
            service,
            client: "first".into(),
        };
        let other = FakeGatt {
            events: tx,
            service,
            client: "other".into(),
        };
        let receiver = serve(
            &mut events,
            service,
            RecvOptions {
                destination: destination.path().to_path_buf(),
                ..Default::default()
            },
        );
        time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = receiver => panic!("incomplete stream must keep waiting"),
                _ = async {
                    assert!(io.write(&[0; CHUNK + 1]).await.is_err());
                    io.write(&[0]).await.unwrap();
                    assert!(other.write(&[0]).await.is_err());
                    assert!(other.read().await.is_err());
                    assert_eq!(io.read().await.unwrap(), [0]);
                } => {}
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn empty_polling_does_not_keep_an_abandoned_session_alive() {
        let destination = tempfile::tempdir().unwrap();
        let (tx, mut events) = mpsc::channel(64);
        let service = Uuid::new_v4();
        let io = FakeGatt {
            events: tx,
            service,
            client: "sender".into(),
        };
        let receiver = serve(
            &mut events,
            service,
            RecvOptions {
                destination: destination.path().to_path_buf(),
                ..Default::default()
            },
        );
        tokio::select! {
            result = receiver => assert!(result.unwrap_err().to_string().contains("120 seconds")),
            _ = async {
                io.write(&[0]).await.unwrap();
                for _ in 0..130 {
                    time::sleep(Duration::from_secs(1)).await;
                    if io.read().await.is_err() { return; }
                }
                panic!("empty polling extended the timeout");
            } => panic!("receiver must time out before polling stops"),
        }
    }
}
