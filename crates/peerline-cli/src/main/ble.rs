use super::*;

pub(super) async fn recv(args: RecvArgs) -> anyhow::Result<()> {
    let config = ConfigStore::user_default()?.load()?;
    let (name, code) = resolve_recv_identity(config.name, args.first, args.second)?;
    let mut receiver = peerline_net::ble::BleReceiver::bind(&name).await?;
    let result = async {
        println!("peerline recv --ble\nname: {name}\ncode: {code}");
        if args.copy_code {
            clipboard::copy_code(&code).await?;
        }
        println!("Offline Bluetooth LE; keep both devices nearby. Ctrl-C to stop.");
        std::io::stdout().flush()?;
        let (events, mut activity) = mpsc::unbounded_channel();
        let options = RecvOptions {
            name,
            code,
            destination: std::env::current_dir()?,
            overwrite: args.overwrite,
            events: Some(events),
            ..Default::default()
        };
        let idle = recv_idle_timeout(args.idle_timeout_minutes);
        loop {
            receiver.ensure_available().await?;
            let transfer = receiver.receive(options.clone());
            tokio::pin!(transfer);
            let mut last_activity = tokio::time::Instant::now();
            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => return Ok(()),
                    event = activity.recv() => {
                        if event.is_some() { last_activity = tokio::time::Instant::now(); }
                    }
                    _ = tokio::time::sleep_until(last_activity + idle.unwrap_or(Duration::from_secs(86400))), if idle.is_some() => {
                        println!("BLE receiver idle timeout");
                        return Ok(());
                    }
                    result = &mut transfer => {
                        match result {
                            Ok(received) => println!("received {} files ({} bytes) over BLE", received.files, received.bytes),
                            Err(error) => {
                                eprintln!("BLE transfer failed: {error:#}");
                                tokio::time::sleep(Duration::from_millis(200)).await;
                            },
                        }
                        break;
                    }
                }
            }
        }
    }.await;
    let closed = receiver.close().await;
    result.and(closed)
}

pub(super) async fn send(args: SendArgs) -> anyhow::Result<()> {
    if args.args.len() < 2 {
        anyhow::bail!("send --ble <name> <path...> --code <code>");
    }
    let code = HumanCode::parse(
        args.code
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("BLE send requires --code"))?,
    )?;
    let mut ui = spawn_send_tui(
        "BLE",
        args.args[0].clone(),
        code.clone(),
        "offline Bluetooth LE".into(),
        false,
    );
    let options = SendOptions {
        endpoint: SocketAddr::from(([127, 0, 0, 1], 0)), // Unused by the BLE byte stream.
        name: None,
        code,
        source_id: ConfigStore::user_default()?.node_id()?,
        paths: args.args[1..].iter().map(PathBuf::from).collect(),
        compression: args.compression.into(),
        events: ui.events.clone(),
    };
    let result = tokio::select! {
        _ = tokio::signal::ctrl_c() => Ok(TaskOutcome::Quit),
        result = wait_with_quit(
        peerline_net::ble::send(&args.args[0], options),
        &mut ui.quit_rx,
    ) => result,
    };
    if let Some(events) = &ui.events {
        let _ = events.send(PeerlineEvent::Shutdown);
    }
    if let Some(task) = ui.task {
        let _ = task.await;
    }
    if let TaskOutcome::Completed(sent) = result? {
        println!("sent {} files ({} bytes) over BLE", sent.files, sent.bytes);
    }
    Ok(())
}
