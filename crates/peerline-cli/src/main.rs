#[path = "main/terminal.rs"]
mod terminal;
#[path = "main/wait.rs"]
mod wait;

use clap::{Args, Parser, Subcommand, ValueEnum};
use peerline_core::{
    Compression, ConfigStore, DEFAULT_DIRECT_PORT, DEFAULT_DIRECT_PORT_WINDOW, HumanCode,
    HumanName, NodeId, PeerlineEvent, TransferId, TransferStage, parse_ip_endpoint,
};
use peerline_net::{
    Candidate, Libp2pRecvOptions, Libp2pSendOptions, RecvOptions, RouteKind, SendOptions,
    bind_direct_listener,
};
use rand::Rng;
use std::{
    io::IsTerminal,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};
use terminal::{init_tracing, register_activity_log_sender, spawn_send_tui, spawn_terminal_ui};
use tokio::sync::{mpsc, watch};
use wait::{
    RecvOutcome, RetryDecision, TaskOutcome, drain_retry_signals, format_duration,
    parse_idle_timeout_minutes, recv_idle_timeout, spawn_event_fanout, wait_for_receiver,
    wait_for_recv_activity, wait_for_retry_or_quit, wait_with_quit,
};

#[derive(Debug, Parser)]
#[command(
    name = "peerline",
    version,
    about = "P2P post-quantum encrypted file transfer"
)]
struct Cli {
    #[arg(long, global = true)]
    debug: bool,
    #[command(subcommand)]
    command: Command,
}

const DEFAULT_RECV_IDLE_TIMEOUT_MINUTES: f64 = 10.0;
const DEFAULT_RETRY_ATTEMPTS: usize = 5;

#[derive(Debug, Subcommand)]
enum Command {
    Recv(RecvArgs),
    Send(SendArgs),
    Set(SetArgs),
}

#[derive(Debug, Args)]
struct RecvArgs {
    #[arg(value_name = "NAME_OR_CODE")]
    first: Option<String>,
    #[arg(value_name = "CODE")]
    second: Option<String>,
    #[arg(long, default_value_t = DEFAULT_DIRECT_PORT)]
    port: u16,
    #[arg(long)]
    overwrite: bool,
    #[arg(long)]
    no_tui: bool,
    #[arg(long, hide = true)]
    allow_relay_fallback: bool,
    #[arg(long)]
    no_relay_fallback: bool,
    #[arg(
        long,
        default_value_t = DEFAULT_RECV_IDLE_TIMEOUT_MINUTES,
        value_parser = parse_idle_timeout_minutes
    )]
    idle_timeout_minutes: f64,
}

#[derive(Debug, Args)]
struct SendArgs {
    #[arg(value_name = "ARGS", required = true)]
    args: Vec<String>,
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    code: Option<String>,
    #[arg(long, value_enum, default_value_t = CompressionArg::Auto)]
    compression: CompressionArg,
    #[arg(long, hide = true)]
    allow_relay_fallback: bool,
    #[arg(long)]
    no_relay_fallback: bool,
    #[arg(long, default_value_t = DEFAULT_RETRY_ATTEMPTS, value_parser = parse_retry_attempts)]
    retry_attempts: usize,
}

#[derive(Debug, Args)]
struct SetArgs {
    #[command(subcommand)]
    command: SetCommand,
}

#[derive(Debug, Subcommand)]
enum SetCommand {
    Name { name: String },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CompressionArg {
    Auto,
    None,
    Zstd,
    Lzma,
}

impl From<CompressionArg> for Compression {
    fn from(value: CompressionArg) -> Self {
        match value {
            CompressionArg::Auto => Compression::Auto,
            CompressionArg::None => Compression::None,
            CompressionArg::Zstd => Compression::Zstd,
            CompressionArg::Lzma => Compression::Lzma,
        }
    }
}

fn parse_retry_attempts(value: &str) -> Result<usize, String> {
    let attempts = value
        .parse::<usize>()
        .map_err(|error| format!("invalid retry attempt count: {error}"))?;
    if attempts == 0 {
        return Err("retry attempts must be at least 1".into());
    }
    Ok(attempts)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.debug);
    match cli.command {
        Command::Recv(args) => recv(args).await,
        Command::Send(args) => send(args).await,
        Command::Set(args) => set(args),
    }
}

async fn recv(args: RecvArgs) -> anyhow::Result<()> {
    let store = ConfigStore::user_default()?;
    let config = store.load()?;
    let (name, code) = resolve_recv_identity(config.name, args.first, args.second)?;
    let _node_id = store.node_id()?;
    let idle_timeout = recv_idle_timeout(args.idle_timeout_minutes);

    let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), args.port);
    let (listener, actual_bind) = bind_direct_listener(bind).await?;
    let destination = std::env::current_dir()?;

    println!("peerline recv");
    println!("name: {name}");
    println!("code: {code}");
    println!("direct: {actual_bind}");
    println!("waiting for transfers over direct TCP or libp2p...");
    match idle_timeout {
        Some(timeout) => println!(
            "idle timeout: {} (change with --idle-timeout-minutes)",
            format_duration(timeout)
        ),
        None => println!("idle timeout: disabled"),
    }

    let (network_events, network_event_rx) = mpsc::unbounded_channel();
    let (activity_tx, mut activity_rx) = mpsc::unbounded_channel();
    let (tui_sender, tui_task, mut quit_rx) = if !args.no_tui && std::io::stdout().is_terminal() {
        let (sender, receiver) = mpsc::unbounded_channel();
        register_activity_log_sender(&sender);
        let (quit_tx, quit_rx) = watch::channel(false);
        let view = peerline_tui::RecvView {
            name: name.clone(),
            code: code.clone(),
            bind: actual_bind.to_string(),
            route_status: "direct TCP ready; libp2p DHT/mDNS/relay/WebRTC ready".into(),
            stage: peerline_core::TransferStage::Discovering,
            progress: None,
        };
        let task = spawn_terminal_ui(peerline_tui::render_once_with_quit(
            view,
            receiver,
            Some(quit_tx),
        ));
        (Some(sender), Some(task), Some(quit_rx))
    } else {
        (None, None, None)
    };
    let event_fanout = spawn_event_fanout(network_event_rx, tui_sender, activity_tx);
    let events = Some(network_events.clone());
    if code.is_low_entropy() {
        tracing::warn!("code entropy looks low; generated codes are safer on public networks");
    }
    let discovery = peerline_net::DiscoveryConfig {
        allow_relay_data_fallback: relay_fallback_enabled(
            args.allow_relay_fallback,
            args.no_relay_fallback,
        ),
        ..Default::default()
    };

    let mut transfers = 0usize;
    let mut files = 0usize;
    let mut bytes = 0u64;

    loop {
        let _ = network_events.send(PeerlineEvent::Message(
            "ready for the next transfer".to_string(),
        ));
        let direct_fut = Box::pin(peerline_net::recv_once_bound(
            &listener,
            RecvOptions {
                name: name.clone(),
                code: code.clone(),
                bind: actual_bind,
                destination: destination.clone(),
                overwrite: args.overwrite,
                events: events.clone(),
            },
        ));
        let libp2p_fut = Box::pin(peerline_net::recv_libp2p(Libp2pRecvOptions {
            name: name.clone(),
            code: code.clone(),
            direct_bind: actual_bind,
            destination: destination.clone(),
            overwrite: args.overwrite,
            discovery: discovery.clone(),
            events: events.clone(),
        }));

        match wait_for_recv_activity(
            wait_for_receiver(direct_fut, libp2p_fut),
            &mut quit_rx,
            &mut activity_rx,
            idle_timeout,
        )
        .await
        {
            Ok(RecvOutcome::Completed(received)) => {
                transfers += 1;
                files += received.files;
                bytes += received.bytes;
                if tui_task.is_none() {
                    println!(
                        "received {} file(s), {} bytes from {}",
                        received.files, received.bytes, received.peer
                    );
                }
                let _ =
                    network_events.send(PeerlineEvent::StageChanged(TransferStage::Discovering));
            }
            Ok(RecvOutcome::Quit) => break,
            Ok(RecvOutcome::IdleTimeout) => {
                let message = idle_timeout
                    .map(|timeout| format!("idle for {}; exiting", format_duration(timeout)))
                    .unwrap_or_else(|| "receiver idle timeout reached; exiting".into());
                let _ = network_events.send(PeerlineEvent::Message(message));
                let _ = network_events.send(PeerlineEvent::Shutdown);
                break;
            }
            Err(error) => {
                let _ = network_events.send(PeerlineEvent::StageChanged(TransferStage::Failed(
                    error.to_string(),
                )));
                drop(events);
                drop(network_events);
                let _ = event_fanout.await;
                if let Some(task) = tui_task {
                    let _ = task.await;
                }
                return Err(error);
            }
        }
    }

    let _ = network_events.send(PeerlineEvent::Shutdown);
    drop(events);
    drop(network_events);
    let _ = event_fanout.await;
    if let Some(task) = tui_task {
        let _ = task.await;
    }
    println!(
        "receiver stopped after {} transfer(s), {} file(s), {} bytes",
        transfers, files, bytes
    );
    Ok(())
}

async fn send(args: SendArgs) -> anyhow::Result<()> {
    if let Some(target) = direct_endpoint_arg(&args) {
        return send_direct_mode(args, target).await;
    }

    send_named_mode(args).await
}

async fn send_direct_mode(args: SendArgs, target: DirectTarget) -> anyhow::Result<()> {
    let retry_attempts = args.retry_attempts;
    let source_id = ConfigStore::user_default()?.node_id()?;
    let code = match args.code {
        Some(code) => HumanCode::parse(code)?,
        None => {
            let code = rpassword::prompt_password("code: ")?;
            HumanCode::parse(code)?
        }
    };
    let paths = args
        .args
        .iter()
        .skip(1)
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    if paths.is_empty() {
        anyhow::bail!("send <ip> requires at least one file or folder path");
    }
    let (target_label, target_value) = match target {
        DirectTarget::Exact(endpoint) => ("endpoint", endpoint.to_string()),
        DirectTarget::Ip(ip) => ("ip", ip.to_string()),
    };
    let mut ui = spawn_send_tui(
        target_label,
        target_value,
        code.clone(),
        match target {
            DirectTarget::Exact(endpoint) => format!("dialing {}", endpoint),
            DirectTarget::Ip(_) => format!(
                "probing direct ports {}..{}",
                DEFAULT_DIRECT_PORT,
                DEFAULT_DIRECT_PORT.saturating_add(DEFAULT_DIRECT_PORT_WINDOW.saturating_sub(1))
            ),
        },
        true,
    );
    let compression = args.compression.into();
    let mut round = 1usize;

    loop {
        drain_retry_signals(&mut ui.retry_rx);
        if round > 1
            && let Some(sender) = ui.events.as_ref()
        {
            let _ = sender.send(PeerlineEvent::StageChanged(TransferStage::Discovering));
            let _ = sender.send(PeerlineEvent::Message(format!(
                "retrying send round {round}"
            )));
        }
        let send_future = direct_send_with_retries(
            target,
            code.clone(),
            source_id,
            paths.clone(),
            compression,
            retry_attempts,
            ui.events.clone(),
        );

        match wait_with_quit(send_future, &mut ui.quit_rx).await {
            Ok(TaskOutcome::Completed(sent)) => {
                if let Some(task) = ui.task {
                    let _ = task.await;
                }
                println!(
                    "sent {} file(s), {} bytes to {}",
                    sent.files, sent.bytes, sent.endpoint
                );
                return Ok(());
            }
            Ok(TaskOutcome::Quit) => {
                if let Some(task) = ui.task {
                    let _ = task.await;
                }
                return Ok(());
            }
            Err(error) => {
                if let Some(sender) = ui.events.as_ref() {
                    let _ = sender.send(PeerlineEvent::StageChanged(TransferStage::Failed(
                        format!(
                            "failed after {retry_attempts} attempt(s): {error}; press r to retry or q to quit"
                        ),
                    )));
                } else {
                    return Err(error);
                }

                match wait_for_retry_or_quit(&mut ui.quit_rx, &mut ui.retry_rx).await {
                    RetryDecision::Retry => {
                        round += 1;
                        continue;
                    }
                    RetryDecision::Quit => {
                        if let Some(task) = ui.task {
                            let _ = task.await;
                        }
                        return Err(error);
                    }
                }
            }
        }
    }
}

async fn send_named_mode(args: SendArgs) -> anyhow::Result<()> {
    let retry_attempts = args.retry_attempts;
    let source_id = ConfigStore::user_default()?.node_id()?;
    let compression = args.compression.into();
    let allow_relay_fallback =
        relay_fallback_enabled(args.allow_relay_fallback, args.no_relay_fallback);
    let (name, code, paths) = resolve_named_send(args)?;
    let mut ui = spawn_send_tui(
        "peer",
        name.to_string(),
        code.clone(),
        "discovering routes through rendezvous, DHT, and mDNS...".into(),
        true,
    );
    if ui.events.is_none() {
        println!("discovering {name} through rendezvous, DHT, and mDNS...");
    }
    if code.is_low_entropy() {
        tracing::warn!("code entropy looks low; generated codes are safer on public networks");
    }
    let discovery = peerline_net::DiscoveryConfig {
        allow_relay_data_fallback: allow_relay_fallback,
        ..Default::default()
    };
    let mut round = 1usize;

    loop {
        drain_retry_signals(&mut ui.retry_rx);
        if round > 1
            && let Some(sender) = ui.events.as_ref()
        {
            let _ = sender.send(PeerlineEvent::StageChanged(TransferStage::Discovering));
            let _ = sender.send(PeerlineEvent::Message(format!(
                "retrying send round {round}"
            )));
        }

        match named_send_with_retries(
            NamedSendPlan {
                name: name.clone(),
                code: code.clone(),
                source_id,
                paths: paths.clone(),
                compression,
                discovery: discovery.clone(),
                events: ui.events.clone(),
                retry_attempts,
            },
            &mut ui.quit_rx,
        )
        .await
        {
            Ok(TaskOutcome::Completed(sent)) => {
                if let Some(task) = ui.task {
                    let _ = task.await;
                }
                println!(
                    "sent {} file(s), {} bytes to {}",
                    sent.files, sent.bytes, sent.endpoint
                );
                return Ok(());
            }
            Ok(TaskOutcome::Quit) => {
                if let Some(task) = ui.task {
                    let _ = task.await;
                }
                return Ok(());
            }
            Err(error) => {
                if let Some(sender) = ui.events.as_ref() {
                    let _ = sender.send(PeerlineEvent::StageChanged(TransferStage::Failed(
                        format!(
                            "failed after {retry_attempts} attempt(s): {error}; press r to retry or q to quit"
                        ),
                    )));
                } else {
                    return Err(error);
                }

                match wait_for_retry_or_quit(&mut ui.quit_rx, &mut ui.retry_rx).await {
                    RetryDecision::Retry => {
                        round += 1;
                        continue;
                    }
                    RetryDecision::Quit => {
                        if let Some(task) = ui.task {
                            let _ = task.await;
                        }
                        return Err(error);
                    }
                }
            }
        }
    }
}

async fn direct_send_with_retries(
    target: DirectTarget,
    code: HumanCode,
    source_id: NodeId,
    paths: Vec<PathBuf>,
    compression: Compression,
    retry_attempts: usize,
    events: Option<mpsc::UnboundedSender<PeerlineEvent>>,
) -> anyhow::Result<peerline_net::SentTransfer> {
    emit_send_message(&events, "building archive from selected paths");
    let archive = peerline_transfer::create_archive(&paths, compression)?;
    let transfer_id = TransferId::random();
    let mut last_error = None;

    for attempt in 1..=retry_attempts {
        if attempt > 1 {
            let delay = retry_delay(attempt);
            emit_send_message(
                &events,
                format!(
                    "retrying attempt {attempt}/{retry_attempts} in {}",
                    format_duration(delay)
                ),
            );
            tokio::time::sleep(delay).await;
        }

        let result = match target {
            DirectTarget::Exact(endpoint) => {
                peerline_net::send_prebuilt_direct(
                    SendOptions {
                        endpoint,
                        name: None,
                        code: code.clone(),
                        source_id,
                        paths: paths.clone(),
                        compression,
                        events: events.clone(),
                    },
                    clone_archive(&archive)?,
                    transfer_id,
                )
                .await
            }
            DirectTarget::Ip(ip) => {
                peerline_net::send_prebuilt_direct_probe(
                    SendOptions {
                        endpoint: SocketAddr::new(ip, DEFAULT_DIRECT_PORT),
                        name: None,
                        code: code.clone(),
                        source_id,
                        paths: paths.clone(),
                        compression,
                        events: events.clone(),
                    },
                    clone_archive(&archive)?,
                    transfer_id,
                )
                .await
            }
        };

        match result {
            Ok(sent) => return Ok(sent),
            Err(error) if is_fatal_error(&error) => return Err(error),
            Err(error) => last_error = Some(error),
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("send failed without an error")))
}

struct NamedSendPlan {
    name: HumanName,
    code: HumanCode,
    source_id: NodeId,
    paths: Vec<PathBuf>,
    compression: Compression,
    discovery: peerline_net::DiscoveryConfig,
    events: Option<mpsc::UnboundedSender<PeerlineEvent>>,
    retry_attempts: usize,
}

async fn named_send_with_retries(
    plan: NamedSendPlan,
    quit_rx: &mut Option<watch::Receiver<bool>>,
) -> anyhow::Result<TaskOutcome<peerline_net::SentTransfer>> {
    emit_send_message(&plan.events, "building archive from selected paths");
    let archive = peerline_transfer::create_archive(&plan.paths, plan.compression)?;
    let transfer_id = TransferId::random();
    let mut last_error = None;

    for attempt in 1..=plan.retry_attempts {
        if attempt > 1 {
            let delay = retry_delay(attempt);
            emit_send_message(
                &plan.events,
                format!(
                    "retrying attempt {attempt}/{} in {}",
                    plan.retry_attempts,
                    format_duration(delay)
                ),
            );
            tokio::time::sleep(delay).await;
        }

        match named_send_attempt(&plan, clone_archive(&archive)?, transfer_id, quit_rx).await {
            Ok(TaskOutcome::Completed(sent)) => return Ok(TaskOutcome::Completed(sent)),
            Ok(TaskOutcome::Quit) => return Ok(TaskOutcome::Quit),
            Err(error) if is_fatal_error(&error) => return Err(error),
            Err(error) => last_error = Some(error),
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("send failed without an error")))
}

async fn named_send_attempt(
    plan: &NamedSendPlan,
    archive: peerline_transfer::Archive,
    transfer_id: TransferId,
    quit_rx: &mut Option<watch::Receiver<bool>>,
) -> anyhow::Result<TaskOutcome<peerline_net::SentTransfer>> {
    let candidates = loop {
        tracing::info!(peer = %plan.name, "discovering routes through rendezvous, DHT, and mDNS");
        let discovery_future = peerline_net::discovery::discover_peer_candidates(
            &plan.name,
            &plan.code,
            plan.discovery.clone(),
        );
        match wait_with_quit(discovery_future, quit_rx).await {
            Ok(TaskOutcome::Completed(candidates)) if !candidates.is_empty() => break candidates,
            Ok(TaskOutcome::Completed(_)) => {
                tracing::error!(
                    peer = %plan.name,
                    "could not discover a route yet; still searching"
                );
            }
            Ok(TaskOutcome::Quit) => {
                return Ok(TaskOutcome::Quit);
            }
            Err(error) => {
                return Err(error);
            }
        }
    };

    let mut last_error = None;
    for candidate in candidates {
        if !route_allowed(&candidate.route, plan.discovery.allow_relay_data_fallback) {
            continue;
        }

        if let Some(sender) = plan.events.as_ref() {
            let _ = sender.send(PeerlineEvent::Message(format!(
                "trying {} via {}",
                candidate.peer_id,
                route_label(&candidate.route)
            )));
        }

        let attempt = send_candidate(&candidate, plan, clone_archive(&archive)?, transfer_id);

        match wait_with_quit(attempt, quit_rx).await {
            Ok(TaskOutcome::Completed(sent)) => {
                return Ok(TaskOutcome::Completed(sent));
            }
            Ok(TaskOutcome::Quit) => {
                return Ok(TaskOutcome::Quit);
            }
            Err(error) => {
                if let Some(sender) = plan.events.as_ref() {
                    let _ = sender.send(PeerlineEvent::Message(error.to_string()));
                }
                last_error = Some(error);
            }
        }
    }

    let error = last_error
        .map(|error| error.to_string())
        .unwrap_or_else(|| "no usable endpoint".into());
    Err(anyhow::anyhow!(
        "discovered {}, but all routes failed: {error}",
        plan.name
    ))
}

async fn send_candidate(
    candidate: &Candidate,
    plan: &NamedSendPlan,
    archive: peerline_transfer::Archive,
    transfer_id: TransferId,
) -> anyhow::Result<peerline_net::SentTransfer> {
    match candidate.route {
        RouteKind::LanDirect | RouteKind::PublicDirect => {
            let endpoint = candidate
                .addresses
                .first()
                .and_then(|address| parse_ip_endpoint(address))
                .ok_or_else(|| anyhow::anyhow!("direct candidate missing socket endpoint"))?;
            peerline_net::send_prebuilt_direct(
                SendOptions {
                    endpoint,
                    name: Some(plan.name.clone()),
                    code: plan.code.clone(),
                    source_id: plan.source_id,
                    paths: plan.paths.clone(),
                    compression: plan.compression,
                    events: plan.events.clone(),
                },
                archive,
                transfer_id,
            )
            .await
        }
        RouteKind::Libp2pDcutr | RouteKind::WebRtcDirect | RouteKind::Libp2pRelay => {
            let peer_id = candidate.peer_id.parse::<libp2p::PeerId>()?;
            let addresses = candidate
                .addresses
                .iter()
                .map(|address| address.parse::<libp2p::Multiaddr>())
                .collect::<Result<Vec<_>, _>>()?;
            if addresses.is_empty() {
                anyhow::bail!("libp2p candidate missing multiaddr");
            }
            peerline_net::send_prebuilt_libp2p(
                Libp2pSendOptions {
                    peer_id,
                    addresses,
                    name: plan.name.clone(),
                    code: plan.code.clone(),
                    source_id: plan.source_id,
                    paths: plan.paths.clone(),
                    compression: plan.compression,
                    route: candidate.route.connection_route(),
                    enable_upnp: plan.discovery.enable_upnp,
                    webrtc_ice_servers: plan.discovery.webrtc_ice_servers.clone(),
                    events: plan.events.clone(),
                },
                archive,
                transfer_id,
            )
            .await
        }
    }
}

fn clone_archive(
    archive: &peerline_transfer::Archive,
) -> anyhow::Result<peerline_transfer::Archive> {
    peerline_transfer::Archive::from_existing(
        archive.manifest.clone(),
        archive.compression,
        archive.resource_id,
        archive.reader()?,
        archive.len(),
    )
}

fn emit_send_message(
    events: &Option<mpsc::UnboundedSender<PeerlineEvent>>,
    message: impl Into<String>,
) {
    if let Some(sender) = events {
        let _ = sender.send(PeerlineEvent::Message(message.into()));
    }
}

fn retry_delay(attempt: usize) -> Duration {
    let base_ms = match attempt {
        0 | 1 => 0,
        2 => 500,
        3 => 1_000,
        4 => 2_000,
        5 => 4_000,
        _ => 8_000,
    };
    let jitter_ms = (base_ms as f64 * 0.2) as i64;
    if jitter_ms <= 0 {
        return Duration::from_millis(base_ms);
    }
    let delta = rand::thread_rng().gen_range(-jitter_ms..=jitter_ms);
    let adjusted = (base_ms as i64 + delta).max(0) as u64;
    Duration::from_millis(adjusted)
}

fn is_fatal_error(error: &anyhow::Error) -> bool {
    if let Some(transfer_error) = error.downcast_ref::<peerline_net::direct::TransferError>() {
        return transfer_error.kind() == peerline_net::direct::TransferErrorKind::Fatal;
    }
    let message = error.to_string();
    message.contains("receiver name mismatch")
        || message.contains("incompatible peerline protocol version")
        || message.contains("authentication")
        || message.contains("hash mismatch")
        || message.contains("protocol version")
}

fn route_label(route: &RouteKind) -> &'static str {
    match route {
        RouteKind::LanDirect => "lan-direct",
        RouteKind::PublicDirect => "public-direct",
        RouteKind::Libp2pDcutr => "libp2p-dcutr",
        RouteKind::WebRtcDirect => "webrtc-direct",
        RouteKind::Libp2pRelay => "libp2p-relay",
    }
}

fn route_allowed(route: &RouteKind, allow_relay_fallback: bool) -> bool {
    !matches!(route, RouteKind::Libp2pRelay) || allow_relay_fallback
}

fn relay_fallback_enabled(allow_relay_fallback: bool, no_relay_fallback: bool) -> bool {
    allow_relay_fallback || !no_relay_fallback
}

fn set(args: SetArgs) -> anyhow::Result<()> {
    match args.command {
        SetCommand::Name { name } => {
            let name = HumanName::parse(name)?;
            ConfigStore::user_default()?.set_name(name.clone())?;
            println!("saved name: {name}");
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum DirectTarget {
    Exact(SocketAddr),
    Ip(IpAddr),
}

fn direct_endpoint_arg(args: &SendArgs) -> Option<DirectTarget> {
    if args.name.is_some() {
        return None;
    }
    let value = args.args.first()?;
    if let Ok(endpoint) = value.parse::<SocketAddr>() {
        return Some(DirectTarget::Exact(endpoint));
    }
    if let Ok(ip) = value.parse::<IpAddr>() {
        return Some(DirectTarget::Ip(ip));
    }
    None
}

fn resolve_recv_identity(
    saved_name: Option<HumanName>,
    first: Option<String>,
    second: Option<String>,
) -> anyhow::Result<(HumanName, HumanCode)> {
    match (saved_name, first, second) {
        (saved_name, None, None) => Ok((
            saved_name.unwrap_or_else(HumanName::generate),
            HumanCode::generate(),
        )),
        (Some(name), Some(code), None) => Ok((name, HumanCode::parse(code)?)),
        (None, Some(_), None) => anyhow::bail!(
            "recv <code> requires a saved name; run `peerline set name <name>` or use `peerline recv <name> <code>`"
        ),
        (_, Some(name), Some(code)) => Ok((HumanName::parse(name)?, HumanCode::parse(code)?)),
        (_, None, Some(_)) => unreachable!("clap cannot produce second positional without first"),
    }
}

fn resolve_named_send(args: SendArgs) -> anyhow::Result<(HumanName, HumanCode, Vec<PathBuf>)> {
    match (args.name, args.code) {
        (Some(name), Some(code)) => {
            let paths = args.args.into_iter().map(PathBuf::from).collect::<Vec<_>>();
            if paths.is_empty() {
                anyhow::bail!("send requires at least one file or folder path");
            }
            return Ok((HumanName::parse(name)?, HumanCode::parse(code)?, paths));
        }
        (Some(_), None) => anyhow::bail!("--name requires --code"),
        (None, Some(_)) => anyhow::bail!(
            "--code without --name is only valid for direct IP sends; use positional `<name> <code>` or add --name"
        ),
        (None, None) => {}
    }

    if args.args.len() < 3 {
        anyhow::bail!("usage: peerline send <name> <code> <path...>");
    }
    let name = HumanName::parse(&args.args[0])?;
    let code = HumanCode::parse(&args.args[1])?;
    let paths = args
        .args
        .iter()
        .skip(2)
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    Ok((name, code, paths))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn recv_one_arg_uses_saved_name_as_code() {
        let (name, code) = resolve_recv_identity(
            Some(HumanName::parse("river-mango-42").unwrap()),
            Some("rose-lime-iris-jade-1234".into()),
            None,
        )
        .unwrap();
        assert_eq!(name.as_str(), "river-mango-42");
        assert_eq!(code.as_str(), "rose-lime-iris-jade-1234");
    }

    #[test]
    fn recv_one_arg_without_saved_name_errors() {
        assert!(resolve_recv_identity(None, Some("code".into()), None).is_err());
    }

    #[test]
    fn name_flag_prevents_ip_like_path_from_becoming_direct_endpoint() {
        let args = SendArgs {
            args: vec!["127.0.0.1".into()],
            name: Some("river-mango-42".into()),
            code: Some("rose-lime-iris-jade-1234".into()),
            compression: CompressionArg::Auto,
            allow_relay_fallback: false,
            no_relay_fallback: false,
            retry_attempts: DEFAULT_RETRY_ATTEMPTS,
        };
        assert!(direct_endpoint_arg(&args).is_none());

        let (_, _, paths) = resolve_named_send(args).unwrap();
        assert_eq!(paths, vec![PathBuf::from("127.0.0.1")]);
    }

    #[test]
    fn direct_endpoint_arg_distinguishes_ip_and_ip_port() {
        let ip_only = SendArgs {
            args: vec!["127.0.0.1".into()],
            name: None,
            code: Some("rose-lime-iris-jade-1234".into()),
            compression: CompressionArg::Auto,
            allow_relay_fallback: false,
            no_relay_fallback: false,
            retry_attempts: DEFAULT_RETRY_ATTEMPTS,
        };
        assert!(matches!(
            direct_endpoint_arg(&ip_only),
            Some(DirectTarget::Ip(ip)) if ip == IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        ));

        let ip_port = SendArgs {
            args: vec!["127.0.0.1:43117".into()],
            name: None,
            code: Some("rose-lime-iris-jade-1234".into()),
            compression: CompressionArg::Auto,
            allow_relay_fallback: false,
            no_relay_fallback: false,
            retry_attempts: DEFAULT_RETRY_ATTEMPTS,
        };
        assert!(matches!(
            direct_endpoint_arg(&ip_port),
            Some(DirectTarget::Exact(endpoint)) if endpoint == SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), DEFAULT_DIRECT_PORT)
        ));
    }

    #[test]
    fn partial_named_flags_are_rejected() {
        let name_only = SendArgs {
            args: vec!["file.txt".into()],
            name: Some("river-mango-42".into()),
            code: None,
            compression: CompressionArg::Auto,
            allow_relay_fallback: false,
            no_relay_fallback: false,
            retry_attempts: DEFAULT_RETRY_ATTEMPTS,
        };
        assert!(resolve_named_send(name_only).is_err());

        let code_only = SendArgs {
            args: vec!["file.txt".into()],
            name: None,
            code: Some("rose-lime-iris-jade-1234".into()),
            compression: CompressionArg::Auto,
            allow_relay_fallback: false,
            no_relay_fallback: false,
            retry_attempts: DEFAULT_RETRY_ATTEMPTS,
        };
        assert!(resolve_named_send(code_only).is_err());
    }

    #[test]
    fn relay_fallback_is_enabled_by_default_and_can_be_disabled() {
        assert!(relay_fallback_enabled(false, false));
        assert!(!relay_fallback_enabled(false, true));
        assert!(relay_fallback_enabled(true, true));
        assert!(!route_allowed(&RouteKind::Libp2pRelay, false));
        assert!(route_allowed(&RouteKind::Libp2pRelay, true));
        assert!(route_allowed(&RouteKind::Libp2pDcutr, false));
    }

    #[test]
    fn retry_attempts_must_be_positive() {
        assert!(parse_retry_attempts("0").is_err());
        assert_eq!(parse_retry_attempts("1").unwrap(), 1);
        assert_eq!(parse_retry_attempts("5").unwrap(), 5);
    }

    #[test]
    fn retry_delay_caps_at_eight_seconds() {
        assert_eq!(retry_delay(1), Duration::from_millis(0));
        assert!(retry_delay(2) <= Duration::from_millis(600));
        assert!(retry_delay(5) <= Duration::from_millis(4_800));
        assert!(retry_delay(6) <= Duration::from_millis(9_600));
    }

    #[test]
    fn fatal_error_detection_matches_protocol_and_auth_failures() {
        assert!(is_fatal_error(&anyhow::anyhow!("receiver name mismatch")));
        assert!(is_fatal_error(&anyhow::anyhow!(
            "incompatible peerline protocol version 1; expected 2"
        )));
        assert!(is_fatal_error(&anyhow::anyhow!("authentication failed")));
        assert!(is_fatal_error(&anyhow::anyhow!("hash mismatch")));
        assert!(!is_fatal_error(&anyhow::anyhow!(
            "connection reset by peer"
        )));
    }

    #[test]
    fn idle_timeout_minutes_accepts_decimal_and_zero_disables() {
        assert_eq!(parse_idle_timeout_minutes("0").unwrap(), 0.0);
        assert!(recv_idle_timeout(0.0).is_none());
        assert_eq!(
            recv_idle_timeout(parse_idle_timeout_minutes("0.5").unwrap()),
            Some(Duration::from_secs(30))
        );
        assert!(parse_idle_timeout_minutes("-1").is_err());
    }
}
