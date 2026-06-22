#[path = "main/doctor/mod.rs"]
mod doctor;
#[path = "main/i2p.rs"]
mod i2p;
#[path = "main/setup/mod.rs"]
mod setup;
#[path = "main/terminal.rs"]
mod terminal;
#[path = "main/wait.rs"]
mod wait;

use clap::{Args, Parser, Subcommand, ValueEnum};
use futures::{StreamExt, stream::FuturesUnordered};
use peerline_core::{
    Compression, ConfigStore, DEFAULT_DIRECT_PORT, DEFAULT_DIRECT_PORT_WINDOW, HumanCode,
    HumanName, NodeId, PeerlineEvent, TransferId, TransferStage, parse_ip_endpoint,
};
use peerline_net::{
    Candidate, Libp2pRecvOptions, Libp2pSendOptions, PublicTunnelEndpoint, PublicTunnelProvider,
    RecvOptions, RouteKind, SendOptions, TorOnionEndpoint, bind_direct_listener,
};
use rand::Rng;
use std::{
    io::{ErrorKind, IsTerminal, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use terminal::{init_tracing, register_activity_log_sender, spawn_send_tui, spawn_terminal_ui};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, BufReader},
    process::{Child, Command as TokioCommand},
    sync::{mpsc, watch},
    task::JoinHandle,
};
use wait::{
    ReceiverPath, RecvOutcome, RetryDecision, TaskOutcome, drain_retry_signals, format_duration,
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
const TOR_FALLBACK_DELAY: Duration = Duration::from_secs(3);
const I2P_FALLBACK_DELAY: Duration = Duration::from_secs(3);

#[derive(Debug, Subcommand)]
enum Command {
    /// Check Peerline configuration and platform dependencies.
    Doctor(doctor::DoctorArgs),
    /// Receive files and folders from a paired peer.
    Recv(RecvArgs),
    /// Send files and folders to a paired peer.
    Send(SendArgs),
    /// Save local Peerline preferences.
    Set(SetArgs),
    /// Guide dependency installation with an interactive terminal UI.
    Setup(setup::SetupArgs),
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
    #[arg(long)]
    no_upnp: bool,
    #[arg(long)]
    no_nat_pmp_pcp: bool,
    #[arg(long)]
    no_quic: bool,
    #[arg(long)]
    no_dcutr: bool,
    #[arg(long)]
    no_turn: bool,
    #[arg(long, hide = true, conflicts_with = "no_tor")]
    tor: bool,
    #[arg(long)]
    no_tor: bool,
    #[arg(long)]
    no_i2p: bool,
    #[arg(long, default_value_t = SocketAddr::from(([127, 0, 0, 1], 7656)))]
    i2p_sam: SocketAddr,
    #[arg(long, value_enum)]
    tunnel: Option<TunnelProviderArg>,
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
    #[arg(long)]
    no_upnp: bool,
    #[arg(long)]
    no_nat_pmp_pcp: bool,
    #[arg(long)]
    no_quic: bool,
    #[arg(long)]
    no_dcutr: bool,
    #[arg(long)]
    no_turn: bool,
    #[arg(long)]
    no_tor: bool,
    #[arg(long)]
    no_i2p: bool,
    #[arg(long, default_value_t = SocketAddr::from(([127, 0, 0, 1], 7656)))]
    i2p_sam: SocketAddr,
    #[arg(long, default_value_t = SocketAddr::from(([127, 0, 0, 1], 9050)))]
    tor_socks_proxy: SocketAddr,
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

#[derive(Clone, Copy, Debug, ValueEnum)]
enum TunnelProviderArg {
    Cloudflared,
    Localtunnel,
    #[value(alias("tunnelmole"))]
    Tmole,
}

impl From<TunnelProviderArg> for PublicTunnelProvider {
    fn from(value: TunnelProviderArg) -> Self {
        match value {
            TunnelProviderArg::Cloudflared => Self::Cloudflared,
            TunnelProviderArg::Localtunnel => Self::Localtunnel,
            TunnelProviderArg::Tmole => Self::Tmole,
        }
    }
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
        Command::Doctor(args) => doctor::run(args).await,
        Command::Recv(args) => recv(args).await,
        Command::Send(args) => send(args).await,
        Command::Set(args) => set(args),
        Command::Setup(args) => setup::run(args).await,
    }
}

async fn recv(args: RecvArgs) -> anyhow::Result<()> {
    let store = ConfigStore::user_default()?;
    let config = store.load()?;
    let tunnel_provider = recv_public_tunnel_provider(&args);
    let discovery = recv_discovery_config(&args);
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
    println!(
        "waiting for transfers over {}...",
        recv_route_status(
            &discovery,
            tunnel_provider,
            discovery.enable_tor,
            discovery.enable_i2p,
        )
    );
    match idle_timeout {
        Some(timeout) => println!(
            "idle timeout: {} (change with --idle-timeout-minutes)",
            format_duration(timeout)
        ),
        None => println!("idle timeout: disabled"),
    }
    std::io::stdout().flush()?;

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
            route_status: recv_route_status(
                &discovery,
                tunnel_provider,
                discovery.enable_tor,
                discovery.enable_i2p,
            ),
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
    let public_tunnel = match tunnel_provider {
        Some(provider) => {
            match wait_with_quit(start_public_tunnel(provider), &mut quit_rx).await? {
                TaskOutcome::Completed(tunnel) => Some(tunnel),
                TaskOutcome::Quit => {
                    finish_recv(network_events, events, event_fanout, tui_task).await;
                    return Ok(());
                }
            }
        }
        None => None,
    };
    let public_tunnel_endpoints = public_tunnel
        .as_ref()
        .map(|tunnel| vec![tunnel.endpoint.clone()])
        .unwrap_or_default();
    if let Some(tunnel) = public_tunnel.as_ref() {
        println!(
            "public tunnel: {} {} (local {})",
            tunnel.provider.label(),
            tunnel.endpoint.url,
            tunnel.local_addr
        );
    }
    let tor_onion = if discovery.enable_tor {
        match wait_with_quit(start_tor_onion(), &mut quit_rx).await {
            Ok(TaskOutcome::Completed(tor)) => Some(tor),
            Ok(TaskOutcome::Quit) => {
                finish_recv(network_events, events, event_fanout, tui_task).await;
                return Ok(());
            }
            Err(error) if args.tor => {
                finish_recv(network_events, events, event_fanout, tui_task).await;
                return Err(error.context("could not start required Tor onion listener"));
            }
            Err(error) => {
                let message = format!("Tor onion unavailable: {error}; continuing without Tor");
                println!("{message}");
                let _ = network_events.send(PeerlineEvent::Message(message));
                None
            }
        }
    } else {
        None
    };
    let tor_onion_endpoints = tor_onion
        .as_ref()
        .map(|tor| vec![tor.endpoint.clone()])
        .unwrap_or_default();
    if let Some(tor) = tor_onion.as_ref() {
        println!("tor onion: {} (local {})", tor.endpoint.url, tor.local_addr);
    }
    let i2p = if discovery.enable_i2p {
        match wait_with_quit(i2p::start_i2p(discovery.i2p_sam), &mut quit_rx).await {
            Ok(TaskOutcome::Completed(i2p)) => Some(i2p),
            Ok(TaskOutcome::Quit) => {
                finish_recv(network_events, events, event_fanout, tui_task).await;
                return Ok(());
            }
            Err(error) => {
                let message = format!("I2P unavailable: {error}; continuing without I2P");
                println!("{message}");
                let _ = network_events.send(PeerlineEvent::Message(message));
                None
            }
        }
    } else {
        None
    };
    let i2p_endpoints = i2p
        .as_ref()
        .map(|i2p| vec![i2p.endpoint.clone()])
        .unwrap_or_default();
    if let Some(i2p) = i2p.as_ref() {
        println!(
            "i2p: {} via SAM {} (local {})",
            i2p.endpoint.url, discovery.i2p_sam, i2p.local_addr
        );
    }

    let mut transfers = 0usize;
    let mut files = 0usize;
    let mut bytes = 0u64;

    loop {
        let _ = network_events.send(PeerlineEvent::Message(
            "ready for the next transfer".to_string(),
        ));
        let mut receiver_paths = vec![
            ReceiverPath::new(
                "direct",
                peerline_net::recv_once_bound(
                    &listener,
                    RecvOptions {
                        name: name.clone(),
                        code: code.clone(),
                        bind: actual_bind,
                        destination: destination.clone(),
                        overwrite: args.overwrite,
                        events: events.clone(),
                    },
                ),
            ),
            ReceiverPath::new(
                "libp2p",
                peerline_net::recv_libp2p(Libp2pRecvOptions {
                    name: name.clone(),
                    code: code.clone(),
                    direct_bind: actual_bind,
                    destination: destination.clone(),
                    overwrite: args.overwrite,
                    discovery: discovery.clone(),
                    public_tunnel_endpoints: public_tunnel_endpoints.clone(),
                    tor_onion_endpoints: tor_onion_endpoints.clone(),
                    i2p_endpoints: i2p_endpoints.clone(),
                    events: events.clone(),
                }),
            ),
        ];
        if let Some(tunnel) = public_tunnel.as_ref() {
            receiver_paths.push(ReceiverPath::new(
                "public-tunnel",
                peerline_net::recv_public_tunnel_bound(
                    &tunnel.listener,
                    RecvOptions {
                        name: name.clone(),
                        code: code.clone(),
                        bind: actual_bind,
                        destination: destination.clone(),
                        overwrite: args.overwrite,
                        events: events.clone(),
                    },
                    tunnel.endpoint.url.clone(),
                ),
            ));
        }
        if let Some(i2p) = i2p.as_ref() {
            receiver_paths.push(ReceiverPath::new(
                "i2p",
                peerline_net::recv_i2p_bound(
                    &i2p.listener,
                    RecvOptions {
                        name: name.clone(),
                        code: code.clone(),
                        bind: actual_bind,
                        destination: destination.clone(),
                        overwrite: args.overwrite,
                        events: events.clone(),
                    },
                    i2p.endpoint.url.clone(),
                ),
            ));
        }
        if let Some(tor) = tor_onion.as_ref() {
            receiver_paths.push(ReceiverPath::new(
                "tor-onion",
                peerline_net::recv_tor_onion_bound(
                    &tor.listener,
                    RecvOptions {
                        name: name.clone(),
                        code: code.clone(),
                        bind: actual_bind,
                        destination: destination.clone(),
                        overwrite: args.overwrite,
                        events: events.clone(),
                    },
                    tor.endpoint.url.clone(),
                ),
            ));
        }

        match wait_for_recv_activity(
            wait_for_receiver(receiver_paths),
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
                finish_recv(network_events, events, event_fanout, tui_task).await;
                return Err(error);
            }
        }
    }

    finish_recv(network_events, events, event_fanout, tui_task).await;
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

async fn finish_recv(
    network_events: mpsc::UnboundedSender<PeerlineEvent>,
    events: Option<mpsc::UnboundedSender<PeerlineEvent>>,
    event_fanout: JoinHandle<()>,
    tui_task: Option<JoinHandle<anyhow::Result<()>>>,
) {
    let _ = network_events.send(PeerlineEvent::Shutdown);
    drop(events);
    drop(network_events);
    let _ = event_fanout.await;
    if let Some(task) = tui_task {
        let _ = task.await;
    }
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
    let discovery = send_discovery_config(&args);
    let (name, code, paths) = resolve_named_send(args)?;
    let mut ui = spawn_send_tui(
        "peer",
        name.to_string(),
        code.clone(),
        send_route_status(&discovery),
        true,
    );
    if ui.events.is_none() {
        println!(
            "discovering {name} through {}",
            send_route_status(&discovery)
        );
    }
    if code.is_low_entropy() {
        tracing::warn!("code entropy looks low; generated codes are safer on public networks");
    }
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

#[derive(Clone)]
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
    tracing::info!(
        peer = %plan.name,
        "discovering routes through pkarr/mainline, rendezvous, DHT, and mDNS"
    );
    let discovery_future = peerline_net::discovery::discover_peer_candidates(
        &plan.name,
        &plan.code,
        plan.discovery.clone(),
    );
    let candidates = match wait_with_quit(discovery_future, quit_rx).await {
        Ok(TaskOutcome::Completed(candidates)) if !candidates.is_empty() => candidates,
        Ok(TaskOutcome::Completed(_)) => {
            let message = format!("could not discover a route for {}", plan.name);
            emit_send_message(&plan.events, message.clone());
            anyhow::bail!("{message}");
        }
        Ok(TaskOutcome::Quit) => {
            return Ok(TaskOutcome::Quit);
        }
        Err(error) => {
            return Err(error);
        }
    };
    emit_send_message(
        &plan.events,
        format!(
            "discovered routes: {}",
            format_candidate_routes(&candidates)
        ),
    );

    let delay_anonymous_fallback = candidates.iter().any(|candidate| {
        !matches!(candidate.route, RouteKind::TorOnion | RouteKind::I2p)
            && route_allowed(&candidate.route, &plan.discovery)
    });
    let mut attempts = FuturesUnordered::new();
    let mut queued_attempts = 0usize;
    for candidate in candidates {
        if !route_allowed(&candidate.route, &plan.discovery) {
            continue;
        }

        if let Some(sender) = plan.events.as_ref() {
            let _ = sender.send(PeerlineEvent::Message(format!(
                "trying {} via {}",
                candidate.peer_id,
                route_label(&candidate.route)
            )));
        }

        let candidate_plan = plan.clone();
        let candidate_archive = clone_archive(&archive)?;
        let candidate_delay = delay_anonymous_fallback
            && matches!(candidate.route, RouteKind::TorOnion | RouteKind::I2p);
        attempts.push(async move {
            if candidate_delay {
                let delay = match candidate.route {
                    RouteKind::I2p => I2P_FALLBACK_DELAY,
                    _ => TOR_FALLBACK_DELAY,
                };
                tokio::time::sleep(delay).await;
            }
            let candidate_route = candidate.route.clone();
            let result =
                send_candidate(&candidate, &candidate_plan, candidate_archive, transfer_id).await;
            (candidate_route, result)
        });
        queued_attempts += 1;
    }

    let mut last_error = None;
    while queued_attempts > 0 {
        let quit_future = async {
            if let Some(rx) = quit_rx.as_mut() {
                wait_for_send_quit(rx).await
            } else {
                std::future::pending::<bool>().await
            }
        };

        tokio::select! {
            maybe_result = attempts.next() => {
                let Some((candidate_route, result)) = maybe_result else {
                    break;
                };
                queued_attempts -= 1;
                match result {
                    Ok(sent) => {
                        return Ok(TaskOutcome::Completed(sent));
                    }
                    Err(error) => {
                        if let Some(sender) = plan.events.as_ref() {
                            let _ = sender.send(PeerlineEvent::Message(format!(
                                "{} via {} failed: {error}",
                                plan.name,
                                route_label(&candidate_route)
                            )));
                        }
                        last_error = Some(error);
                    }
                }
            }
            quit = quit_future => {
                if quit {
                    return Ok(TaskOutcome::Quit);
                }
                *quit_rx = None;
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
        RouteKind::PublicTunnel => {
            let endpoint = candidate
                .addresses
                .first()
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("public tunnel candidate missing URL"))?;
            peerline_net::send_prebuilt_public_tunnel(
                SendOptions {
                    endpoint: "127.0.0.1:0".parse().expect("static endpoint"),
                    name: Some(plan.name.clone()),
                    code: plan.code.clone(),
                    source_id: plan.source_id,
                    paths: plan.paths.clone(),
                    compression: plan.compression,
                    events: plan.events.clone(),
                },
                archive,
                transfer_id,
                endpoint,
            )
            .await
        }
        RouteKind::TorOnion => {
            let endpoint = candidate
                .addresses
                .first()
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Tor onion candidate missing URL"))?;
            peerline_net::send_prebuilt_tor_onion(
                SendOptions {
                    endpoint: "127.0.0.1:0".parse().expect("static endpoint"),
                    name: Some(plan.name.clone()),
                    code: plan.code.clone(),
                    source_id: plan.source_id,
                    paths: plan.paths.clone(),
                    compression: plan.compression,
                    events: plan.events.clone(),
                },
                archive,
                transfer_id,
                endpoint,
                plan.discovery.tor_socks_proxy,
            )
            .await
        }
        RouteKind::I2p => {
            let endpoint = candidate
                .addresses
                .first()
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("I2P candidate missing URL"))?;
            peerline_net::send_prebuilt_i2p(
                SendOptions {
                    endpoint: "127.0.0.1:0".parse().expect("static endpoint"),
                    name: Some(plan.name.clone()),
                    code: plan.code.clone(),
                    source_id: plan.source_id,
                    paths: plan.paths.clone(),
                    compression: plan.compression,
                    events: plan.events.clone(),
                },
                archive,
                transfer_id,
                endpoint,
                plan.discovery.i2p_sam,
            )
            .await
        }
        RouteKind::Libp2pQuic
        | RouteKind::Libp2pDcutr
        | RouteKind::WebRtcDirect
        | RouteKind::WebRtcTurn
        | RouteKind::Libp2pRelay => {
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

async fn wait_for_send_quit(rx: &mut watch::Receiver<bool>) -> bool {
    match rx.changed().await {
        Ok(()) => *rx.borrow(),
        Err(_) => false,
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
        RouteKind::PublicTunnel => "public-tunnel",
        RouteKind::TorOnion => "tor-onion",
        RouteKind::I2p => "i2p",
        RouteKind::Libp2pQuic => "libp2p-quic",
        RouteKind::Libp2pDcutr => "libp2p-dcutr",
        RouteKind::WebRtcDirect => "webrtc-direct",
        RouteKind::WebRtcTurn => "webrtc-turn",
        RouteKind::Libp2pRelay => "libp2p-relay",
    }
}

fn format_candidate_routes(candidates: &[Candidate]) -> String {
    let mut routes = candidates
        .iter()
        .map(|candidate| route_label(&candidate.route))
        .collect::<Vec<_>>();
    routes.sort_unstable();
    routes.dedup();
    if routes.is_empty() {
        "none".into()
    } else {
        routes.join(", ")
    }
}

fn route_allowed(route: &RouteKind, discovery: &peerline_net::DiscoveryConfig) -> bool {
    discovery.route_enabled(route)
}

fn relay_fallback_enabled(allow_relay_fallback: bool, no_relay_fallback: bool) -> bool {
    allow_relay_fallback || !no_relay_fallback
}

fn recv_discovery_config(args: &RecvArgs) -> peerline_net::DiscoveryConfig {
    let mut discovery = peerline_net::DiscoveryConfig {
        allow_relay_data_fallback: relay_fallback_enabled(
            args.allow_relay_fallback,
            args.no_relay_fallback,
        ),
        ..Default::default()
    };
    apply_discovery_flags(&mut discovery, DiscoveryDisableFlags::from_recv(args));
    discovery.i2p_sam = args.i2p_sam;
    if args.tor {
        discovery.enable_tor = true;
    }
    discovery
}

fn send_discovery_config(args: &SendArgs) -> peerline_net::DiscoveryConfig {
    let mut discovery = peerline_net::DiscoveryConfig {
        allow_relay_data_fallback: relay_fallback_enabled(
            args.allow_relay_fallback,
            args.no_relay_fallback,
        ),
        tor_socks_proxy: args.tor_socks_proxy,
        ..Default::default()
    };
    apply_discovery_flags(&mut discovery, DiscoveryDisableFlags::from_send(args));
    discovery.i2p_sam = args.i2p_sam;
    discovery
}

#[derive(Clone, Copy)]
struct DiscoveryDisableFlags {
    no_upnp: bool,
    no_nat_pmp_pcp: bool,
    no_quic: bool,
    no_dcutr: bool,
    no_turn: bool,
    no_tor: bool,
    no_i2p: bool,
}

impl DiscoveryDisableFlags {
    fn from_recv(args: &RecvArgs) -> Self {
        Self {
            no_upnp: args.no_upnp,
            no_nat_pmp_pcp: args.no_nat_pmp_pcp,
            no_quic: args.no_quic,
            no_dcutr: args.no_dcutr,
            no_turn: args.no_turn,
            no_tor: args.no_tor,
            no_i2p: args.no_i2p,
        }
    }

    fn from_send(args: &SendArgs) -> Self {
        Self {
            no_upnp: args.no_upnp,
            no_nat_pmp_pcp: args.no_nat_pmp_pcp,
            no_quic: args.no_quic,
            no_dcutr: args.no_dcutr,
            no_turn: args.no_turn,
            no_tor: args.no_tor,
            no_i2p: args.no_i2p,
        }
    }
}

fn apply_discovery_flags(
    discovery: &mut peerline_net::DiscoveryConfig,
    flags: DiscoveryDisableFlags,
) {
    if flags.no_upnp {
        discovery.enable_upnp = false;
    }
    if flags.no_nat_pmp_pcp {
        discovery.enable_natpmp_pcp = false;
    }
    if flags.no_quic {
        discovery.enable_quic = false;
    }
    if flags.no_dcutr {
        discovery.enable_dcutr = false;
    }
    if flags.no_turn {
        discovery.enable_turn = false;
    }
    if flags.no_tor {
        discovery.enable_tor = false;
    }
    if flags.no_i2p {
        discovery.enable_i2p = false;
    }
    if !discovery.enable_turn {
        discovery.webrtc_ice_servers =
            peerline_net::without_turn_ice_servers(&discovery.webrtc_ice_servers);
    }
}

fn recv_public_tunnel_provider(args: &RecvArgs) -> Option<PublicTunnelProvider> {
    args.tunnel.map(Into::into)
}

fn recv_route_status(
    discovery: &peerline_net::DiscoveryConfig,
    tunnel_provider: Option<PublicTunnelProvider>,
    tor_onion: bool,
    i2p: bool,
) -> String {
    let mut routes = vec!["direct TCP".to_string()];
    if let Some(provider) = tunnel_provider {
        routes.push(format!("{} public tunnel", provider.label()));
    }
    if tor_onion {
        routes.push("Tor onion".into());
    }
    if i2p {
        routes.push("I2P".into());
    }
    routes.push("libp2p TCP".into());
    if !discovery.libp2p_rendezvous_peers.is_empty() {
        routes.push("libp2p rendezvous".into());
    }
    if discovery.enable_quic {
        routes.push("libp2p QUIC".into());
    }
    if discovery.enable_dcutr {
        routes.push("DCUtR".into());
    }
    routes.push(if discovery.enable_turn {
        "WebRTC/TURN".into()
    } else {
        "WebRTC direct".into()
    });
    if discovery.allow_relay_data_fallback {
        routes.push("relay fallback".into());
    }
    routes.join(", ")
}

fn send_route_status(discovery: &peerline_net::DiscoveryConfig) -> String {
    let mut routes = vec!["rendezvous".to_string(), "DHT".into(), "mDNS".into()];
    if !discovery.libp2p_rendezvous_peers.is_empty() {
        routes.push("libp2p rendezvous".into());
    }
    if discovery.enable_public_tunnels {
        routes.push("public tunnel".into());
    }
    if discovery.enable_tor {
        routes.push("Tor onion".into());
    }
    if discovery.enable_i2p {
        routes.push("I2P".into());
    }
    routes.push("direct TCP".into());
    if discovery.enable_quic {
        routes.push("QUIC".into());
    }
    if discovery.enable_dcutr {
        routes.push("DCUtR".into());
    }
    routes.push(if discovery.enable_turn {
        "WebRTC/TURN".into()
    } else {
        "WebRTC direct".into()
    });
    if discovery.allow_relay_data_fallback {
        routes.push("relay fallback".into());
    }
    routes.join(", ")
}

struct RunningPublicTunnel {
    provider: PublicTunnelProvider,
    endpoint: PublicTunnelEndpoint,
    listener: tokio::net::TcpListener,
    local_addr: SocketAddr,
    _process: TunnelProcess,
}

struct RunningTorOnion {
    endpoint: TorOnionEndpoint,
    listener: tokio::net::TcpListener,
    local_addr: SocketAddr,
    _process: TorProcess,
}

struct TunnelProcess {
    child: Child,
    readers: Vec<JoinHandle<()>>,
}

struct TorProcess {
    child: Child,
    readers: Vec<JoinHandle<()>>,
    _temp_dir: tempfile::TempDir,
}

impl Drop for TunnelProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        for reader in &self.readers {
            reader.abort();
        }
    }
}

impl Drop for TorProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        for reader in &self.readers {
            reader.abort();
        }
    }
}

async fn start_public_tunnel(
    provider: PublicTunnelProvider,
) -> anyhow::Result<RunningPublicTunnel> {
    let (listener, local_addr) = peerline_net::bind_public_tunnel_listener().await?;
    let (command, args, mut child) = spawn_public_tunnel_process(provider, local_addr.port())?;
    tracing::info!(
        provider = provider.label(),
        command = %command,
        args = ?args,
        local = %local_addr,
        "started public tunnel command"
    );

    let (url_tx, mut url_rx) = mpsc::unbounded_channel();
    let mut readers = Vec::new();
    if let Some(stdout) = child.stdout.take() {
        readers.push(spawn_tunnel_output_reader(
            provider,
            "stdout",
            stdout,
            url_tx.clone(),
        ));
    }
    if let Some(stderr) = child.stderr.take() {
        readers.push(spawn_tunnel_output_reader(
            provider, "stderr", stderr, url_tx,
        ));
    }

    let mut process = TunnelProcess { child, readers };
    let url = wait_for_public_tunnel_url(provider, &mut url_rx, &mut process.child).await?;
    let endpoint = PublicTunnelEndpoint {
        provider: provider.label().into(),
        url,
    };

    Ok(RunningPublicTunnel {
        provider,
        endpoint,
        listener,
        local_addr,
        _process: process,
    })
}

async fn start_tor_onion() -> anyhow::Result<RunningTorOnion> {
    let (listener, local_addr) = peerline_net::bind_tor_onion_listener().await?;
    let (mut child, temp_dir, hidden_service_dir) = spawn_tor_onion_process(local_addr.port())?;
    tracing::info!(local = %local_addr, "started Tor onion service process");

    let mut readers = Vec::new();
    if let Some(stdout) = child.stdout.take() {
        readers.push(spawn_process_output_reader("tor", "stdout", stdout));
    }
    if let Some(stderr) = child.stderr.take() {
        readers.push(spawn_process_output_reader("tor", "stderr", stderr));
    }

    let mut process = TorProcess {
        child,
        readers,
        _temp_dir: temp_dir,
    };
    let hostname = wait_for_tor_onion_hostname(&hidden_service_dir, &mut process.child).await?;
    let endpoint = TorOnionEndpoint {
        url: peerline_net::normalize_tor_onion_url(&hostname)?,
    };

    Ok(RunningTorOnion {
        endpoint,
        listener,
        local_addr,
        _process: process,
    })
}

fn spawn_public_tunnel_process(
    provider: PublicTunnelProvider,
    port: u16,
) -> anyhow::Result<(String, Vec<String>, Child)> {
    let candidates = public_tunnel_command_candidates(provider, port);
    let mut not_found = Vec::new();
    for (command, args) in candidates {
        let mut process = TokioCommand::new(&command);
        process
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        match process.spawn() {
            Ok(child) => return Ok((command, args, child)),
            Err(error) if error.kind() == ErrorKind::NotFound => not_found.push(command),
            Err(error) => return Err(error.into()),
        }
    }
    anyhow::bail!(
        "could not find {} tunnel command ({})",
        provider.label(),
        not_found.join(", ")
    )
}

fn spawn_tor_onion_process(local_port: u16) -> anyhow::Result<(Child, tempfile::TempDir, PathBuf)> {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let hidden_service_dir = temp_dir.path().join("onion-service");
    let torrc = temp_dir.path().join("torrc");
    let torrc_contents = format!(
        "DataDirectory {}\nSocksPort 0\nControlPort 0\nHiddenServiceDir {}\nHiddenServicePort 80 127.0.0.1:{local_port}\nLog notice stdout\n",
        torrc_path(&data_dir),
        torrc_path(&hidden_service_dir),
    );
    std::fs::write(&torrc, torrc_contents)?;

    let mut process = TokioCommand::new("tor");
    process
        .arg("-f")
        .arg(&torrc)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = process.spawn().map_err(|error| {
        if error.kind() == ErrorKind::NotFound {
            anyhow::anyhow!("could not find tor command; install Tor or run without --tor")
        } else {
            error.into()
        }
    })?;
    Ok((child, temp_dir, hidden_service_dir))
}

fn torrc_path(path: &Path) -> String {
    path.display().to_string()
}

async fn wait_for_tor_onion_hostname(
    hidden_service_dir: &Path,
    child: &mut Child,
) -> anyhow::Result<String> {
    let hostname_path = hidden_service_dir.join("hostname");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        if let Some(status) = child.try_wait()? {
            anyhow::bail!("Tor exited before publishing an onion hostname: {status}");
        }

        match tokio::fs::read_to_string(&hostname_path).await {
            Ok(hostname) => {
                let hostname = hostname.trim();
                if !hostname.is_empty() {
                    return Ok(hostname.to_string());
                }
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }

        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("Tor did not create an onion hostname within 90 seconds");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn public_tunnel_command_candidates(
    provider: PublicTunnelProvider,
    port: u16,
) -> Vec<(String, Vec<String>)> {
    match provider {
        PublicTunnelProvider::Cloudflared => vec![(
            "cloudflared".into(),
            vec![
                "tunnel".into(),
                "--url".into(),
                format!("http://127.0.0.1:{port}"),
            ],
        )],
        PublicTunnelProvider::Localtunnel => ["lt", "localtunnel"]
            .into_iter()
            .map(|command| {
                (
                    command.into(),
                    vec![
                        "--port".into(),
                        port.to_string(),
                        "--local-host".into(),
                        "127.0.0.1".into(),
                    ],
                )
            })
            .collect(),
        PublicTunnelProvider::Tmole => {
            vec![("tmole".into(), vec![port.to_string()])]
        }
    }
}

async fn wait_for_public_tunnel_url(
    provider: PublicTunnelProvider,
    url_rx: &mut mpsc::UnboundedReceiver<String>,
    child: &mut Child,
) -> anyhow::Result<String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    loop {
        match url_rx.try_recv() {
            Ok(url) => return Ok(url),
            Err(mpsc::error::TryRecvError::Empty) => {}
            Err(mpsc::error::TryRecvError::Disconnected) => {
                anyhow::bail!("{} did not report a public URL", provider.label());
            }
        }

        if let Some(status) = child.try_wait()? {
            match tokio::time::timeout(Duration::from_secs(1), url_rx.recv()).await {
                Ok(Some(url)) => return Ok(url),
                Ok(None) | Err(_) => {
                    anyhow::bail!(
                        "{} exited before reporting a public URL: {status}",
                        provider.label()
                    );
                }
            }
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "{} did not report a public URL within 45 seconds",
                provider.label()
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn spawn_tunnel_output_reader<R>(
    provider: PublicTunnelProvider,
    stream_name: &'static str,
    reader: R,
    url_sender: mpsc::UnboundedSender<String>,
) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    tracing::debug!(
                        provider = provider.label(),
                        stream = stream_name,
                        line = %line,
                        "public tunnel output"
                    );
                    if let Some(url) = extract_public_tunnel_url(&line) {
                        let _ = url_sender.send(url);
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    tracing::debug!(%error, provider = provider.label(), stream = stream_name, "public tunnel output reader failed");
                    break;
                }
            }
        }
    })
}

pub(crate) fn spawn_process_output_reader<R>(
    process_name: &'static str,
    stream_name: &'static str,
    reader: R,
) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    tracing::debug!(
                        process = process_name,
                        stream = stream_name,
                        line = %line,
                        "process output"
                    );
                }
                Ok(None) => break,
                Err(error) => {
                    tracing::debug!(%error, process = process_name, stream = stream_name, "process output reader failed");
                    break;
                }
            }
        }
    })
}

fn extract_public_tunnel_url(line: &str) -> Option<String> {
    let mut fallback = None;
    for token in line.split_whitespace() {
        let Some(start) = token
            .find("https://")
            .or_else(|| token.find("http://"))
            .or_else(|| token.find("wss://"))
            .or_else(|| token.find("ws://"))
        else {
            continue;
        };
        let raw = token[start..].trim_matches(|ch: char| {
            matches!(
                ch,
                '"' | '\'' | '`' | '<' | '>' | ',' | ';' | '(' | ')' | '[' | ']' | '{' | '}'
            )
        });
        let raw = raw.trim_end_matches('.');
        let Ok(url) = peerline_net::normalize_public_tunnel_url(raw) else {
            continue;
        };
        if url.starts_with("wss://") {
            return Some(url);
        }
        fallback = Some(url);
    }
    fallback
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
    use clap::Parser;
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
            no_upnp: false,
            no_nat_pmp_pcp: false,
            no_quic: false,
            no_dcutr: false,
            no_turn: false,
            no_tor: false,
            no_i2p: false,
            i2p_sam: SocketAddr::from(([127, 0, 0, 1], 7656)),
            tor_socks_proxy: SocketAddr::from(([127, 0, 0, 1], 9050)),
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
            no_upnp: false,
            no_nat_pmp_pcp: false,
            no_quic: false,
            no_dcutr: false,
            no_turn: false,
            no_tor: false,
            no_i2p: false,
            i2p_sam: SocketAddr::from(([127, 0, 0, 1], 7656)),
            tor_socks_proxy: SocketAddr::from(([127, 0, 0, 1], 9050)),
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
            no_upnp: false,
            no_nat_pmp_pcp: false,
            no_quic: false,
            no_dcutr: false,
            no_turn: false,
            no_tor: false,
            no_i2p: false,
            i2p_sam: SocketAddr::from(([127, 0, 0, 1], 7656)),
            tor_socks_proxy: SocketAddr::from(([127, 0, 0, 1], 9050)),
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
            no_upnp: false,
            no_nat_pmp_pcp: false,
            no_quic: false,
            no_dcutr: false,
            no_turn: false,
            no_tor: false,
            no_i2p: false,
            i2p_sam: SocketAddr::from(([127, 0, 0, 1], 7656)),
            tor_socks_proxy: SocketAddr::from(([127, 0, 0, 1], 9050)),
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
            no_upnp: false,
            no_nat_pmp_pcp: false,
            no_quic: false,
            no_dcutr: false,
            no_turn: false,
            no_tor: false,
            no_i2p: false,
            i2p_sam: SocketAddr::from(([127, 0, 0, 1], 7656)),
            tor_socks_proxy: SocketAddr::from(([127, 0, 0, 1], 9050)),
            retry_attempts: DEFAULT_RETRY_ATTEMPTS,
        };
        assert!(resolve_named_send(code_only).is_err());
    }

    #[test]
    fn relay_fallback_is_enabled_by_default_and_can_be_disabled() {
        assert!(relay_fallback_enabled(false, false));
        assert!(!relay_fallback_enabled(false, true));
        assert!(relay_fallback_enabled(true, true));
        let enabled = peerline_net::DiscoveryConfig::default();
        let mut disabled_relay = enabled.clone();
        disabled_relay.allow_relay_data_fallback = false;
        assert!(!route_allowed(&RouteKind::Libp2pRelay, &disabled_relay));
        assert!(route_allowed(&RouteKind::Libp2pRelay, &enabled));
        assert!(route_allowed(&RouteKind::Libp2pDcutr, &enabled));
    }

    #[test]
    fn recv_tor_is_enabled_by_default_and_can_be_disabled() {
        let enabled = Cli::try_parse_from([
            "peerline",
            "recv",
            "river-mango-42",
            "rose-lime-iris-jade-1234",
        ])
        .unwrap();
        let Command::Recv(args) = enabled.command else {
            panic!("expected recv command");
        };
        assert!(recv_discovery_config(&args).enable_tor);

        let disabled = Cli::try_parse_from([
            "peerline",
            "recv",
            "river-mango-42",
            "rose-lime-iris-jade-1234",
            "--no-tor",
        ])
        .unwrap();
        let Command::Recv(args) = disabled.command else {
            panic!("expected recv command");
        };
        assert!(!recv_discovery_config(&args).enable_tor);
    }

    #[test]
    fn tunnel_flag_accepts_tunnelmole_alias() {
        let cli = Cli::try_parse_from([
            "peerline",
            "recv",
            "river-mango-42",
            "rose-lime-iris-jade-1234",
            "--tunnel",
            "tunnelmole",
        ])
        .unwrap();

        let Command::Recv(args) = cli.command else {
            panic!("expected recv command");
        };

        assert!(matches!(args.tunnel, Some(TunnelProviderArg::Tmole)));
        assert!(matches!(
            recv_public_tunnel_provider(&args),
            Some(PublicTunnelProvider::Tmole)
        ));
    }

    #[test]
    fn legacy_tunnel_flags_are_rejected() {
        for flag in ["--cloudflared", "--localtunnel", "--tmole"] {
            let parsed = Cli::try_parse_from([
                "peerline",
                "recv",
                "river-mango-42",
                "rose-lime-iris-jade-1234",
                flag,
            ]);
            assert!(parsed.is_err(), "{flag} should no longer be accepted");
        }
    }

    #[test]
    fn candidate_route_summary_lists_unique_routes() {
        let candidates = vec![
            Candidate {
                peer_id: "peer".into(),
                addresses: vec!["wss://example.com".into()],
                route: RouteKind::PublicTunnel,
            },
            Candidate {
                peer_id: "peer".into(),
                addresses: vec!["ws://exampleabcdefghijklmnopqrstuvwx.onion".into()],
                route: RouteKind::TorOnion,
            },
            Candidate {
                peer_id: "peer".into(),
                addresses: vec!["wss://example.com/2".into()],
                route: RouteKind::PublicTunnel,
            },
        ];

        assert_eq!(
            format_candidate_routes(&candidates),
            "public-tunnel, tor-onion"
        );
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
