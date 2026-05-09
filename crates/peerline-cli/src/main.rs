use clap::{Args, Parser, Subcommand, ValueEnum};
use peerline_core::{
    Compression, ConfigStore, DEFAULT_DIRECT_PORT, HumanCode, HumanName, parse_ip_endpoint,
};
use peerline_net::{
    Candidate, Libp2pRecvOptions, Libp2pSendOptions, ReceivedTransfer, RecvOptions, RouteKind,
    SendOptions,
};
use std::{
    io::IsTerminal,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};
use tokio::{net::TcpListener, task::JoinHandle};

#[derive(Debug, Parser)]
#[command(
    name = "peerline",
    version,
    about = "P2P post-quantum encrypted file transfer"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

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
    #[arg(long)]
    allow_relay_fallback: bool,
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
    #[arg(long)]
    allow_relay_fallback: bool,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_filter())
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
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
    if code.is_low_entropy() {
        eprintln!("warning: code entropy looks low; generated codes are safer on public networks");
    }

    let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), args.port);
    let listener = TcpListener::bind(bind).await?;
    let actual_bind = listener.local_addr()?;
    let destination = std::env::current_dir()?;
    let discovery = peerline_net::DiscoveryConfig {
        allow_relay_data_fallback: args.allow_relay_fallback,
        ..Default::default()
    };

    println!("peerline recv");
    println!("name: {name}");
    println!("code: {code}");
    println!("direct: {actual_bind}");
    println!("waiting for one transfer over direct TCP or libp2p...");

    let (events, tui_task) = if !args.no_tui && std::io::stdout().is_terminal() {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let view = peerline_tui::RecvView {
            name: name.clone(),
            code: code.clone(),
            bind: actual_bind.to_string(),
            route_status: "direct TCP ready; libp2p DHT/mDNS/relay/WebRTC ready".into(),
            stage: peerline_core::TransferStage::Discovering,
            progress: None,
        };
        let task = tokio::spawn(peerline_tui::render_once(view, receiver));
        (Some(sender), Some(task))
    } else {
        (None, None)
    };

    let direct_task = tokio::spawn(peerline_net::recv_once_bound(
        listener,
        RecvOptions {
            name: name.clone(),
            code: code.clone(),
            bind: actual_bind,
            destination: destination.clone(),
            overwrite: args.overwrite,
            events: events.clone(),
        },
    ));
    let libp2p_task = tokio::spawn(peerline_net::recv_libp2p(Libp2pRecvOptions {
        name,
        code,
        direct_bind: actual_bind,
        destination,
        overwrite: args.overwrite,
        discovery,
        events: events.clone(),
    }));
    let received = match wait_for_receiver(direct_task, libp2p_task).await {
        Ok(received) => received,
        Err(error) => {
            if let Some(sender) = events.as_ref() {
                let _ = sender.send(peerline_core::PeerlineEvent::StageChanged(
                    peerline_core::TransferStage::Failed(error.to_string()),
                ));
            }
            if let Some(task) = tui_task {
                let _ = task.await;
            }
            return Err(error);
        }
    };
    if let Some(task) = tui_task {
        let _ = task.await;
    }
    println!(
        "received {} file(s), {} bytes from {}",
        received.files, received.bytes, received.peer
    );
    Ok(())
}

async fn wait_for_receiver(
    mut direct_task: JoinHandle<anyhow::Result<ReceivedTransfer>>,
    mut libp2p_task: JoinHandle<anyhow::Result<ReceivedTransfer>>,
) -> anyhow::Result<ReceivedTransfer> {
    let mut direct_done = false;
    let mut libp2p_done = false;
    let mut last_error = None;

    loop {
        if direct_done && libp2p_done {
            break;
        }

        tokio::select! {
            result = &mut direct_task, if !direct_done => {
                direct_done = true;
                match result {
                    Ok(Ok(received)) => {
                        if !libp2p_done {
                            libp2p_task.abort();
                            let _ = libp2p_task.await;
                        }
                        return Ok(received);
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "direct receiver path stopped");
                        last_error = Some(error);
                    }
                    Err(error) => {
                        last_error = Some(anyhow::anyhow!("direct receiver task failed: {error}"));
                    }
                }
            }
            result = &mut libp2p_task, if !libp2p_done => {
                libp2p_done = true;
                match result {
                    Ok(Ok(received)) => {
                        if !direct_done {
                            direct_task.abort();
                            let _ = direct_task.await;
                        }
                        return Ok(received);
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "libp2p receiver path stopped");
                        last_error = Some(error);
                    }
                    Err(error) => {
                        last_error = Some(anyhow::anyhow!("libp2p receiver task failed: {error}"));
                    }
                }
            }
        }
    }

    Err(last_error
        .unwrap_or_else(|| anyhow::anyhow!("receiver stopped without a completed transfer")))
}

fn tracing_filter() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("error,peerline=info"))
}

async fn send(args: SendArgs) -> anyhow::Result<()> {
    if let Some(endpoint) = direct_endpoint_arg(&args) {
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
        let sent = peerline_net::send_direct(SendOptions {
            endpoint,
            name: None,
            code,
            paths,
            compression: args.compression.into(),
        })
        .await?;
        println!(
            "sent {} file(s), {} bytes to {}",
            sent.files, sent.bytes, sent.endpoint
        );
        return Ok(());
    }

    let compression = args.compression.into();
    let allow_relay_fallback = args.allow_relay_fallback;
    let (name, code, paths) = resolve_named_send(args)?;
    if code.is_low_entropy() {
        eprintln!("warning: code entropy looks low; generated codes are safer on public networks");
    }
    println!("discovering {name} through libp2p Kademlia/mDNS...");
    let discovery = peerline_net::DiscoveryConfig {
        allow_relay_data_fallback: allow_relay_fallback,
        ..Default::default()
    };
    let candidates =
        peerline_net::discovery::discover_peer_candidates(&name, &code, discovery.clone()).await?;
    if candidates.is_empty() {
        anyhow::bail!(
            "could not discover a route for {name}; use `peerline send <ip> <path...> --code=<code>` if you know the receiver address"
        );
    }

    let mut last_error = None;
    for candidate in candidates {
        if !route_allowed(&candidate.route, discovery.allow_relay_data_fallback) {
            continue;
        }

        match send_candidate(&candidate, &name, &code, &paths, compression).await {
            Ok(sent) => {
                println!(
                    "sent {} file(s), {} bytes to {}",
                    sent.files, sent.bytes, sent.endpoint
                );
                return Ok(());
            }
            Err(error) => last_error = Some(error),
        }
    }

    let error = last_error
        .map(|error| error.to_string())
        .unwrap_or_else(|| "no usable endpoint".into());
    anyhow::bail!("discovered {name}, but all routes failed: {error}");
}

async fn send_candidate(
    candidate: &Candidate,
    name: &HumanName,
    code: &HumanCode,
    paths: &[PathBuf],
    compression: Compression,
) -> anyhow::Result<peerline_net::SentTransfer> {
    match candidate.route {
        RouteKind::LanDirect | RouteKind::PublicDirect => {
            let endpoint = candidate
                .addresses
                .first()
                .and_then(|address| parse_ip_endpoint(address))
                .ok_or_else(|| anyhow::anyhow!("direct candidate missing socket endpoint"))?;
            peerline_net::send_direct(SendOptions {
                endpoint,
                name: Some(name.clone()),
                code: code.clone(),
                paths: paths.to_vec(),
                compression,
            })
            .await
        }
        RouteKind::Libp2pDcutr | RouteKind::WebRtcTurn | RouteKind::Libp2pRelay => {
            let peer_id = candidate.peer_id.parse::<libp2p::PeerId>()?;
            let addresses = candidate
                .addresses
                .iter()
                .map(|address| address.parse::<libp2p::Multiaddr>())
                .collect::<Result<Vec<_>, _>>()?;
            if addresses.is_empty() {
                anyhow::bail!("libp2p candidate missing multiaddr");
            }
            peerline_net::send_libp2p(Libp2pSendOptions {
                peer_id,
                addresses,
                name: name.clone(),
                code: code.clone(),
                paths: paths.to_vec(),
                compression,
                route_label: route_label(&candidate.route).into(),
            })
            .await
        }
    }
}

fn route_label(route: &RouteKind) -> &'static str {
    match route {
        RouteKind::LanDirect => "lan-direct",
        RouteKind::PublicDirect => "public-direct",
        RouteKind::Libp2pDcutr => "libp2p-dcutr",
        RouteKind::WebRtcTurn => "webrtc-turn",
        RouteKind::Libp2pRelay => "libp2p-relay",
    }
}

fn route_allowed(route: &RouteKind, allow_relay_fallback: bool) -> bool {
    !matches!(route, RouteKind::Libp2pRelay) || allow_relay_fallback
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

fn direct_endpoint_arg(args: &SendArgs) -> Option<SocketAddr> {
    if args.name.is_some() {
        return None;
    }
    args.args.first().and_then(|value| parse_ip_endpoint(value))
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
        };
        assert!(direct_endpoint_arg(&args).is_none());

        let (_, _, paths) = resolve_named_send(args).unwrap();
        assert_eq!(paths, vec![PathBuf::from("127.0.0.1")]);
    }

    #[test]
    fn partial_named_flags_are_rejected() {
        let name_only = SendArgs {
            args: vec!["file.txt".into()],
            name: Some("river-mango-42".into()),
            code: None,
            compression: CompressionArg::Auto,
            allow_relay_fallback: false,
        };
        assert!(resolve_named_send(name_only).is_err());

        let code_only = SendArgs {
            args: vec!["file.txt".into()],
            name: None,
            code: Some("rose-lime-iris-jade-1234".into()),
            compression: CompressionArg::Auto,
            allow_relay_fallback: false,
        };
        assert!(resolve_named_send(code_only).is_err());
    }

    #[test]
    fn relay_fallback_requires_explicit_opt_in() {
        assert!(!route_allowed(&RouteKind::Libp2pRelay, false));
        assert!(route_allowed(&RouteKind::Libp2pRelay, true));
        assert!(route_allowed(&RouteKind::Libp2pDcutr, false));
    }
}
