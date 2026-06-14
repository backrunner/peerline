use std::{io::ErrorKind, net::SocketAddr, process::Stdio, time::Duration};

use peerline_net::I2pEndpoint;
use tokio::{process::Child, process::Command as TokioCommand, task::JoinHandle};

pub(crate) struct RunningI2p {
    pub(crate) endpoint: I2pEndpoint,
    pub(crate) listener: tokio::net::TcpListener,
    pub(crate) local_addr: SocketAddr,
    _forward: peerline_net::I2pForward,
    _process: Option<I2pProcess>,
}

struct I2pProcess {
    child: Child,
    expect_foreground: bool,
    readers: Vec<JoinHandle<()>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct I2pRouterCommand {
    command: String,
    args: Vec<String>,
    expect_foreground: bool,
}

impl Drop for I2pProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        for reader in &self.readers {
            reader.abort();
        }
    }
}

pub(crate) async fn start_i2p(sam_addr: SocketAddr) -> anyhow::Result<RunningI2p> {
    let mut process = None;
    if !peerline_net::i2p_sam_available(sam_addr).await {
        process = Some(spawn_i2p_router_process(sam_addr).await?);
        wait_for_i2p_sam(sam_addr, process.as_mut()).await?;
    }
    let (listener, local_addr) = peerline_net::bind_i2p_listener().await?;
    let session = peerline_net::create_i2p_stream_session(sam_addr).await?;
    let forward = peerline_net::forward_i2p_to_listener(sam_addr, &session, local_addr).await?;
    let endpoint = I2pEndpoint {
        url: peerline_net::normalize_i2p_url(&session.b32)?,
    };
    Ok(RunningI2p {
        endpoint,
        listener,
        local_addr,
        _forward: forward,
        _process: process,
    })
}

async fn spawn_i2p_router_process(sam_addr: SocketAddr) -> anyhow::Result<I2pProcess> {
    let candidates = i2p_router_command_candidates();
    let mut not_found = Vec::new();
    for candidate in candidates {
        let mut process = TokioCommand::new(&candidate.command);
        process
            .args(&candidate.args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        match process.spawn() {
            Ok(mut child) => {
                tracing::info!(command = %candidate.command, args = ?candidate.args, sam = %sam_addr, "started I2P router command");
                let mut readers = Vec::new();
                if let Some(stdout) = child.stdout.take() {
                    readers.push(crate::spawn_process_output_reader("i2p", "stdout", stdout));
                }
                if let Some(stderr) = child.stderr.take() {
                    readers.push(crate::spawn_process_output_reader("i2p", "stderr", stderr));
                }
                return Ok(I2pProcess {
                    child,
                    expect_foreground: candidate.expect_foreground,
                    readers,
                });
            }
            Err(error) if error.kind() == ErrorKind::NotFound => not_found.push(candidate.command),
            Err(error) => return Err(error.into()),
        }
    }
    anyhow::bail!(
        "no SAM service on {}; install and start I2P/i2pd, or provide --i2p-sam for an existing SAM bridge (tried: {})",
        sam_addr,
        not_found.join(", ")
    )
}

fn i2p_router_command_candidates() -> Vec<I2pRouterCommand> {
    vec![
        I2pRouterCommand {
            command: "i2pd".into(),
            args: Vec::new(),
            expect_foreground: true,
        },
        I2pRouterCommand {
            command: "i2prouter".into(),
            args: vec!["start".into()],
            expect_foreground: false,
        },
    ]
}

async fn wait_for_i2p_sam(
    sam_addr: SocketAddr,
    mut process: Option<&mut I2pProcess>,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        if peerline_net::i2p_sam_available(sam_addr).await {
            return Ok(());
        }
        if let Some(process) = process.as_deref_mut()
            && let Some(status) = process.child.try_wait()?
            && (!status.success() || process.expect_foreground)
        {
            anyhow::bail!("I2P router command exited before SAM became available: {status}");
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "I2P SAM {} did not become available within 90 seconds",
                sam_addr
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::i2p_router_command_candidates;

    #[test]
    fn router_command_metadata_matches_process_lifecycle() {
        let commands = i2p_router_command_candidates();
        assert!(commands[0].expect_foreground);
        assert_eq!(commands[0].command, "i2pd");
        assert!(!commands[1].expect_foreground);
        assert_eq!(commands[1].command, "i2prouter");
        assert_eq!(commands[1].args, vec!["start"]);
    }
}
