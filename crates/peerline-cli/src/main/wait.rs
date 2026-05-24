use futures::StreamExt;
use peerline_core::{PeerlineEvent, TransferStage};
use peerline_net::ReceivedTransfer;
use std::{future::Future, pin::Pin, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time,
};

pub(super) enum RecvOutcome<T> {
    Completed(T),
    Quit,
    IdleTimeout,
}

pub(super) fn parse_idle_timeout_minutes(value: &str) -> Result<f64, String> {
    let minutes = value
        .parse::<f64>()
        .map_err(|error| format!("invalid minute value: {error}"))?;
    if !minutes.is_finite() || minutes < 0.0 {
        return Err("idle timeout must be a non-negative number of minutes".into());
    }
    Ok(minutes)
}

pub(super) fn recv_idle_timeout(minutes: f64) -> Option<Duration> {
    if minutes == 0.0 {
        None
    } else {
        Some(Duration::from_secs_f64(minutes * 60.0))
    }
}

pub(super) fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    if seconds >= 60 && seconds.is_multiple_of(60) {
        format!("{} min", seconds / 60)
    } else if seconds > 0 {
        format!("{seconds}s")
    } else {
        format!("{}ms", duration.as_millis())
    }
}

pub(super) fn spawn_event_fanout(
    mut receiver: mpsc::UnboundedReceiver<PeerlineEvent>,
    tui_sender: Option<mpsc::UnboundedSender<PeerlineEvent>>,
    activity_sender: mpsc::UnboundedSender<()>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            if is_recv_activity_event(&event) {
                let _ = activity_sender.send(());
            }
            if let Some(sender) = tui_sender.as_ref() {
                let _ = sender.send(event);
            }
        }
    })
}

fn is_recv_activity_event(event: &PeerlineEvent) -> bool {
    matches!(
        event,
        PeerlineEvent::TransferStarted { .. }
            | PeerlineEvent::Progress { .. }
            | PeerlineEvent::StageChanged(
                TransferStage::ReceivingManifest
                    | TransferStage::Transferring
                    | TransferStage::Verifying
                    | TransferStage::Complete
                    | TransferStage::Failed(_)
            )
    )
}

pub(super) async fn wait_for_recv_activity<F, T>(
    future: F,
    quit_rx: &mut Option<watch::Receiver<bool>>,
    activity_rx: &mut mpsc::UnboundedReceiver<()>,
    idle_timeout: Option<Duration>,
) -> anyhow::Result<RecvOutcome<T>>
where
    F: Future<Output = anyhow::Result<T>>,
{
    tokio::pin!(future);
    let mut idle_deadline = idle_timeout.map(|timeout| time::Instant::now() + timeout);
    let mut activity_open = true;

    loop {
        let quit_future = async {
            if let Some(rx) = quit_rx.as_mut() {
                wait_for_quit(rx).await
            } else {
                std::future::pending::<bool>().await
            }
        };
        let idle_future = async {
            if let Some(deadline) = idle_deadline {
                time::sleep_until(deadline).await;
            } else {
                std::future::pending::<()>().await;
            }
        };

        tokio::select! {
            result = &mut future => {
                return result.map(RecvOutcome::Completed);
            }
            quit = quit_future => {
                if quit {
                    return Ok(RecvOutcome::Quit);
                }
                *quit_rx = None;
            }
            activity = activity_rx.recv(), if activity_open => {
                if activity.is_some() {
                    idle_deadline = idle_timeout.map(|timeout| time::Instant::now() + timeout);
                } else {
                    activity_open = false;
                }
            }
            _ = idle_future, if idle_timeout.is_some() => {
                return Ok(RecvOutcome::IdleTimeout);
            }
        }
    }
}

pub(super) enum TaskOutcome<T> {
    Completed(T),
    Quit,
}

pub(super) enum RetryDecision {
    Retry,
    Quit,
}

pub(super) async fn wait_with_quit<F, T>(
    future: F,
    quit_rx: &mut Option<watch::Receiver<bool>>,
) -> anyhow::Result<TaskOutcome<T>>
where
    F: Future<Output = anyhow::Result<T>>,
{
    tokio::pin!(future);

    loop {
        if let Some(rx) = quit_rx.as_mut() {
            tokio::select! {
                result = &mut future => {
                    return result.map(TaskOutcome::Completed);
                }
                quit = wait_for_quit(rx) => {
                    if quit {
                        return Ok(TaskOutcome::Quit);
                    }
                    *quit_rx = None;
                }
            }
        } else {
            return future.await.map(TaskOutcome::Completed);
        }
    }
}

async fn wait_for_quit(rx: &mut watch::Receiver<bool>) -> bool {
    match rx.changed().await {
        Ok(()) => *rx.borrow(),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    use tokio::sync::watch;

    #[tokio::test]
    async fn wait_with_quit_drops_pending_future_when_quit_is_signaled() {
        struct DropFlag(Arc<AtomicBool>);

        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let guard = DropFlag(dropped.clone());
        let (quit_tx, quit_rx) = watch::channel(false);
        let mut quit_rx = Some(quit_rx);

        let future = async move {
            let _guard = guard;
            std::future::pending::<anyhow::Result<()>>().await
        };

        let send_quit = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(25)).await;
            let _ = quit_tx.send(true);
        });

        let outcome = wait_with_quit(future, &mut quit_rx).await;
        send_quit.await.unwrap();

        assert!(matches!(outcome, Ok(TaskOutcome::Quit)));
        assert!(dropped.load(Ordering::SeqCst));
    }
}

pub(super) async fn wait_for_retry_or_quit(
    quit_rx: &mut Option<watch::Receiver<bool>>,
    retry_rx: &mut Option<mpsc::UnboundedReceiver<()>>,
) -> RetryDecision {
    loop {
        let quit_future = async {
            if let Some(rx) = quit_rx.as_mut() {
                wait_for_quit(rx).await
            } else {
                std::future::pending::<bool>().await
            }
        };
        let retry_future = async {
            if let Some(rx) = retry_rx.as_mut() {
                rx.recv().await.is_some()
            } else {
                std::future::pending::<bool>().await
            }
        };

        tokio::select! {
            quit = quit_future => {
                if quit {
                    return RetryDecision::Quit;
                }
                *quit_rx = None;
            }
            retry = retry_future => {
                if retry {
                    return RetryDecision::Retry;
                }
                *retry_rx = None;
                if quit_rx.is_none() {
                    return RetryDecision::Quit;
                }
            }
        }
    }
}

pub(super) fn drain_retry_signals(retry_rx: &mut Option<mpsc::UnboundedReceiver<()>>) {
    if let Some(rx) = retry_rx.as_mut() {
        while rx.try_recv().is_ok() {}
    }
}

pub(super) struct ReceiverPath<'a> {
    future: Pin<Box<dyn Future<Output = (&'static str, anyhow::Result<ReceivedTransfer>)> + 'a>>,
}

impl<'a> ReceiverPath<'a> {
    pub(super) fn new<F>(label: &'static str, future: F) -> Self
    where
        F: Future<Output = anyhow::Result<ReceivedTransfer>> + 'a,
    {
        Self {
            future: Box::pin(async move { (label, future.await) }),
        }
    }
}

pub(super) async fn wait_for_receiver(
    paths: Vec<ReceiverPath<'_>>,
) -> anyhow::Result<ReceivedTransfer> {
    let mut last_error = None;
    let mut pending = paths
        .into_iter()
        .map(|path| path.future)
        .collect::<futures::stream::FuturesUnordered<_>>();

    while let Some((label, result)) = pending.next().await {
        match result {
            Ok(received) => return Ok(received),
            Err(error) => {
                tracing::warn!(%error, path = label, "receiver path stopped");
                last_error = Some(error);
            }
        }
    }

    Err(last_error
        .unwrap_or_else(|| anyhow::anyhow!("receiver stopped without a completed transfer")))
}
