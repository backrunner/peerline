use peerline_core::{HumanCode, PeerlineEvent, PeerlineLogLevel, TransferStage};
use std::{
    fmt,
    future::Future,
    io::{self, IsTerminal, Write},
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tracing::{Event, Level, Subscriber, field::Field};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

pub(super) fn init_tracing(debug: bool) {
    tracing_subscriber::registry()
        .with(tracing_filter(debug))
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(TuiAwareStderr)
                .with_target(true),
        )
        .with(ActivityLogLayer)
        .init();
}

fn tracing_filter(debug: bool) -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::new(if debug {
            "error,peerline=debug"
        } else {
            "error,peerline=info"
        })
    })
}

static ACTIVITY_LOG_SENDERS: OnceLock<Mutex<Vec<mpsc::UnboundedSender<PeerlineEvent>>>> =
    OnceLock::new();
static TERMINAL_UI_ACTIVE: AtomicBool = AtomicBool::new(false);

struct TerminalUiLogGuard;

impl TerminalUiLogGuard {
    fn activate() -> Self {
        TERMINAL_UI_ACTIVE.store(true, Ordering::SeqCst);
        Self
    }
}

impl Drop for TerminalUiLogGuard {
    fn drop(&mut self) {
        TERMINAL_UI_ACTIVE.store(false, Ordering::SeqCst);
    }
}

pub(super) fn spawn_terminal_ui<F>(future: F) -> JoinHandle<anyhow::Result<()>>
where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let guard = TerminalUiLogGuard::activate();
    tokio::spawn(async move {
        let _guard = guard;
        future.await
    })
}

struct TuiAwareStderr;

enum TuiAwareStderrWriter {
    Stderr(io::Stderr),
    Sink(io::Sink),
}

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for TuiAwareStderr {
    type Writer = TuiAwareStderrWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        if TERMINAL_UI_ACTIVE.load(Ordering::SeqCst) && io::stderr().is_terminal() {
            TuiAwareStderrWriter::Sink(io::sink())
        } else {
            TuiAwareStderrWriter::Stderr(io::stderr())
        }
    }
}

impl Write for TuiAwareStderrWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Stderr(stderr) => stderr.write(buf),
            Self::Sink(sink) => sink.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Stderr(stderr) => stderr.flush(),
            Self::Sink(sink) => sink.flush(),
        }
    }
}

pub(super) fn register_activity_log_sender(sender: &mpsc::UnboundedSender<PeerlineEvent>) {
    let Ok(mut senders) = ACTIVITY_LOG_SENDERS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
    else {
        return;
    };
    senders.push(sender.clone());
}

struct ActivityLogLayer;

impl<S> Layer<S> for ActivityLogLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let Some(senders) = ACTIVITY_LOG_SENDERS.get() else {
            return;
        };
        let metadata = event.metadata();
        let mut visitor = LogFieldVisitor::default();
        event.record(&mut visitor);
        let mut message = visitor
            .message
            .unwrap_or_else(|| metadata.name().to_string());
        if !visitor.fields.is_empty() {
            message.push(' ');
            message.push_str(&visitor.fields.join(" "));
        }
        let log_event = PeerlineEvent::Log {
            level: peerline_log_level(metadata.level()),
            target: metadata.target().to_string(),
            message,
        };

        let Ok(mut senders) = senders.lock() else {
            return;
        };
        senders.retain(|sender| sender.send(log_event.clone()).is_ok());
    }
}

#[derive(Default)]
struct LogFieldVisitor {
    message: Option<String>,
    fields: Vec<String>,
}

impl tracing::field::Visit for LogFieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_value(field, value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record_value(field, format!("{value:?}"));
    }
}

impl LogFieldVisitor {
    fn record_value(&mut self, field: &Field, value: String) {
        if field.name() == "message" {
            self.message = Some(value);
        } else {
            self.fields.push(format!("{}={}", field.name(), value));
        }
    }
}

fn peerline_log_level(level: &Level) -> PeerlineLogLevel {
    match *level {
        Level::ERROR => PeerlineLogLevel::Error,
        Level::WARN => PeerlineLogLevel::Warn,
        Level::INFO => PeerlineLogLevel::Info,
        Level::DEBUG => PeerlineLogLevel::Debug,
        Level::TRACE => PeerlineLogLevel::Trace,
    }
}

pub(super) struct SendUi {
    pub(super) events: Option<mpsc::UnboundedSender<PeerlineEvent>>,
    pub(super) task: Option<JoinHandle<anyhow::Result<()>>>,
    pub(super) quit_rx: Option<watch::Receiver<bool>>,
    pub(super) retry_rx: Option<mpsc::UnboundedReceiver<()>>,
}

pub(super) fn spawn_send_tui(
    target_label: &str,
    target: String,
    code: HumanCode,
    route_status: String,
    retry_enabled: bool,
) -> SendUi {
    if !std::io::stdout().is_terminal() {
        return SendUi {
            events: None,
            task: None,
            quit_rx: None,
            retry_rx: None,
        };
    }

    let (sender, receiver) = mpsc::unbounded_channel();
    register_activity_log_sender(&sender);
    let (quit_tx, quit_rx) = watch::channel(false);
    let (retry_tx, retry_rx) = if retry_enabled {
        let (tx, rx) = mpsc::unbounded_channel();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    let view = peerline_tui::SendView {
        target_label: target_label.to_string(),
        target,
        code,
        route_status,
        stage: TransferStage::Discovering,
        progress: None,
    };
    let task = spawn_terminal_ui(peerline_tui::render_send_once_with_controls(
        view,
        receiver,
        Some(quit_tx),
        retry_tx,
    ));
    SendUi {
        events: Some(sender),
        task: Some(task),
        quit_rx: Some(quit_rx),
        retry_rx,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_ui_log_guard_resets_formatter_suppression() {
        TERMINAL_UI_ACTIVE.store(false, Ordering::SeqCst);

        {
            let _guard = TerminalUiLogGuard::activate();
            assert!(TERMINAL_UI_ACTIVE.load(Ordering::SeqCst));
        }

        assert!(!TERMINAL_UI_ACTIVE.load(Ordering::SeqCst));
    }
}
