use peerline_core::{
    ConnectionRoute, HumanCode, HumanName, PeerlineEvent, PeerlineLogLevel, TransferStage,
};
use ratatui::style::{Color, Modifier, Style};
use tokio::sync::mpsc::UnboundedReceiver;

mod dashboard;

#[derive(Clone, Debug)]
pub struct RecvView {
    pub name: HumanName,
    pub code: HumanCode,
    pub bind: String,
    pub route_status: String,
    pub stage: TransferStage,
    pub progress: Option<(u64, u64)>,
}

#[derive(Clone, Debug)]
pub struct SendView {
    pub target_label: String,
    pub target: String,
    pub code: HumanCode,
    pub route_status: String,
    pub stage: TransferStage,
    pub progress: Option<(u64, u64)>,
}

trait TransferView {
    fn stage_mut(&mut self) -> &mut TransferStage;
    fn route_status_mut(&mut self) -> &mut String;
    fn progress_mut(&mut self) -> &mut Option<(u64, u64)>;
}

impl TransferView for RecvView {
    fn stage_mut(&mut self) -> &mut TransferStage {
        &mut self.stage
    }

    fn route_status_mut(&mut self) -> &mut String {
        &mut self.route_status
    }

    fn progress_mut(&mut self) -> &mut Option<(u64, u64)> {
        &mut self.progress
    }
}

impl TransferView for SendView {
    fn stage_mut(&mut self) -> &mut TransferStage {
        &mut self.stage
    }

    fn route_status_mut(&mut self) -> &mut String {
        &mut self.route_status
    }

    fn progress_mut(&mut self) -> &mut Option<(u64, u64)> {
        &mut self.progress
    }
}

impl RecvView {
    pub fn from_events(
        name: HumanName,
        code: HumanCode,
        bind: String,
        events: &[PeerlineEvent],
    ) -> Self {
        let mut view = Self {
            name,
            code,
            bind,
            route_status: "direct TCP ready; libp2p TCP/QUIC/WebRTC/relay ready".to_string(),
            stage: TransferStage::Discovering,
            progress: None,
        };
        fold_events(&mut view, events);
        view
    }
}

impl SendView {
    pub fn from_events(
        target_label: impl Into<String>,
        target: impl Into<String>,
        code: HumanCode,
        route_status: impl Into<String>,
        events: &[PeerlineEvent],
    ) -> Self {
        let mut view = Self {
            target_label: target_label.into(),
            target: target.into(),
            code,
            route_status: route_status.into(),
            stage: TransferStage::Discovering,
            progress: None,
        };
        fold_events(&mut view, events);
        view
    }
}

pub async fn render_once(
    view: RecvView,
    events: UnboundedReceiver<PeerlineEvent>,
) -> anyhow::Result<()> {
    render_once_with_quit(view, events, None).await
}

pub async fn render_once_with_quit(
    view: RecvView,
    events: UnboundedReceiver<PeerlineEvent>,
    quit_signal: Option<tokio::sync::watch::Sender<bool>>,
) -> anyhow::Result<()> {
    dashboard::run_recv(view, events, quit_signal).await
}

pub async fn render_send_once(
    view: SendView,
    events: UnboundedReceiver<PeerlineEvent>,
) -> anyhow::Result<()> {
    render_send_once_with_quit(view, events, None).await
}

pub async fn render_send_once_with_quit(
    view: SendView,
    events: UnboundedReceiver<PeerlineEvent>,
    quit_signal: Option<tokio::sync::watch::Sender<bool>>,
) -> anyhow::Result<()> {
    render_send_once_with_controls(view, events, quit_signal, None).await
}

pub async fn render_send_once_with_controls(
    view: SendView,
    events: UnboundedReceiver<PeerlineEvent>,
    quit_signal: Option<tokio::sync::watch::Sender<bool>>,
    retry_signal: Option<tokio::sync::mpsc::UnboundedSender<()>>,
) -> anyhow::Result<()> {
    dashboard::run_send(view, events, quit_signal, retry_signal).await
}

fn fold_events<T: TransferView>(view: &mut T, events: &[PeerlineEvent]) {
    for event in events {
        let _ = apply_event(view, event.clone());
    }
}

fn apply_event<T: TransferView>(view: &mut T, event: PeerlineEvent) -> bool {
    match event {
        PeerlineEvent::Shutdown => true,
        PeerlineEvent::StageChanged(next) => {
            let done = matches!(next, TransferStage::Complete | TransferStage::Failed(_));
            *view.stage_mut() = next;
            done
        }
        PeerlineEvent::TransferStarted {
            files,
            bytes,
            resume_offset,
            ..
        } => {
            *view.route_status_mut() = format!("{files} file(s), {bytes} bytes");
            *view.progress_mut() = Some((resume_offset, bytes));
            false
        }
        PeerlineEvent::Progress {
            bytes_done,
            bytes_total,
            ..
        } => {
            *view.progress_mut() = Some((bytes_done, bytes_total));
            false
        }
        PeerlineEvent::Message(message) => {
            *view.route_status_mut() = message;
            false
        }
        PeerlineEvent::Log { level, message, .. } => {
            if matches!(level, PeerlineLogLevel::Error | PeerlineLogLevel::Warn) {
                *view.route_status_mut() = message;
            }
            false
        }
    }
}

fn stage_label(stage: &TransferStage) -> String {
    match stage {
        TransferStage::Discovering => "discovering".into(),
        TransferStage::Connecting(route) => {
            format!("connecting via {}", connection_route_label(route))
        }
        TransferStage::Authenticating => "authenticating".into(),
        TransferStage::ReceivingManifest => "receiving manifest".into(),
        TransferStage::Transferring => "transferring".into(),
        TransferStage::Verifying => "verifying".into(),
        TransferStage::Complete => "complete".into(),
        TransferStage::Failed(error) => format!("failed: {error}"),
    }
}

fn stage_style(stage: &TransferStage) -> Style {
    match stage {
        TransferStage::Complete => Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD),
        TransferStage::Failed(_) => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        TransferStage::Authenticating | TransferStage::Verifying => {
            Style::default().fg(Color::Yellow)
        }
        TransferStage::Transferring => Style::default().fg(Color::Green),
        TransferStage::Discovering => Style::default().fg(Color::Blue),
        TransferStage::Connecting(_) | TransferStage::ReceivingManifest => {
            Style::default().fg(Color::Cyan)
        }
    }
}

fn connection_route_label(route: &ConnectionRoute) -> &'static str {
    match route {
        ConnectionRoute::LanDirect => "lan-direct",
        ConnectionRoute::PublicDirect => "public-direct",
        ConnectionRoute::PublicTunnel => "public-tunnel",
        ConnectionRoute::TorOnion => "tor-onion",
        ConnectionRoute::Libp2pQuic => "libp2p-quic",
        ConnectionRoute::Libp2pDcutr => "libp2p-dcutr",
        ConnectionRoute::Libp2pRelay => "libp2p-relay",
        ConnectionRoute::WebRtcDirect => "webrtc-direct",
        ConnectionRoute::WebRtcTurn => "webrtc-turn",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use peerline_core::{ConnectionRoute, PeerlineEvent};

    #[test]
    fn recv_view_uses_latest_progress_and_stage() {
        let name = HumanName::parse("river-mango-42").unwrap();
        let code = HumanCode::parse("rose-lime-iris-jade-1234").unwrap();
        let events = vec![
            PeerlineEvent::StageChanged(TransferStage::Connecting(ConnectionRoute::LanDirect)),
            PeerlineEvent::Progress {
                id: peerline_core::manifest::TransferId::random(),
                bytes_done: 10,
                bytes_total: 100,
            },
            PeerlineEvent::StageChanged(TransferStage::Transferring),
            PeerlineEvent::Progress {
                id: peerline_core::manifest::TransferId::random(),
                bytes_done: 60,
                bytes_total: 100,
            },
        ];

        let view = RecvView::from_events(name, code, "127.0.0.1:43117".into(), &events);
        assert!(matches!(view.stage, TransferStage::Transferring));
        assert_eq!(view.progress, Some((60, 100)));
    }

    #[test]
    fn send_view_uses_latest_progress_and_stage() {
        let code = HumanCode::parse("rose-lime-iris-jade-1234").unwrap();
        let events = vec![
            PeerlineEvent::StageChanged(TransferStage::Discovering),
            PeerlineEvent::TransferStarted {
                id: peerline_core::manifest::TransferId::random(),
                peer: "203.0.113.7:43117".into(),
                files: 2,
                bytes: 300,
                resume_offset: 0,
            },
            PeerlineEvent::StageChanged(TransferStage::Connecting(ConnectionRoute::PublicDirect)),
            PeerlineEvent::Progress {
                id: peerline_core::manifest::TransferId::random(),
                bytes_done: 200,
                bytes_total: 300,
            },
        ];

        let view = SendView::from_events(
            "peer",
            "203.0.113.7:43117",
            code,
            "discovering routes through libp2p Kademlia/mDNS...",
            &events,
        );
        assert!(matches!(
            view.stage,
            TransferStage::Connecting(ConnectionRoute::PublicDirect)
        ));
        assert_eq!(view.progress, Some((200, 300)));
        assert_eq!(view.route_status, "2 file(s), 300 bytes");
    }
}
