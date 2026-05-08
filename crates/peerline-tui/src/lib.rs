use peerline_core::{HumanCode, HumanName, PeerlineEvent, TransferStage};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, Paragraph},
};
use std::{io, time::Duration};
use tokio::sync::mpsc::UnboundedReceiver;

#[derive(Clone, Debug)]
pub struct RecvView {
    pub name: HumanName,
    pub code: HumanCode,
    pub bind: String,
    pub route_status: String,
    pub stage: TransferStage,
    pub progress: Option<(u64, u64)>,
}

impl RecvView {
    pub fn from_events(
        name: HumanName,
        code: HumanCode,
        bind: String,
        events: &[PeerlineEvent],
    ) -> Self {
        let mut stage = TransferStage::Discovering;
        let mut progress = None;
        let mut route_status = "direct TCP ready; libp2p discovery publishing".to_string();
        for event in events {
            match event {
                PeerlineEvent::StageChanged(next) => stage = next.clone(),
                PeerlineEvent::TransferStarted { files, bytes, .. } => {
                    route_status = format!("{files} file(s), {bytes} bytes");
                }
                PeerlineEvent::Progress {
                    bytes_done,
                    bytes_total,
                    ..
                } => progress = Some((*bytes_done, *bytes_total)),
                PeerlineEvent::Message(message) => route_status = message.clone(),
            }
        }
        Self {
            name,
            code,
            bind,
            route_status,
            stage,
            progress,
        }
    }
}

pub async fn render_once(
    mut view: RecvView,
    mut events: UnboundedReceiver<PeerlineEvent>,
) -> anyhow::Result<()> {
    crossterm::terminal::enable_raw_mode()?;
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, crossterm::terminal::EnterAlternateScreen)?;
    let _cleanup = TerminalCleanup;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let mut tick = tokio::time::interval(Duration::from_millis(100));

    loop {
        terminal.draw(|frame| draw_view(frame, &view))?;

        tokio::select! {
            maybe_event = events.recv() => {
                match maybe_event {
                    Some(event) => {
                        if apply_event(&mut view, event) {
                            terminal.draw(|frame| draw_view(frame, &view))?;
                            break;
                        }
                    }
                    None => {
                        if matches!(view.stage, TransferStage::Complete | TransferStage::Failed(_)) {
                            break;
                        }
                    }
                }
            }
            _ = tick.tick() => {
                while crossterm::event::poll(Duration::from_millis(0))? {
                    if let crossterm::event::Event::Key(key) = crossterm::event::read()? {
                        if matches!(
                            key.code,
                            crossterm::event::KeyCode::Char('q') | crossterm::event::KeyCode::Esc
                        ) {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn apply_event(view: &mut RecvView, event: PeerlineEvent) -> bool {
    match event {
        PeerlineEvent::StageChanged(next) => {
            let done = matches!(next, TransferStage::Complete | TransferStage::Failed(_));
            view.stage = next;
            done
        }
        PeerlineEvent::TransferStarted { files, bytes, .. } => {
            view.route_status = format!("{files} file(s), {bytes} bytes");
            view.progress = Some((0, bytes));
            false
        }
        PeerlineEvent::Progress {
            bytes_done,
            bytes_total,
            ..
        } => {
            view.progress = Some((bytes_done, bytes_total));
            false
        }
        PeerlineEvent::Message(message) => {
            view.route_status = message;
            false
        }
    }
}

fn draw_view(frame: &mut ratatui::Frame<'_>, view: &RecvView) {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(7),
            Constraint::Length(5),
            Constraint::Length(3),
            Constraint::Min(1),
        ])
        .split(area);

    let identity = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("name  ", Style::default().fg(Color::Gray)),
            Span::styled(
                view.name.as_str(),
                Style::default().add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("code  ", Style::default().fg(Color::Gray)),
            Span::styled(view.code.as_str(), Style::default().fg(Color::Yellow)),
        ]),
        Line::from(vec![
            Span::styled("listen ", Style::default().fg(Color::Gray)),
            Span::raw(&view.bind),
        ]),
    ])
    .block(Block::new().title("Peerline recv").borders(Borders::ALL));
    frame.render_widget(identity, chunks[0]);

    let status = Paragraph::new(vec![
        Line::from(format!("stage: {:?}", view.stage)),
        Line::from(format!("route: {}", view.route_status)),
    ])
    .block(Block::new().title("Status").borders(Borders::ALL));
    frame.render_widget(status, chunks[1]);

    let ratio = view
        .progress
        .map(|(done, total)| {
            if total == 0 {
                0.0
            } else {
                done as f64 / total as f64
            }
        })
        .unwrap_or(0.0);
    let progress = Gauge::default()
        .block(Block::new().title("Transfer").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Cyan))
        .ratio(ratio.clamp(0.0, 1.0));
    frame.render_widget(progress, chunks[2]);
}

struct TerminalCleanup;

impl Drop for TerminalCleanup {
    fn drop(&mut self) {
        let mut stdout = io::stdout();
        let _ = crossterm::execute!(stdout, crossterm::terminal::LeaveAlternateScreen);
        let _ = crossterm::terminal::disable_raw_mode();
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
}
