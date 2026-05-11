use super::{RecvView, SendView, stage_label, stage_style};
use peerline_core::{PeerlineEvent, PeerlineLogLevel, TransferId, TransferStage};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, List, ListItem, Paragraph, Row, Table, Wrap},
};
use std::{
    collections::VecDeque,
    io,
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc::UnboundedReceiver, watch},
    time,
};

const MAX_LOGS: usize = 64;
const MAX_TRANSFERS: usize = 32;

pub async fn run_recv(
    view: RecvView,
    events: UnboundedReceiver<PeerlineEvent>,
    quit_signal: Option<watch::Sender<bool>>,
) -> anyhow::Result<()> {
    run_dashboard(Dashboard::new_recv(view), events, quit_signal).await
}

pub async fn run_send(
    view: SendView,
    events: UnboundedReceiver<PeerlineEvent>,
    quit_signal: Option<watch::Sender<bool>>,
) -> anyhow::Result<()> {
    run_dashboard(Dashboard::new_send(view), events, quit_signal).await
}

struct Dashboard {
    kind: DashboardKind,
    stage: TransferStage,
    status: String,
    progress: Option<(u64, u64)>,
    active_transfer: Option<TransferId>,
    transfers: Vec<TransferRow>,
    logs: VecDeque<LogEntry>,
    started_at: Instant,
}

enum DashboardKind {
    Recv {
        name: String,
        code: String,
        bind: String,
    },
    Send {
        target_label: String,
        target: String,
        code: String,
    },
}

#[derive(Clone)]
struct TransferRow {
    id: TransferId,
    peer: String,
    files: usize,
    bytes: u64,
    progress: Option<(u64, u64)>,
    stage: TransferStage,
    status: String,
    updated_at: Instant,
}

#[derive(Clone, Copy)]
enum LogKind {
    Info,
    Success,
    Warn,
    Error,
    Status,
}

#[derive(Clone)]
struct LogEntry {
    elapsed: Duration,
    kind: LogKind,
    source: Option<String>,
    text: String,
}

impl Dashboard {
    fn new_recv(view: RecvView) -> Self {
        Self {
            kind: DashboardKind::Recv {
                name: view.name.to_string(),
                code: view.code.to_string(),
                bind: view.bind,
            },
            stage: view.stage,
            status: view.route_status,
            progress: view.progress,
            active_transfer: None,
            transfers: Vec::new(),
            logs: VecDeque::new(),
            started_at: Instant::now(),
        }
    }

    fn new_send(view: SendView) -> Self {
        Self {
            kind: DashboardKind::Send {
                target_label: view.target_label,
                target: view.target,
                code: view.code.to_string(),
            },
            stage: view.stage,
            status: view.route_status,
            progress: view.progress,
            active_transfer: None,
            transfers: Vec::new(),
            logs: VecDeque::new(),
            started_at: Instant::now(),
        }
    }

    fn apply_event(&mut self, event: PeerlineEvent) -> bool {
        let now = Instant::now();
        match event {
            PeerlineEvent::StageChanged(next) => {
                self.stage = next.clone();
                self.status = stage_label(&next);
                if let Some(id) = self.active_transfer {
                    let progress = self.progress;
                    let peer = self.transfer_mut(id).map(|row| {
                        row.stage = next.clone();
                        row.status = stage_label(&next);
                        row.updated_at = now;
                        row.progress = progress;
                        row.peer.clone()
                    });
                    self.push_log(stage_kind(&next), stage_label(&next), peer);
                } else {
                    self.push_log(stage_kind(&next), stage_label(&next), None);
                }
                if matches!(next, TransferStage::Complete | TransferStage::Failed(_)) {
                    self.active_transfer = None;
                    return true;
                }
                false
            }
            PeerlineEvent::TransferStarted {
                id,
                peer,
                files,
                bytes,
            } => {
                self.active_transfer = Some(id);
                self.status = format!("{files} file(s) from {peer}");
                self.progress = Some((0, bytes));
                self.transfers.push(TransferRow {
                    id,
                    peer: peer.clone(),
                    files,
                    bytes,
                    progress: Some((0, bytes)),
                    stage: self.stage.clone(),
                    status: stage_label(&self.stage),
                    updated_at: now,
                });
                if self.transfers.len() > MAX_TRANSFERS {
                    self.transfers.remove(0);
                }
                self.push_log(
                    LogKind::Info,
                    format!(
                        "started {files} file(s) from {peer} ({})",
                        format_bytes(bytes)
                    ),
                    Some(peer),
                );
                false
            }
            PeerlineEvent::Progress {
                id,
                bytes_done,
                bytes_total,
            } => {
                let progress = Some((bytes_done, bytes_total));
                let status = if let Some(row) = self.transfer_mut(id) {
                    row.progress = progress;
                    row.status = format_progress(bytes_done, bytes_total);
                    row.updated_at = now;
                    row.status.clone()
                } else {
                    format_progress(bytes_done, bytes_total)
                };
                self.status = status;
                self.progress = progress;
                false
            }
            PeerlineEvent::Message(message) => {
                let peer = self
                    .active_transfer
                    .and_then(|id| self.transfer(id).map(|row| row.peer.clone()));
                let log_source = peer.clone();
                self.push_log(LogKind::Info, message.clone(), log_source);
                if let Some(id) = self.active_transfer {
                    if let Some(row) = self.transfer_mut(id) {
                        row.status = message.clone();
                        row.updated_at = now;
                    }
                    self.status = message;
                } else {
                    self.status = message;
                }
                false
            }
            PeerlineEvent::Log {
                level,
                target,
                message,
            } => {
                let kind = log_level_kind(&level);
                self.push_log(kind, message.clone(), Some(target));
                if matches!(level, PeerlineLogLevel::Error | PeerlineLogLevel::Warn) {
                    if let Some(id) = self.active_transfer
                        && let Some(row) = self.transfer_mut(id)
                    {
                        row.status = message.clone();
                        row.updated_at = now;
                    }
                    self.status = message;
                }
                false
            }
        }
    }

    fn draw(&self, frame: &mut ratatui::Frame<'_>) {
        let area = frame.area();
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(6),
                Constraint::Min(10),
                Constraint::Length(8),
                Constraint::Length(1),
            ])
            .split(area);

        self.draw_header(frame, chunks[0]);
        self.draw_transfers(frame, chunks[1]);
        self.draw_logs(frame, chunks[2]);
        self.draw_footer(frame, chunks[3]);
    }

    fn draw_header(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let mut lines = vec![
            self.title_line(),
            self.meta_line(),
            self.stage_line(),
            self.count_line(),
        ];
        if lines.is_empty() {
            lines.push(Line::from(""));
        }
        let header = Paragraph::new(lines)
            .wrap(Wrap { trim: true })
            .block(Block::new().title(self.title()).borders(Borders::ALL));
        frame.render_widget(header, area);
    }

    fn draw_transfers(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let block = Block::new().title("Transfers").borders(Borders::ALL);
        if self.transfers.is_empty() {
            let body = Paragraph::new("waiting for the first peer")
                .style(Style::default().fg(Color::DarkGray))
                .block(block);
            frame.render_widget(body, area);
            return;
        }

        let rows = self
            .transfers
            .iter()
            .rev()
            .map(|transfer| {
                let active = self.active_transfer == Some(transfer.id);
                let style = transfer_style(&transfer.stage, active);
                Row::new(vec![
                    Cell::from(truncate_middle(&transfer.peer, 24)),
                    Cell::from(transfer.files.to_string()),
                    Cell::from(format_bytes(transfer.bytes)),
                    Cell::from(transfer.progress.map_or_else(
                        || "0%".to_string(),
                        |(done, total)| format_progress(done, total),
                    )),
                    Cell::from(stage_label(&transfer.stage)),
                    Cell::from(truncate_middle(&transfer.status, 32)),
                ])
                .style(style)
            })
            .collect::<Vec<_>>();

        let table = Table::new(
            rows,
            [
                Constraint::Length(24),
                Constraint::Length(6),
                Constraint::Length(12),
                Constraint::Length(20),
                Constraint::Length(14),
                Constraint::Min(16),
            ],
        )
        .header(
            Row::new(vec![
                "peer", "files", "bytes", "progress", "stage", "status",
            ])
            .style(
                Style::default()
                    .fg(Color::Gray)
                    .add_modifier(Modifier::BOLD),
            ),
        )
        .block(block)
        .column_spacing(1);

        frame.render_widget(table, area);
    }

    fn draw_logs(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let block = Block::new().title("Activity").borders(Borders::ALL);
        if self.logs.is_empty() {
            let empty = Paragraph::new("waiting for events")
                .style(Style::default().fg(Color::DarkGray))
                .block(block);
            frame.render_widget(empty, area);
            return;
        }

        let visible_rows = area.height.saturating_sub(2) as usize;
        let mut items = self
            .logs
            .iter()
            .rev()
            .take(visible_rows.max(1))
            .cloned()
            .collect::<Vec<_>>();
        items.reverse();

        let list = List::new(items.into_iter().map(render_log_item).collect::<Vec<_>>())
            .block(block)
            .highlight_style(Style::default().add_modifier(Modifier::BOLD));
        frame.render_widget(list, area);
    }

    fn draw_footer(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let footer = Paragraph::new(Line::from(vec![
            Span::styled(
                "q / Esc",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" quit", Style::default().fg(Color::Gray)),
        ]));
        frame.render_widget(footer, area);
    }

    fn title(&self) -> &'static str {
        match self.kind {
            DashboardKind::Recv { .. } => "Peerline receive",
            DashboardKind::Send { .. } => "Peerline send",
        }
    }

    fn title_line(&self) -> Line<'_> {
        Line::from(vec![
            Span::styled(
                self.title(),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(
                format!("{} transfer(s)", self.transfers.len()),
                Style::default().fg(Color::Gray),
            ),
        ])
    }

    fn meta_line(&self) -> Line<'_> {
        match &self.kind {
            DashboardKind::Recv { name, code, bind } => Line::from(vec![
                label_span("name"),
                Span::styled(name.as_str(), Style::default().add_modifier(Modifier::BOLD)),
                Span::raw("  "),
                label_span("code"),
                Span::styled(code.as_str(), Style::default().fg(Color::Yellow)),
                Span::raw("  "),
                label_span("listen"),
                Span::styled(bind.as_str(), Style::default().fg(Color::Cyan)),
            ]),
            DashboardKind::Send {
                target_label,
                target,
                code,
            } => Line::from(vec![
                label_span(target_label),
                Span::styled(
                    target.as_str(),
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::raw("  "),
                label_span("code"),
                Span::styled(code.as_str(), Style::default().fg(Color::Yellow)),
            ]),
        }
    }

    fn stage_line(&self) -> Line<'_> {
        Line::from(vec![
            label_span("stage"),
            Span::styled(stage_label(&self.stage), stage_style(&self.stage)),
            Span::raw("  "),
            label_span("status"),
            Span::styled(self.status.as_str(), Style::default().fg(Color::White)),
        ])
    }

    fn count_line(&self) -> Line<'_> {
        let active = self
            .transfers
            .iter()
            .filter(|row| {
                !matches!(
                    row.stage,
                    TransferStage::Complete | TransferStage::Failed(_)
                )
            })
            .count();
        let done = self
            .transfers
            .iter()
            .filter(|row| matches!(row.stage, TransferStage::Complete))
            .count();
        let failed = self
            .transfers
            .iter()
            .filter(|row| matches!(row.stage, TransferStage::Failed(_)))
            .count();

        Line::from(vec![
            label_span("active"),
            Span::styled(active.to_string(), Style::default().fg(Color::Cyan)),
            Span::raw("  "),
            label_span("done"),
            Span::styled(done.to_string(), Style::default().fg(Color::Green)),
            Span::raw("  "),
            label_span("failed"),
            Span::styled(failed.to_string(), Style::default().fg(Color::Red)),
            Span::raw("  "),
            label_span("logs"),
            Span::styled(
                self.logs.len().to_string(),
                Style::default().fg(Color::Gray),
            ),
        ])
    }

    fn push_log(&mut self, kind: LogKind, text: impl Into<String>, source: Option<String>) {
        self.logs.push_back(LogEntry {
            elapsed: self.started_at.elapsed(),
            kind,
            source,
            text: text.into(),
        });
        while self.logs.len() > MAX_LOGS {
            self.logs.pop_front();
        }
    }

    fn transfer_mut(&mut self, id: TransferId) -> Option<&mut TransferRow> {
        self.transfers.iter_mut().find(|row| row.id == id)
    }

    fn transfer(&self, id: TransferId) -> Option<&TransferRow> {
        self.transfers.iter().find(|row| row.id == id)
    }
}

async fn run_dashboard(
    mut dashboard: Dashboard,
    mut events: UnboundedReceiver<PeerlineEvent>,
    quit_signal: Option<watch::Sender<bool>>,
) -> anyhow::Result<()> {
    crossterm::terminal::enable_raw_mode()?;
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, crossterm::terminal::EnterAlternateScreen)?;
    let _cleanup = TerminalCleanup;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let mut tick = time::interval(Duration::from_millis(75));

    loop {
        terminal.draw(|frame| dashboard.draw(frame))?;

        tokio::select! {
            maybe_event = events.recv() => {
                match maybe_event {
                    Some(event) => {
                        let should_exit = dashboard.apply_event(event);
                        terminal.draw(|frame| dashboard.draw(frame))?;
                        if should_exit {
                            break;
                        }
                    }
                    None => {
                        terminal.draw(|frame| dashboard.draw(frame))?;
                        break;
                    }
                }
            }
            _ = tick.tick() => {
                while crossterm::event::poll(Duration::from_millis(0))? {
                    match crossterm::event::read()? {
                        crossterm::event::Event::Key(key) if is_quit_key(key) => {
                            if let Some(signal) = quit_signal.as_ref() {
                                let _ = signal.send(true);
                            }
                            return Ok(());
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    Ok(())
}

fn stage_kind(stage: &TransferStage) -> LogKind {
    match stage {
        TransferStage::Complete => LogKind::Success,
        TransferStage::Failed(_) => LogKind::Error,
        TransferStage::Authenticating | TransferStage::Verifying => LogKind::Warn,
        TransferStage::Transferring => LogKind::Success,
        TransferStage::Discovering
        | TransferStage::Connecting(_)
        | TransferStage::ReceivingManifest => LogKind::Status,
    }
}

fn log_level_kind(level: &PeerlineLogLevel) -> LogKind {
    match level {
        PeerlineLogLevel::Error => LogKind::Error,
        PeerlineLogLevel::Warn => LogKind::Warn,
        PeerlineLogLevel::Info => LogKind::Info,
        PeerlineLogLevel::Debug | PeerlineLogLevel::Trace => LogKind::Status,
    }
}

fn transfer_style(stage: &TransferStage, active: bool) -> Style {
    let mut style = stage_style(stage);
    if active {
        style = style.add_modifier(Modifier::BOLD);
    }
    style
}

fn is_quit_key(key: crossterm::event::KeyEvent) -> bool {
    matches!(
        key.code,
        crossterm::event::KeyCode::Char('q') | crossterm::event::KeyCode::Esc
    ) || matches!(
        key.code,
        crossterm::event::KeyCode::Char('c')
            if key
                .modifiers
                .contains(crossterm::event::KeyModifiers::CONTROL)
    )
}

fn render_log_item(entry: LogEntry) -> ListItem<'static> {
    let mut spans = vec![Span::styled(
        format!("[{}] ", format_elapsed(entry.elapsed)),
        Style::default().fg(Color::DarkGray),
    )];
    if let Some(source) = entry.source {
        spans.push(Span::styled(
            format!("[{}] ", truncate_middle(&source, 18)),
            Style::default().fg(Color::Gray),
        ));
    }
    spans.push(Span::styled(
        format!("[{}] ", log_kind_label(entry.kind)),
        log_kind_style(entry.kind),
    ));
    let text_style = match entry.kind {
        LogKind::Info => Style::default().fg(Color::White),
        LogKind::Success => Style::default().fg(Color::Green),
        LogKind::Warn => Style::default().fg(Color::Yellow),
        LogKind::Error => Style::default().fg(Color::Red),
        LogKind::Status => Style::default().fg(Color::Cyan),
    };
    spans.push(Span::styled(entry.text, text_style));
    ListItem::new(Line::from(spans))
}

fn log_kind_label(kind: LogKind) -> &'static str {
    match kind {
        LogKind::Info => "info",
        LogKind::Success => "ok",
        LogKind::Warn => "warn",
        LogKind::Error => "error",
        LogKind::Status => "status",
    }
}

fn log_kind_style(kind: LogKind) -> Style {
    match kind {
        LogKind::Info => Style::default().fg(Color::White),
        LogKind::Success => Style::default().fg(Color::Green),
        LogKind::Warn => Style::default().fg(Color::Yellow),
        LogKind::Error => Style::default().fg(Color::Red),
        LogKind::Status => Style::default().fg(Color::Cyan),
    }
}

fn label_span(label: &str) -> Span<'static> {
    Span::styled(
        format!("{label:<8}"),
        Style::default()
            .fg(Color::Gray)
            .add_modifier(Modifier::BOLD),
    )
}

fn format_progress(done: u64, total: u64) -> String {
    if total == 0 {
        return "0%".to_string();
    }

    let ratio = (done as f64 / total as f64).clamp(0.0, 1.0);
    format!(
        "{:.0}% {} / {}",
        ratio * 100.0,
        format_bytes(done),
        format_bytes(total)
    )
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{} {}", bytes, UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn format_elapsed(elapsed: Duration) -> String {
    let secs = elapsed.as_secs();
    let tenths = elapsed.subsec_millis() / 100;
    if secs >= 60 {
        let minutes = secs / 60;
        let seconds = secs % 60;
        format!("{minutes:02}:{seconds:02}.{tenths}")
    } else {
        format!("{secs}.{tenths}s")
    }
}

fn truncate_middle(value: &str, max_chars: usize) -> String {
    let chars = value.chars().count();
    if chars <= max_chars {
        return value.to_string();
    }
    if max_chars <= 3 {
        return value.chars().take(max_chars).collect();
    }

    let keep = max_chars - 3;
    let head = keep / 2;
    let tail = keep - head;
    let start = value.chars().take(head).collect::<String>();
    let end = value
        .chars()
        .rev()
        .take(tail)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    format!("{start}...{end}")
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
    use peerline_core::{ConnectionRoute, HumanCode, HumanName, TransferId};

    fn recv_dashboard() -> Dashboard {
        Dashboard::new_recv(RecvView {
            name: HumanName::parse("river-mango-42").unwrap(),
            code: HumanCode::parse("rose-lime-iris-jade-1234").unwrap(),
            bind: "127.0.0.1:43117".into(),
            route_status: "ready".into(),
            stage: TransferStage::Discovering,
            progress: None,
        })
    }

    #[test]
    fn tracks_multiple_transfer_rows_with_peer_labels() {
        let mut dashboard = recv_dashboard();
        let first = TransferId::random();
        let second = TransferId::random();

        dashboard.apply_event(PeerlineEvent::TransferStarted {
            id: first,
            peer: "127.0.0.1:50001".into(),
            files: 2,
            bytes: 200,
        });
        dashboard.apply_event(PeerlineEvent::Progress {
            id: first,
            bytes_done: 100,
            bytes_total: 200,
        });
        dashboard.apply_event(PeerlineEvent::StageChanged(TransferStage::Complete));
        dashboard.apply_event(PeerlineEvent::TransferStarted {
            id: second,
            peer: "12D3KooWExamplePeer".into(),
            files: 1,
            bytes: 50,
        });
        dashboard.apply_event(PeerlineEvent::StageChanged(TransferStage::Connecting(
            ConnectionRoute::Libp2pDcutr,
        )));

        assert_eq!(dashboard.transfers.len(), 2);
        assert_eq!(dashboard.transfers[0].peer, "127.0.0.1:50001");
        assert_eq!(dashboard.transfers[1].peer, "12D3KooWExamplePeer");
        assert!(matches!(
            dashboard.transfers[0].stage,
            TransferStage::Complete
        ));
        assert!(matches!(
            dashboard.transfers[1].stage,
            TransferStage::Connecting(ConnectionRoute::Libp2pDcutr)
        ));
        assert!(dashboard.logs.len() >= 4);
    }

    #[test]
    fn q_and_escape_are_quit_keys() {
        assert!(is_quit_key(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Char('q'),
            crossterm::event::KeyModifiers::NONE,
        )));
        assert!(is_quit_key(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Esc,
            crossterm::event::KeyModifiers::NONE,
        )));
        assert!(is_quit_key(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Char('c'),
            crossterm::event::KeyModifiers::CONTROL,
        )));
        assert!(!is_quit_key(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Char('x'),
            crossterm::event::KeyModifiers::NONE,
        )));
    }
}
