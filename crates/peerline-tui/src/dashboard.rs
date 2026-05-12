use super::{RecvView, SendView, stage_label, stage_style};
use peerline_core::{PeerlineEvent, PeerlineLogLevel, TransferId, TransferStage};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table},
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
    log_scroll_top: Option<usize>,
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
            log_scroll_top: None,
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
            log_scroll_top: None,
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
        let layout = dashboard_layout(area);

        self.draw_header(frame, layout.header);
        self.draw_transfers(frame, layout.transfers);
        self.draw_logs(frame, layout.logs);
        self.draw_footer(frame, layout.footer);
    }

    fn draw_header(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let mut lines = vec![
            self.summary_line(area.width),
            self.identity_line(area.width),
            self.code_line(area.width),
            self.state_line(area.width),
        ];
        if lines.is_empty() {
            lines.push(Line::from(""));
        }
        let header =
            Paragraph::new(lines).block(Block::new().title(self.title()).borders(Borders::ALL));
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

        let inner_width = area.width.saturating_sub(2) as usize;
        let peer_width = if inner_width >= 96 { 24 } else { 18 };
        let status_width = inner_width
            .saturating_sub(peer_width + 5 + 10 + 14 + 12 + 5)
            .max(10);
        let rows = self
            .transfers
            .iter()
            .rev()
            .map(|transfer| {
                let active = self.active_transfer == Some(transfer.id);
                let style = transfer_style(&transfer.stage, active);
                Row::new(vec![
                    Cell::from(truncate_middle(&transfer.peer, peer_width)),
                    Cell::from(transfer.files.to_string()),
                    Cell::from(format_bytes(transfer.bytes)),
                    Cell::from(transfer.progress.map_or_else(
                        || "0%".to_string(),
                        |(done, total)| truncate_end(&format_progress(done, total), 14),
                    )),
                    Cell::from(truncate_end(&stage_label(&transfer.stage), 12)),
                    Cell::from(truncate_end(&transfer.status, status_width)),
                ])
                .style(style)
            })
            .collect::<Vec<_>>();

        let table = Table::new(
            rows,
            [
                Constraint::Length(peer_width as u16),
                Constraint::Length(5),
                Constraint::Length(10),
                Constraint::Length(14),
                Constraint::Length(12),
                Constraint::Min(10),
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
        let title = if self.log_scroll_top.is_some() {
            "Activity (history)"
        } else {
            "Activity"
        };
        let block = Block::new().title(title).borders(Borders::ALL);
        if self.logs.is_empty() {
            let empty = Paragraph::new("waiting for events")
                .style(Style::default().fg(Color::DarkGray))
                .block(block);
            frame.render_widget(empty, area);
            return;
        }

        frame.render_widget(
            Paragraph::new(self.visible_activity_lines(area)).block(block),
            area,
        );
    }

    fn draw_footer(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let controls = if self.log_scroll_top.is_some() {
            " quit  logs Up/Down PgUp/PgDn Home/End  history  "
        } else {
            " quit  logs Up/Down PgUp/PgDn Home/End  "
        };
        let fixed_width = "q/Esc".chars().count() + controls.chars().count();
        let summary = truncate_end(
            &format!(
                "transfers {}  active {}  done {}  failed {}  logs {}",
                self.transfers.len(),
                self.active_transfer_count(),
                self.complete_transfer_count(),
                self.failed_transfer_count(),
                self.logs.len(),
            ),
            (area.width as usize).saturating_sub(fixed_width),
        );
        let footer = Paragraph::new(Line::from(vec![
            Span::styled(
                "q/Esc",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(controls, Style::default().fg(Color::Gray)),
            Span::styled(summary, Style::default().fg(Color::DarkGray)),
        ]));
        frame.render_widget(footer, area);
    }

    fn title(&self) -> &'static str {
        match self.kind {
            DashboardKind::Recv { .. } => "Peerline receive",
            DashboardKind::Send { .. } => "Peerline send",
        }
    }

    fn summary_line(&self, width: u16) -> Line<'static> {
        let summary = format!(
            "transfers {} | active {} | done {} | failed {}",
            self.transfers.len(),
            self.active_transfer_count(),
            self.complete_transfer_count(),
            self.failed_transfer_count(),
        );
        let summary_width = width.saturating_sub(2) as usize;
        Line::from(vec![Span::styled(
            truncate_end(&summary, summary_width),
            Style::default().fg(Color::Gray),
        )])
    }

    fn identity_line(&self, width: u16) -> Line<'static> {
        let content_width = width.saturating_sub(2) as usize;
        match &self.kind {
            DashboardKind::Recv {
                name,
                code: _,
                bind,
            } => Line::from(vec![
                label_span("name"),
                Span::styled(
                    truncate_middle(name.as_str(), field_width(content_width, 28)),
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::raw("  "),
                label_span("listen"),
                Span::styled(
                    truncate_middle(bind.as_str(), field_width(content_width, 24)),
                    Style::default().fg(Color::Cyan),
                ),
            ]),
            DashboardKind::Send {
                target_label,
                target,
                code: _,
            } => Line::from(vec![
                label_span(target_label),
                Span::styled(
                    truncate_middle(target.as_str(), field_width(content_width, 36)),
                    Style::default().add_modifier(Modifier::BOLD),
                ),
            ]),
        }
    }

    fn code_line(&self, _width: u16) -> Line<'static> {
        let code = match &self.kind {
            DashboardKind::Recv { code, .. } | DashboardKind::Send { code, .. } => code,
        };
        Line::from(vec![
            label_span("code"),
            Span::styled(code.clone(), Style::default().fg(Color::Yellow)),
        ])
    }

    fn state_line(&self, width: u16) -> Line<'static> {
        let stage = stage_label(&self.stage);
        let status = if self.status == stage {
            String::new()
        } else {
            format!(" | {}", self.status)
        };
        let prefix_width = 8 + stage.chars().count();
        let status_width = (width as usize).saturating_sub(prefix_width).max(1);
        Line::from(vec![
            label_span("state"),
            Span::styled(stage, stage_style(&self.stage)),
            Span::styled(
                truncate_end(&status, status_width),
                Style::default().fg(Color::White),
            ),
        ])
    }

    fn active_transfer_count(&self) -> usize {
        self.transfers
            .iter()
            .filter(|row| {
                !matches!(
                    row.stage,
                    TransferStage::Complete | TransferStage::Failed(_)
                )
            })
            .count()
    }

    fn complete_transfer_count(&self) -> usize {
        self.transfers
            .iter()
            .filter(|row| matches!(row.stage, TransferStage::Complete))
            .count()
    }

    fn failed_transfer_count(&self) -> usize {
        self.transfers
            .iter()
            .filter(|row| matches!(row.stage, TransferStage::Failed(_)))
            .count()
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

    fn visible_activity_lines(&self, area: Rect) -> Vec<Line<'static>> {
        let visible_rows = activity_visible_rows(area);
        let content_width = activity_content_width(area);
        visible_log_lines(&self.logs, content_width, visible_rows, self.log_scroll_top)
    }

    fn handle_activity_scroll_key(&mut self, key: crossterm::event::KeyEvent, area: Rect) -> bool {
        let Some(command) = activity_scroll_command(key) else {
            return false;
        };
        let visible_rows = activity_visible_rows(area);
        let content_width = activity_content_width(area);
        let total_rows = rendered_log_line_count(&self.logs, content_width);
        let max_start = max_log_start(total_rows, visible_rows);
        if max_start == 0 {
            self.log_scroll_top = None;
            return true;
        }

        let current = log_view_start(total_rows, visible_rows, self.log_scroll_top);
        let page = visible_rows.saturating_sub(1).max(1);
        let next = match command {
            ActivityScrollCommand::Up => Some(current.saturating_sub(1)),
            ActivityScrollCommand::Down => {
                let next = current.saturating_add(1).min(max_start);
                (next < max_start).then_some(next)
            }
            ActivityScrollCommand::PageUp => Some(current.saturating_sub(page)),
            ActivityScrollCommand::PageDown => {
                let next = current.saturating_add(page).min(max_start);
                (next < max_start).then_some(next)
            }
            ActivityScrollCommand::Top => Some(0),
            ActivityScrollCommand::Bottom => None,
        };
        self.log_scroll_top = next;
        true
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
    terminal.clear()?;
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
                        crossterm::event::Event::Key(key) => {
                            let size = terminal.size()?;
                            let layout = dashboard_layout(Rect::new(0, 0, size.width, size.height));
                            if dashboard.handle_activity_scroll_key(key, layout.logs) {
                                terminal.draw(|frame| dashboard.draw(frame))?;
                            }
                        }
                        crossterm::event::Event::Resize(_, _) => {
                            terminal.clear()?;
                            terminal.draw(|frame| dashboard.draw(frame))?;
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

#[derive(Clone, Copy)]
struct DashboardLayout {
    header: Rect,
    transfers: Rect,
    logs: Rect,
    footer: Rect,
}

fn dashboard_layout(area: Rect) -> DashboardLayout {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(6),
            Constraint::Min(8),
            Constraint::Length(1),
        ])
        .split(area);
    let body = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Ratio(6, 11), Constraint::Ratio(5, 11)])
        .split(chunks[1]);

    DashboardLayout {
        header: chunks[0],
        transfers: body[0],
        logs: body[1],
        footer: chunks[2],
    }
}

#[derive(Clone, Copy)]
enum ActivityScrollCommand {
    Up,
    Down,
    PageUp,
    PageDown,
    Top,
    Bottom,
}

fn activity_scroll_command(key: crossterm::event::KeyEvent) -> Option<ActivityScrollCommand> {
    match key.code {
        crossterm::event::KeyCode::Up => Some(ActivityScrollCommand::Up),
        crossterm::event::KeyCode::Down => Some(ActivityScrollCommand::Down),
        crossterm::event::KeyCode::PageUp => Some(ActivityScrollCommand::PageUp),
        crossterm::event::KeyCode::PageDown => Some(ActivityScrollCommand::PageDown),
        crossterm::event::KeyCode::Home => Some(ActivityScrollCommand::Top),
        crossterm::event::KeyCode::End => Some(ActivityScrollCommand::Bottom),
        _ => None,
    }
}

fn activity_visible_rows(area: Rect) -> usize {
    area.height.saturating_sub(2) as usize
}

fn activity_content_width(area: Rect) -> usize {
    area.width.saturating_sub(2) as usize
}

fn visible_log_lines(
    logs: &VecDeque<LogEntry>,
    width: usize,
    visible_rows: usize,
    scroll_top: Option<usize>,
) -> Vec<Line<'static>> {
    let lines = rendered_log_lines(logs, width);
    let start = log_view_start(lines.len(), visible_rows, scroll_top);
    lines.into_iter().skip(start).take(visible_rows).collect()
}

fn rendered_log_lines(logs: &VecDeque<LogEntry>, width: usize) -> Vec<Line<'static>> {
    logs.iter()
        .flat_map(|entry| render_log_lines(entry, width))
        .collect()
}

fn rendered_log_line_count(logs: &VecDeque<LogEntry>, width: usize) -> usize {
    logs.iter()
        .map(|entry| render_log_lines(entry, width).len())
        .sum()
}

fn log_view_start(total_rows: usize, visible_rows: usize, scroll_top: Option<usize>) -> usize {
    let max_start = max_log_start(total_rows, visible_rows);
    scroll_top.unwrap_or(max_start).min(max_start)
}

fn max_log_start(total_rows: usize, visible_rows: usize) -> usize {
    total_rows.saturating_sub(visible_rows)
}

fn render_log_lines(entry: &LogEntry, width: usize) -> Vec<Line<'static>> {
    let width = width.max(1);
    let source_width = if width >= 84 {
        18
    } else if width >= 56 {
        12
    } else {
        0
    };
    let mut prefix = Vec::new();
    let mut prefix_width = 0usize;
    if width >= 24 {
        let elapsed = format!("[{}] ", format_elapsed(entry.elapsed));
        prefix_width += elapsed.chars().count();
        prefix.push(Span::styled(elapsed, Style::default().fg(Color::DarkGray)));
    }
    if source_width > 0
        && let Some(source) = &entry.source
    {
        let source = format!("[{}] ", log_source_label(source, source_width));
        prefix_width += source.chars().count();
        prefix.push(Span::styled(source, Style::default().fg(Color::Gray)));
    }
    if width >= 10 {
        let kind = format!("[{}] ", log_kind_label(entry.kind));
        prefix_width += kind.chars().count();
        prefix.push(Span::styled(kind, log_kind_style(entry.kind)));
    }
    let text_style = match entry.kind {
        LogKind::Info => Style::default().fg(Color::White),
        LogKind::Success => Style::default().fg(Color::Green),
        LogKind::Warn => Style::default().fg(Color::Yellow),
        LogKind::Error => Style::default().fg(Color::Red),
        LogKind::Status => Style::default().fg(Color::Cyan),
    };
    let first_width = width.saturating_sub(prefix_width).max(1);
    let continuation_prefix = " ".repeat(prefix_width.min(width.saturating_sub(1)));
    let continuation_width = width
        .saturating_sub(continuation_prefix.chars().count())
        .max(1);
    let wrapped = wrap_log_text(&entry.text, first_width, continuation_width);
    let mut lines = Vec::with_capacity(wrapped.len().max(1));
    for (index, text) in wrapped.into_iter().enumerate() {
        if index == 0 {
            let mut spans = prefix.clone();
            spans.push(Span::styled(text, text_style));
            lines.push(Line::from(spans));
        } else {
            lines.push(Line::from(vec![
                Span::styled(
                    continuation_prefix.clone(),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(text, text_style),
            ]));
        }
    }
    if lines.is_empty() {
        let mut spans = prefix;
        spans.push(Span::styled("", text_style));
        lines.push(Line::from(spans));
    }
    lines
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

fn log_source_label(source: &str, max_chars: usize) -> String {
    let compact = source
        .strip_prefix("peerline_")
        .or_else(|| source.strip_prefix("peerline::"))
        .unwrap_or(source);
    truncate_middle(compact, max_chars)
}

fn label_span(label: &str) -> Span<'static> {
    Span::styled(
        format!("{label:<8}"),
        Style::default()
            .fg(Color::Gray)
            .add_modifier(Modifier::BOLD),
    )
}

fn field_width(content_width: usize, preferred: usize) -> usize {
    preferred.min(content_width.saturating_div(2).max(8))
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

fn truncate_end(value: &str, max_chars: usize) -> String {
    let chars = value.chars().count();
    if chars <= max_chars {
        return value.to_string();
    }
    if max_chars <= 3 {
        return value.chars().take(max_chars).collect();
    }
    let mut truncated = value.chars().take(max_chars - 3).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn wrap_log_text(value: &str, first_width: usize, continuation_width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut next_width = first_width.max(1);
    for logical in value.replace('\r', "").split('\n') {
        if logical.is_empty() {
            lines.push(String::new());
            next_width = continuation_width.max(1);
            continue;
        }
        let mut current = String::new();
        let mut width = 0usize;
        for ch in logical.chars() {
            if width >= next_width {
                lines.push(current);
                current = String::new();
                width = 0;
                next_width = continuation_width.max(1);
            }
            current.push(ch);
            width += 1;
        }
        lines.push(current);
        next_width = continuation_width.max(1);
    }
    lines
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

    fn line_text(line: Line<'_>) -> String {
        line.spans
            .into_iter()
            .map(|span| span.content.into_owned())
            .collect()
    }

    fn key(code: crossterm::event::KeyCode) -> crossterm::event::KeyEvent {
        crossterm::event::KeyEvent::new(code, crossterm::event::KeyModifiers::NONE)
    }

    #[test]
    fn header_summary_does_not_repeat_the_block_title() {
        let dashboard = recv_dashboard();

        let summary = line_text(dashboard.summary_line(80));

        assert!(!summary.contains("Peerline receive"));
        assert_eq!(summary, "transfers 0 | active 0 | done 0 | failed 0");
    }

    #[test]
    fn pairing_code_is_rendered_without_middle_truncation() {
        let dashboard = recv_dashboard();

        let code = line_text(dashboard.code_line(20));

        assert!(code.contains("rose-lime-iris-jade-1234"));
        assert!(!code.contains("..."));
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
    fn activity_log_lines_wrap_to_the_available_width() {
        let entry = LogEntry {
            elapsed: Duration::from_millis(1250),
            kind: LogKind::Info,
            source: Some("peerline_cli::very_long_target_name".into()),
            text: "abcdefghijklmnopqrstuvwxyz0123456789\nsecond-line-with-more-text".into(),
        };

        let lines = render_log_lines(&entry, 32);

        assert!(lines.len() > 2);
        assert!(lines.iter().all(|line| line.width() <= 32));
    }

    #[test]
    fn activity_log_prefix_degrades_for_tiny_widths() {
        let entry = LogEntry {
            elapsed: Duration::from_millis(1250),
            kind: LogKind::Warn,
            source: Some("peerline_cli::very_long_target_name".into()),
            text: "abcdefghijklmnopqrstuvwxyz".into(),
        };

        let lines = render_log_lines(&entry, 8);

        assert!(lines.len() > 1);
        assert!(lines.iter().all(|line| line.width() <= 8));
    }

    #[test]
    fn activity_log_source_labels_omit_peerline_prefix() {
        assert_eq!(log_source_label("peerline_cli::main", 18), "cli::main");
        assert_eq!(
            log_source_label("peerline_net::libp2p_transfer::receiver", 18),
            "net::li...receiver"
        );
        assert_eq!(
            log_source_label("external_crate::module", 32),
            "external_crate::module"
        );
    }

    #[test]
    fn activity_log_follows_the_bottom_by_default() {
        let mut dashboard = recv_dashboard();
        for index in 0..8 {
            dashboard.push_log(LogKind::Info, format!("log-{index}"), None);
        }

        let lines = dashboard
            .visible_activity_lines(Rect::new(0, 0, 80, 6))
            .into_iter()
            .map(line_text)
            .collect::<Vec<_>>();

        assert_eq!(lines.len(), 4);
        assert!(lines[0].contains("log-4"));
        assert!(lines[3].contains("log-7"));
    }

    #[test]
    fn activity_log_scroll_keys_lock_and_release_history_view() {
        let mut dashboard = recv_dashboard();
        let area = Rect::new(0, 0, 80, 6);
        for index in 0..8 {
            dashboard.push_log(LogKind::Info, format!("log-{index}"), None);
        }

        assert!(dashboard.handle_activity_scroll_key(key(crossterm::event::KeyCode::Up), area));
        assert_eq!(dashboard.log_scroll_top, Some(3));

        let lines = dashboard
            .visible_activity_lines(area)
            .into_iter()
            .map(line_text)
            .collect::<Vec<_>>();
        assert!(lines[0].contains("log-3"));
        assert!(lines[3].contains("log-6"));

        assert!(dashboard.handle_activity_scroll_key(key(crossterm::event::KeyCode::End), area));
        assert_eq!(dashboard.log_scroll_top, None);
        let lines = dashboard
            .visible_activity_lines(area)
            .into_iter()
            .map(line_text)
            .collect::<Vec<_>>();
        assert!(lines[0].contains("log-4"));
        assert!(lines[3].contains("log-7"));
    }

    #[test]
    fn activity_log_history_view_is_not_yanked_by_new_logs() {
        let mut dashboard = recv_dashboard();
        let area = Rect::new(0, 0, 80, 6);
        for index in 0..8 {
            dashboard.push_log(LogKind::Info, format!("log-{index}"), None);
        }

        assert!(dashboard.handle_activity_scroll_key(key(crossterm::event::KeyCode::Home), area));
        assert_eq!(dashboard.log_scroll_top, Some(0));

        dashboard.push_log(LogKind::Info, "log-8", None);
        let lines = dashboard
            .visible_activity_lines(area)
            .into_iter()
            .map(line_text)
            .collect::<Vec<_>>();

        assert!(lines[0].contains("log-0"));
        assert!(lines[3].contains("log-3"));
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
