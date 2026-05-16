use self::{
    format::{
        field_width, format_bytes, format_progress, label_span, truncate_end, truncate_middle,
    },
    logs::{
        ActivityScrollCommand, LogEntry, LogKind, activity_content_width, activity_scroll_command,
        activity_visible_rows, log_level_kind, log_view_start, max_log_start,
        rendered_log_line_count, stage_kind, visible_log_lines,
    },
};
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

mod format;
mod logs;

#[cfg(test)]
mod tests;

const MAX_LOGS: usize = 64;
const MAX_TRANSFERS: usize = 32;
const LOG_COALESCE_LOOKBACK: usize = 24;

pub async fn run_recv(
    view: RecvView,
    events: UnboundedReceiver<PeerlineEvent>,
    quit_signal: Option<watch::Sender<bool>>,
) -> anyhow::Result<()> {
    run_dashboard(Dashboard::new_recv(view), events, quit_signal, None).await
}

pub async fn run_send(
    view: SendView,
    events: UnboundedReceiver<PeerlineEvent>,
    quit_signal: Option<watch::Sender<bool>>,
    retry_signal: Option<tokio::sync::mpsc::UnboundedSender<()>>,
) -> anyhow::Result<()> {
    run_dashboard(
        Dashboard::new_send(view, retry_signal.is_some()),
        events,
        quit_signal,
        retry_signal,
    )
    .await
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
    retry_enabled: bool,
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
            retry_enabled: false,
        }
    }

    fn new_send(view: SendView, retry_enabled: bool) -> Self {
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
            retry_enabled,
        }
    }

    fn apply_event(&mut self, event: PeerlineEvent) -> bool {
        let now = Instant::now();
        match event {
            PeerlineEvent::Shutdown => true,
            PeerlineEvent::StageChanged(next) => {
                let should_exit = self.should_exit_after_stage(&next);
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
                    return should_exit;
                }
                false
            }
            PeerlineEvent::TransferStarted {
                id,
                peer,
                files,
                bytes,
                resume_offset,
            } => {
                self.active_transfer = Some(id);
                self.status = format!("{files} file(s) from {peer}");
                self.progress = Some((resume_offset, bytes));
                self.transfers.push(TransferRow {
                    id,
                    peer: peer.clone(),
                    files,
                    bytes,
                    progress: Some((resume_offset, bytes)),
                    stage: self.stage.clone(),
                    status: stage_label(&self.stage),
                    updated_at: now,
                });
                if self.transfers.len() > MAX_TRANSFERS {
                    self.transfers.remove(0);
                }
                let message = if resume_offset > 0 {
                    format!(
                        "resuming {files} file(s) from {peer} at {} of {}",
                        format_bytes(resume_offset),
                        format_bytes(bytes)
                    )
                } else {
                    format!(
                        "started {files} file(s) from {peer} ({})",
                        format_bytes(bytes)
                    )
                };
                self.push_log(LogKind::Info, message, Some(peer));
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
        let retry = if self.retry_available() {
            "r retry  "
        } else {
            ""
        };
        let controls = if self.log_scroll_top.is_some() {
            format!("{retry}quit  logs Up/Down PgUp/PgDn Home/End  history  ")
        } else {
            format!("{retry}quit  logs Up/Down PgUp/PgDn Home/End  ")
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

    fn retry_available(&self) -> bool {
        self.retry_enabled
            && matches!(self.kind, DashboardKind::Send { .. })
            && matches!(self.stage, TransferStage::Failed(_))
    }

    fn should_exit_after_stage(&self, stage: &TransferStage) -> bool {
        match self.kind {
            DashboardKind::Recv { .. } => false,
            DashboardKind::Send { .. } => matches!(stage, TransferStage::Complete),
        }
    }

    fn push_log(&mut self, kind: LogKind, text: impl Into<String>, source: Option<String>) {
        let text = text.into();
        let elapsed = self.started_at.elapsed();
        if let Some(index) = self.repeated_log_index(kind, source.as_deref(), &text)
            && let Some(mut entry) = self.logs.remove(index)
        {
            entry.elapsed = elapsed;
            entry.repeat_count = entry.repeat_count.saturating_add(1);
            self.logs.push_back(entry);
            return;
        }

        self.logs.push_back(LogEntry {
            elapsed,
            kind,
            source,
            text,
            repeat_count: 1,
        });
        while self.logs.len() > MAX_LOGS {
            self.logs.pop_front();
        }
    }

    fn repeated_log_index(&self, kind: LogKind, source: Option<&str>, text: &str) -> Option<usize> {
        self.logs
            .iter()
            .enumerate()
            .rev()
            .take(LOG_COALESCE_LOOKBACK)
            .find_map(|(index, entry)| {
                (entry.kind == kind && entry.source.as_deref() == source && entry.text == text)
                    .then_some(index)
            })
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
    retry_signal: Option<tokio::sync::mpsc::UnboundedSender<()>>,
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
                        crossterm::event::Event::Key(key)
                            if is_retry_key(key) && dashboard.retry_available() =>
                        {
                            if let Some(signal) = retry_signal.as_ref() {
                                let _ = signal.send(());
                            }
                            dashboard.push_log(LogKind::Status, "retry requested", None);
                            dashboard.status = "retry requested".into();
                            terminal.draw(|frame| dashboard.draw(frame))?;
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

fn is_retry_key(key: crossterm::event::KeyEvent) -> bool {
    matches!(
        key.code,
        crossterm::event::KeyCode::Char('r') | crossterm::event::KeyCode::Char('R')
    ) && key.modifiers.is_empty()
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

struct TerminalCleanup;

impl Drop for TerminalCleanup {
    fn drop(&mut self) {
        let mut stdout = io::stdout();
        let _ = crossterm::execute!(stdout, crossterm::terminal::LeaveAlternateScreen);
        let _ = crossterm::terminal::disable_raw_mode();
    }
}
