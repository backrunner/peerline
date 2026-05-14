use peerline_core::{PeerlineLogLevel, TransferStage};
use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
};
use std::{collections::VecDeque, time::Duration};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum LogKind {
    Info,
    Success,
    Warn,
    Error,
    Status,
}

#[derive(Clone)]
pub(super) struct LogEntry {
    pub(super) elapsed: Duration,
    pub(super) kind: LogKind,
    pub(super) source: Option<String>,
    pub(super) text: String,
    pub(super) repeat_count: usize,
}

pub(super) fn stage_kind(stage: &TransferStage) -> LogKind {
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

pub(super) fn log_level_kind(level: &PeerlineLogLevel) -> LogKind {
    match level {
        PeerlineLogLevel::Error => LogKind::Error,
        PeerlineLogLevel::Warn => LogKind::Warn,
        PeerlineLogLevel::Info => LogKind::Info,
        PeerlineLogLevel::Debug | PeerlineLogLevel::Trace => LogKind::Status,
    }
}

#[derive(Clone, Copy)]
pub(super) enum ActivityScrollCommand {
    Up,
    Down,
    PageUp,
    PageDown,
    Top,
    Bottom,
}

pub(super) fn activity_scroll_command(
    key: crossterm::event::KeyEvent,
) -> Option<ActivityScrollCommand> {
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

pub(super) fn activity_visible_rows(area: Rect) -> usize {
    area.height.saturating_sub(2) as usize
}

pub(super) fn activity_content_width(area: Rect) -> usize {
    area.width.saturating_sub(2) as usize
}

pub(super) fn visible_log_lines(
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

pub(super) fn rendered_log_line_count(logs: &VecDeque<LogEntry>, width: usize) -> usize {
    logs.iter()
        .map(|entry| render_log_lines(entry, width).len())
        .sum()
}

pub(super) fn log_view_start(
    total_rows: usize,
    visible_rows: usize,
    scroll_top: Option<usize>,
) -> usize {
    let max_start = max_log_start(total_rows, visible_rows);
    scroll_top.unwrap_or(max_start).min(max_start)
}

pub(super) fn max_log_start(total_rows: usize, visible_rows: usize) -> usize {
    total_rows.saturating_sub(visible_rows)
}

pub(super) fn render_log_lines(entry: &LogEntry, width: usize) -> Vec<Line<'static>> {
    let width = width.max(1);
    let mut prefix = Vec::new();
    let mut prefix_width = 0usize;
    if width >= 24 {
        let elapsed = format!("[{}] ", format_elapsed(entry.elapsed));
        prefix_width += elapsed.chars().count();
        prefix.push(Span::styled(elapsed, Style::default().fg(Color::DarkGray)));
    }
    let kind = if width >= 10 {
        Some(format!("[{}] ", log_kind_label(entry.kind)))
    } else {
        None
    };
    if width >= 56
        && let Some(source) = &entry.source
    {
        let source = format!("[{}] ", log_source_label(source));
        let reserved_width = kind.as_ref().map_or(0, |label| label.chars().count()) + 12;
        if prefix_width + source.chars().count() + reserved_width <= width {
            prefix_width += source.chars().count();
            prefix.push(Span::styled(source, Style::default().fg(Color::Gray)));
        }
    }
    if let Some(kind) = kind {
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
    let text = log_entry_text(entry);
    let wrapped = wrap_log_text(&text, first_width, continuation_width);
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

fn log_entry_text(entry: &LogEntry) -> String {
    if entry.repeat_count > 1 {
        format!("{} (x{})", entry.text, entry.repeat_count)
    } else {
        entry.text.clone()
    }
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

pub(super) fn log_source_label(source: &str) -> &str {
    source
        .strip_prefix("peerline_")
        .or_else(|| source.strip_prefix("peerline::"))
        .unwrap_or(source)
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
