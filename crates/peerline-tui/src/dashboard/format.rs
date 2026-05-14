use ratatui::{
    style::{Color, Modifier, Style},
    text::Span,
};

pub(super) fn label_span(label: &str) -> Span<'static> {
    Span::styled(
        format!("{label:<8}"),
        Style::default()
            .fg(Color::Gray)
            .add_modifier(Modifier::BOLD),
    )
}

pub(super) fn field_width(content_width: usize, preferred: usize) -> usize {
    preferred.min(content_width.saturating_div(2).max(8))
}

pub(super) fn format_progress(done: u64, total: u64) -> String {
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

pub(super) fn format_bytes(bytes: u64) -> String {
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

pub(super) fn truncate_middle(value: &str, max_chars: usize) -> String {
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

pub(super) fn truncate_end(value: &str, max_chars: usize) -> String {
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
