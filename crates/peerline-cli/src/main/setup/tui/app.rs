use crate::doctor::{DependencyReport, DependencyStatus, DoctorReport};
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, Wrap},
};
use std::collections::VecDeque;

const MAX_LOGS: usize = 32;

pub(super) struct SetupApp {
    report: DoctorReport,
    selected: usize,
    logs: VecDeque<String>,
}

impl SetupApp {
    pub(super) fn new(report: DoctorReport) -> Self {
        let mut app = Self {
            report,
            selected: 0,
            logs: VecDeque::new(),
        };
        app.push_log("doctor snapshot loaded");
        app
    }

    pub(super) fn draw(&self, frame: &mut ratatui::Frame<'_>) {
        let area = frame.area();
        let layout = setup_layout(area);
        self.draw_header(frame, layout.header);
        self.draw_dependencies(frame, layout.dependencies);
        self.draw_details(frame, layout.details);
        self.draw_logs(frame, layout.logs);
        self.draw_footer(frame, layout.footer);
    }

    pub(super) fn selected_dependency(&self) -> Option<&DependencyReport> {
        self.report.dependencies.get(self.selected)
    }

    pub(super) fn installable_action_indices(&self) -> Vec<usize> {
        self.report
            .dependencies
            .iter()
            .enumerate()
            .filter(|(_, dependency)| dependency.needs_action() && dependency.install.is_some())
            .map(|(index, _)| index)
            .collect()
    }

    pub(super) fn set_selected(&mut self, selected: usize) {
        self.selected = selected.min(self.report.dependencies.len().saturating_sub(1));
    }

    pub(super) fn move_selection(&mut self, delta: isize) {
        if self.report.dependencies.is_empty() {
            self.selected = 0;
            return;
        }
        let last = self.report.dependencies.len().saturating_sub(1) as isize;
        self.selected = (self.selected as isize + delta).clamp(0, last) as usize;
    }

    pub(super) fn push_log(&mut self, message: impl Into<String>) {
        self.logs.push_back(message.into());
        while self.logs.len() > MAX_LOGS {
            self.logs.pop_front();
        }
    }

    pub(super) fn refresh_report(&mut self, report: DoctorReport) {
        self.report = report;
        if self.selected >= self.report.dependencies.len() {
            self.selected = self.report.dependencies.len().saturating_sub(1);
        }
        self.push_log("doctor snapshot refreshed");
    }

    fn draw_header(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let manager = self
            .report
            .package_manager
            .as_ref()
            .map(|manager| manager.label().to_string())
            .unwrap_or_else(|| "not detected".into());
        let actionable = self.report.actionable_dependencies().len();
        let status = if actionable == 0 {
            Span::styled("ready", Style::default().fg(Color::Green))
        } else {
            Span::styled(
                format!("{actionable} item(s) need attention"),
                Style::default().fg(Color::Yellow),
            )
        };
        let lines = vec![
            Line::from(vec![
                Span::styled(
                    "peerline setup",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw("  "),
                status,
            ]),
            Line::from(vec![
                Span::styled("platform ", label_style()),
                Span::raw(format!(
                    "{} {}",
                    self.report.platform.os.label(),
                    self.report.platform.arch
                )),
                Span::raw("   "),
                Span::styled("package manager ", label_style()),
                Span::raw(manager),
            ]),
        ];
        frame.render_widget(
            Paragraph::new(lines).block(Block::new().title("Overview").borders(Borders::ALL)),
            area,
        );
    }

    fn draw_dependencies(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let rows = self
            .report
            .dependencies
            .iter()
            .enumerate()
            .map(|(index, dependency)| {
                let selected = index == self.selected;
                Row::new(vec![
                    Cell::from(if selected { ">" } else { " " }),
                    Cell::from(status_label(dependency.status)),
                    Cell::from(dependency.label.clone()),
                    Cell::from(truncate_end(
                        &dependency.detail,
                        area.width.saturating_sub(32) as usize,
                    )),
                ])
                .style(dependency_style(dependency.status, selected))
            })
            .collect::<Vec<_>>();

        let table = Table::new(
            rows,
            [
                Constraint::Length(1),
                Constraint::Length(9),
                Constraint::Length(24),
                Constraint::Min(12),
            ],
        )
        .header(
            Row::new(vec!["", "status", "check", "detail"]).style(
                Style::default()
                    .fg(Color::Gray)
                    .add_modifier(Modifier::BOLD),
            ),
        )
        .block(Block::new().title("Checks").borders(Borders::ALL))
        .column_spacing(1);

        frame.render_widget(table, area);
    }

    fn draw_details(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let Some(dependency) = self.selected_dependency() else {
            frame.render_widget(
                Paragraph::new("no dependency selected")
                    .style(Style::default().fg(Color::DarkGray))
                    .block(Block::new().title("Action").borders(Borders::ALL)),
                area,
            );
            return;
        };

        let mut lines = vec![Line::from(vec![
            Span::styled(
                dependency.label.clone(),
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(
                status_label(dependency.status),
                dependency_style(dependency.status, false),
            ),
        ])];
        lines.push(Line::from(dependency.detail.clone()));
        if let Some(path) = &dependency.command_path {
            lines.push(Line::from(vec![
                Span::styled("path ", label_style()),
                Span::raw(path.clone()),
            ]));
        }
        if let Some(version) = &dependency.version {
            lines.push(Line::from(vec![
                Span::styled("version ", label_style()),
                Span::raw(version.clone()),
            ]));
        }
        lines.push(Line::from(""));
        match dependency.install.as_ref() {
            Some(plan) => {
                lines.push(Line::from(vec![
                    Span::styled("plan ", label_style()),
                    Span::raw(plan.summary.clone()),
                ]));
                for command in &plan.commands {
                    let prefix = if command.program.is_some() {
                        "run "
                    } else {
                        "manual "
                    };
                    lines.push(Line::from(vec![
                        Span::styled(prefix, label_style()),
                        Span::raw(command.display.clone()),
                    ]));
                }
                for note in &plan.notes {
                    lines.push(Line::from(vec![
                        Span::styled("note ", label_style()),
                        Span::raw(note.clone()),
                    ]));
                }
            }
            None => lines.push(Line::from("no install action is needed")),
        }

        frame.render_widget(
            Paragraph::new(lines)
                .wrap(Wrap { trim: true })
                .block(Block::new().title("Action").borders(Borders::ALL)),
            area,
        );
    }

    fn draw_logs(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let lines = self
            .logs
            .iter()
            .rev()
            .take(area.height.saturating_sub(2) as usize)
            .rev()
            .cloned()
            .map(Line::from)
            .collect::<Vec<_>>();
        let body = if lines.is_empty() {
            Paragraph::new("waiting for setup actions").style(Style::default().fg(Color::DarkGray))
        } else {
            Paragraph::new(lines)
        };
        frame.render_widget(
            body.block(Block::new().title("Activity").borders(Borders::ALL)),
            area,
        );
    }

    fn draw_footer(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let can_install = self
            .selected_dependency()
            .and_then(|dependency| dependency.install.as_ref())
            .is_some();
        let install = if can_install {
            "Enter install/show  "
        } else {
            ""
        };
        let footer = Paragraph::new(Line::from(vec![
            Span::styled(
                "q/Esc",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("  {install}a all  r refresh  Up/Down select"),
                Style::default().fg(Color::Gray),
            ),
        ]));
        frame.render_widget(footer, area);
    }
}

#[derive(Clone, Copy)]
struct SetupLayout {
    header: Rect,
    dependencies: Rect,
    details: Rect,
    logs: Rect,
    footer: Rect,
}

fn setup_layout(area: Rect) -> SetupLayout {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Min(14),
            Constraint::Length(7),
            Constraint::Length(1),
        ])
        .split(area);
    let body = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Ratio(5, 9), Constraint::Ratio(4, 9)])
        .split(chunks[1]);

    SetupLayout {
        header: chunks[0],
        dependencies: body[0],
        details: body[1],
        logs: chunks[2],
        footer: chunks[3],
    }
}

fn status_label(status: DependencyStatus) -> &'static str {
    match status {
        DependencyStatus::Ok => "ok",
        DependencyStatus::Missing => "missing",
        DependencyStatus::Broken => "broken",
    }
}

fn dependency_style(status: DependencyStatus, selected: bool) -> Style {
    let style = match status {
        DependencyStatus::Ok => Style::default().fg(Color::Green),
        DependencyStatus::Missing => Style::default().fg(Color::Yellow),
        DependencyStatus::Broken => Style::default().fg(Color::Red),
    };
    if selected {
        style.add_modifier(Modifier::BOLD)
    } else {
        style
    }
}

fn label_style() -> Style {
    Style::default()
        .fg(Color::Gray)
        .add_modifier(Modifier::BOLD)
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
