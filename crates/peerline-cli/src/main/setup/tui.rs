use self::app::SetupApp;
use super::install::run_install_plan_in_normal_terminal;
use crate::doctor::{self, DependencyReport, DoctorReport, InstallPlan};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};
use std::{
    io::{self, Write},
    time::Duration,
};
use tokio::time;

mod app;

pub(super) async fn run_setup_tui(report: DoctorReport) -> anyhow::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    let _cleanup = TerminalCleanup;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let mut app = SetupApp::new(report);
    let mut tick = time::interval(Duration::from_millis(75));

    loop {
        terminal.draw(|frame| app.draw(frame))?;
        tick.tick().await;
        while event::poll(Duration::from_millis(0))? {
            match event::read()? {
                Event::Key(key) if is_quit_key(key) => return Ok(()),
                Event::Key(key) if is_refresh_key(key) => {
                    app.refresh_report(doctor::collect_report().await);
                }
                Event::Key(key) if is_install_all_key(key) => {
                    install_all_actionable(&mut app, &mut terminal).await?;
                }
                Event::Key(key) if is_enter_key(key) => {
                    install_selected(&mut app, &mut terminal).await?;
                }
                Event::Key(key) if key.code == KeyCode::Up => app.move_selection(-1),
                Event::Key(key) if key.code == KeyCode::Down => app.move_selection(1),
                Event::Resize(_, _) => terminal.clear()?,
                _ => {}
            }
        }
    }
}

async fn install_all_actionable(
    app: &mut SetupApp,
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
) -> anyhow::Result<()> {
    let targets = app.installable_action_indices();
    if targets.is_empty() {
        app.push_log("no installable dependencies need action");
        return Ok(());
    }
    for index in targets {
        app.set_selected(index);
        install_selected(app, terminal).await?;
    }
    Ok(())
}

async fn install_selected(
    app: &mut SetupApp,
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
) -> anyhow::Result<()> {
    let Some(dependency) = app.selected_dependency().cloned() else {
        return Ok(());
    };
    let Some(plan) = dependency.install.clone() else {
        app.push_log(format!("{} needs no install action", dependency.label));
        return Ok(());
    };

    terminal.draw(|frame| app.draw(frame))?;
    app.push_log(format!("opening install step for {}", dependency.label));
    match run_install_plan_in_shell(&dependency, &plan) {
        Ok(outcome) => app.push_log(outcome),
        Err(error) => {
            app.push_log(format!(
                "{} setup did not complete: {error:#}",
                dependency.label
            ));
            app.push_log("review the terminal output, then retry or install manually");
        }
    }
    app.refresh_report(doctor::collect_report().await);
    terminal.clear()?;
    Ok(())
}

fn run_install_plan_in_shell(
    dependency: &DependencyReport,
    plan: &InstallPlan,
) -> anyhow::Result<String> {
    leave_tui()?;
    let result = run_install_plan_in_normal_terminal(dependency, plan);
    let resume = prompt_to_return();
    enter_tui()?;
    resume?;
    result
}

fn prompt_to_return() -> anyhow::Result<()> {
    println!();
    print!("Press Enter to return to peerline setup...");
    let _ = io::stdout().flush();
    let mut line = String::new();
    let _ = io::stdin().read_line(&mut line);
    Ok(())
}

fn enter_tui() -> anyhow::Result<()> {
    enable_raw_mode()?;
    crossterm::execute!(io::stdout(), EnterAlternateScreen)?;
    Ok(())
}

fn leave_tui() -> anyhow::Result<()> {
    crossterm::execute!(io::stdout(), LeaveAlternateScreen)?;
    disable_raw_mode()?;
    Ok(())
}

fn is_quit_key(key: KeyEvent) -> bool {
    matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
        || matches!(
            key.code,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL)
        )
}

fn is_refresh_key(key: KeyEvent) -> bool {
    matches!(key.code, KeyCode::Char('r') | KeyCode::Char('R')) && key.modifiers.is_empty()
}

fn is_install_all_key(key: KeyEvent) -> bool {
    matches!(key.code, KeyCode::Char('a') | KeyCode::Char('A')) && key.modifiers.is_empty()
}

fn is_enter_key(key: KeyEvent) -> bool {
    matches!(key.code, KeyCode::Enter)
}

struct TerminalCleanup;

impl Drop for TerminalCleanup {
    fn drop(&mut self) {
        let _ = crossterm::execute!(io::stdout(), LeaveAlternateScreen);
        let _ = disable_raw_mode();
    }
}
