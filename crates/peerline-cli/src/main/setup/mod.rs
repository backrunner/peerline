use clap::Args;
use std::io::{self, IsTerminal};

mod install;
mod text;
mod tui;

#[derive(Debug, Args)]
pub(super) struct SetupArgs {
    /// Print the setup plan without launching the interactive TUI.
    #[arg(long)]
    no_tui: bool,
}

pub(super) async fn run(args: SetupArgs) -> anyhow::Result<()> {
    let report = crate::doctor::collect_report().await;
    if args.no_tui || !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        println!("{}", text::render_setup_plan(&report));
        return Ok(());
    }

    tui::run_setup_tui(report).await
}
