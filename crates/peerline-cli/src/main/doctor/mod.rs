use clap::Args;

mod checks;
pub(crate) mod model;
mod platform;
mod render;

pub(crate) use checks::collect_report;
pub(crate) use model::{
    DependencyReport, DependencyStatus, DoctorReport, InstallCommand, InstallPlan,
};

#[derive(Debug, Args)]
pub(super) struct DoctorArgs {
    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

pub(super) async fn run(args: DoctorArgs) -> anyhow::Result<()> {
    let report = collect_report().await;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("{}", render::render_human_report(&report));
    }
    Ok(())
}
