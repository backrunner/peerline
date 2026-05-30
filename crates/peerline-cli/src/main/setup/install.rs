use crate::doctor::{DependencyReport, InstallCommand, InstallPlan};
use std::{
    io::{self, Write},
    process::{Command as ProcessCommand, Stdio},
};

pub(super) fn run_install_plan_in_normal_terminal(
    dependency: &DependencyReport,
    plan: &InstallPlan,
) -> anyhow::Result<String> {
    println!("peerline setup: {}", dependency.label);
    println!("{}", plan.summary);
    if !plan.notes.is_empty() {
        for note in &plan.notes {
            println!("note: {note}");
        }
    }
    let mut ran_any = false;
    for command in &plan.commands {
        println!();
        println!("$ {}", command.display);
        match run_install_command(command)? {
            InstallCommandOutcome::Ran(status) => {
                ran_any = true;
                if !status.success() {
                    println!("command exited with {status}");
                    return Ok(format!(
                        "{} install command failed with {status}",
                        dependency.label
                    ));
                }
            }
            InstallCommandOutcome::ManualOnly => {
                println!("manual command; run it in your shell, then rerun `peerline setup`");
            }
            InstallCommandOutcome::Skipped => {
                println!("skipped");
            }
        }
    }

    if ran_any {
        Ok(format!("{} install command completed", dependency.label))
    } else {
        Ok(format!("{} install instructions shown", dependency.label))
    }
}

enum InstallCommandOutcome {
    Ran(std::process::ExitStatus),
    ManualOnly,
    Skipped,
}

fn run_install_command(command: &InstallCommand) -> anyhow::Result<InstallCommandOutcome> {
    let Some(program) = command.program.as_ref() else {
        return Ok(InstallCommandOutcome::ManualOnly);
    };
    if !confirm_command(command)? {
        return Ok(InstallCommandOutcome::Skipped);
    }
    let status = ProcessCommand::new(program)
        .args(&command.args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()?;
    Ok(InstallCommandOutcome::Ran(status))
}

fn confirm_command(command: &InstallCommand) -> anyhow::Result<bool> {
    print!("Run `{}` now? [y/N] ", command.display);
    let _ = io::stdout().flush();
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    Ok(matches!(line.trim(), "y" | "Y" | "yes" | "YES" | "Yes"))
}
