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
        match run_install_command(command) {
            Ok(InstallCommandOutcome::Ran(status)) => {
                ran_any = true;
                if !status.success() {
                    println!(
                        "install step failed: `{}` exited with {status}",
                        command.display
                    );
                    print_failure_hint(command);
                    return Ok(format!(
                        "{} install command failed: `{}` exited with {status}",
                        dependency.label, command.display
                    ));
                }
            }
            Ok(InstallCommandOutcome::ManualOnly) => {
                println!("manual command; run it in your shell, then rerun `peerline setup`");
            }
            Ok(InstallCommandOutcome::Skipped) => {
                println!("skipped");
            }
            Err(error) => {
                println!("install step failed: {error}");
                print_failure_hint(command);
                return Err(error);
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
        .status()
        .map_err(|error| anyhow::anyhow!("could not launch `{}`: {error}", command.display))?;
    Ok(InstallCommandOutcome::Ran(status))
}

fn confirm_command(command: &InstallCommand) -> anyhow::Result<bool> {
    print!("Run `{}` now? [y/N] ", command.display);
    let _ = io::stdout().flush();
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    Ok(matches!(line.trim(), "y" | "Y" | "yes" | "YES" | "Yes"))
}

fn print_failure_hint(command: &InstallCommand) {
    let hint = if command.display.contains("choco ") {
        "Open an elevated PowerShell or Command Prompt, verify Chocolatey works with `choco -v`, then rerun `peerline setup`."
    } else if command.display.contains("brew ") {
        "Check Homebrew with `brew doctor`, make sure the network/repositories are reachable, then rerun `peerline setup`."
    } else if command.display.contains("apt-get ")
        || command.display.contains("dnf ")
        || command.display.contains("yum ")
        || command.display.contains("pacman ")
        || command.display.contains("zypper ")
        || command.display.contains("apk ")
    {
        "Check the package-manager output above for permissions, repository, lock, or network errors. Use the command for your Linux distro, then rerun `peerline setup`."
    } else {
        "Review the command output above, install the dependency manually if needed, then rerun `peerline setup`."
    };
    println!("hint: {hint}");
}
