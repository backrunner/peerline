use crate::doctor::DoctorReport;

pub(super) fn render_setup_plan(report: &DoctorReport) -> String {
    let mut lines = Vec::new();
    lines.push("peerline setup".to_string());
    let actionable = report.actionable_dependencies();
    if actionable.is_empty() {
        lines.push("All checked dependencies look ready.".to_string());
    } else {
        lines.push(format!("{} item(s) need attention:", actionable.len()));
        for dependency in actionable {
            lines.push(format!("  - {}: {}", dependency.label, dependency.detail));
            if let Some(plan) = dependency.install.as_ref() {
                lines.push(format!("    {}", plan.summary));
                for command in &plan.commands {
                    lines.push(format!("    {}", command.display));
                }
                for note in &plan.notes {
                    lines.push(format!("    note: {note}"));
                }
            }
        }
    }
    if !report.notes.is_empty() {
        lines.push(String::new());
        lines.push("notes:".into());
        lines.extend(report.notes.iter().map(|note| format!("  - {note}")));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doctor::model::{
        ConfigReport, ConfigStatus, DependencyReport, DependencyStatus, InstallCommand,
        InstallPlan, Platform, PlatformReport,
    };

    #[test]
    fn setup_plan_lists_missing_dependency_commands() {
        let report = DoctorReport {
            platform: PlatformReport {
                os: Platform::Linux,
                arch: "x86_64".into(),
            },
            package_manager: None,
            config: ConfigReport {
                path: None,
                status: ConfigStatus::Missing,
                saved_name: None,
                node_id_present: false,
                error: None,
            },
            dependencies: vec![DependencyReport {
                key: "tor".into(),
                label: "Tor onion routing".into(),
                status: DependencyStatus::Missing,
                detail: "tor command is missing".into(),
                command_path: None,
                version: None,
                install: Some(InstallPlan {
                    summary: "install Tor".into(),
                    commands: vec![InstallCommand::manual("brew install tor")],
                    notes: vec![],
                }),
            }],
            notes: vec![],
        };

        let output = render_setup_plan(&report);

        assert!(output.contains("1 item(s) need attention"));
        assert!(output.contains("brew install tor"));
    }
}
