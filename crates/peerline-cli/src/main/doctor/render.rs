use super::model::{ConfigReport, ConfigStatus, DoctorReport};

pub(super) fn render_human_report(report: &DoctorReport) -> String {
    let mut lines = Vec::new();
    lines.push("peerline doctor".to_string());
    lines.push(format!(
        "platform: {} {}",
        report.platform.os.label(),
        report.platform.arch
    ));
    match &report.package_manager {
        Some(manager) => lines.push(format!(
            "package manager: {} ({})",
            manager.label(),
            manager.path
        )),
        None => lines.push("package manager: not detected".to_string()),
    }

    lines.push(format_config_line(&report.config));
    lines.push(String::new());
    lines.push("dependencies:".to_string());
    for dependency in &report.dependencies {
        lines.push(format!(
            "  [{:<7}] {} - {}",
            dependency.status.label(),
            dependency.label,
            dependency.detail
        ));
        if let Some(version) = &dependency.version {
            lines.push(format!("            version: {version}"));
        }
        if let Some(path) = &dependency.command_path {
            lines.push(format!("            path: {path}"));
        }
        if let Some(plan) = &dependency.install {
            lines.push(format!("            fix: {}", plan.summary));
            for command in &plan.commands {
                lines.push(format!("                 {}", command.display));
            }
            for note in &plan.notes {
                lines.push(format!("                 note: {note}"));
            }
        }
    }

    if !report.notes.is_empty() {
        lines.push(String::new());
        lines.push("notes:".to_string());
        lines.extend(report.notes.iter().map(|note| format!("  - {note}")));
    }

    let actionable = report.actionable_dependencies();
    if !actionable.is_empty() {
        lines.push(String::new());
        lines.push(format!(
            "next: run `peerline setup` to resolve {} item(s)",
            actionable.len()
        ));
    }

    lines.join("\n")
}

fn format_config_line(config: &ConfigReport) -> String {
    match config.status {
        ConfigStatus::Ready => {
            let name = config.saved_name.as_deref().unwrap_or("not set");
            format!(
                "config: ready at {} (name: {name}, node id: {})",
                config.path.as_deref().unwrap_or("unknown"),
                if config.node_id_present {
                    "present"
                } else {
                    "not generated"
                }
            )
        }
        ConfigStatus::Missing => format!(
            "config: missing; peerline will create {} when needed",
            config.path.as_deref().unwrap_or("a user config file")
        ),
        ConfigStatus::Error => format!(
            "config: error at {} ({})",
            config.path.as_deref().unwrap_or("unknown"),
            config.error.as_deref().unwrap_or("could not read config")
        ),
    }
}
