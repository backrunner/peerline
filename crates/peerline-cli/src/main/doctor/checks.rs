use super::{
    model::{
        ConfigReport, ConfigStatus, DependencyReport, DependencyStatus, DoctorReport,
        InstallCommand, InstallPlan, PackageManager, PackageManagerReport, Platform,
        PlatformReport,
    },
    platform::{command_path, detect_package_manager},
};
use peerline_core::ConfigStore;
use std::{env, path::Path, process::Stdio, time::Duration};
use tokio::{process::Command as TokioCommand, time};

const VERSION_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) async fn collect_report() -> DoctorReport {
    let platform = PlatformReport::current();
    let package_manager = detect_package_manager(platform.os);
    let config = config_report();
    let tor = tor_report(platform.os, package_manager.as_ref()).await;
    let mainline = pkarr_mainline_report();
    let notes = setup_notes(platform.os, package_manager.as_ref());

    DoctorReport {
        platform,
        package_manager,
        config,
        dependencies: vec![tor, mainline],
        notes,
    }
}

async fn tor_report(
    platform: Platform,
    package_manager: Option<&PackageManagerReport>,
) -> DependencyReport {
    let install = tor_install_plan(platform, package_manager);
    let tor_disabled = env_flag("PEERLINE_DISABLE_TOR");
    match command_path("tor") {
        Some(path) => match command_version(&path).await {
            Ok(version) => DependencyReport {
                key: "tor".into(),
                label: "Tor onion routing".into(),
                status: DependencyStatus::Ok,
                detail: if tor_disabled {
                    "tor command is installed, but PEERLINE_DISABLE_TOR is set".into()
                } else {
                    "tor command is available for onion receive/send routes".into()
                },
                command_path: Some(path.display().to_string()),
                version: Some(version),
                install: None,
            },
            Err(error) => DependencyReport {
                key: "tor".into(),
                label: "Tor onion routing".into(),
                status: DependencyStatus::Broken,
                detail: format!("tor command was found but did not run cleanly: {error}"),
                command_path: Some(path.display().to_string()),
                version: None,
                install,
            },
        },
        None => DependencyReport {
            key: "tor".into(),
            label: "Tor onion routing".into(),
            status: DependencyStatus::Missing,
            detail: if tor_disabled {
                "tor command is missing; Tor routes are disabled by PEERLINE_DISABLE_TOR".into()
            } else {
                "tor command is missing; Peerline will skip onion routes".into()
            },
            command_path: None,
            version: None,
            install,
        },
    }
}

fn pkarr_mainline_report() -> DependencyReport {
    let bootstrap = env::var("PEERLINE_PKARR_BOOTSTRAP")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let bootstrap_detail = match bootstrap {
        Some(value) => format!("custom bootstrap via PEERLINE_PKARR_BOOTSTRAP ({value})"),
        None => "default pkarr/mainline bootstrap network".to_string(),
    };

    DependencyReport {
        key: "pkarr_mainline".into(),
        label: "pkarr/mainline discovery".into(),
        status: DependencyStatus::Ok,
        detail: format!(
            "built into peerline; no separate bt/mainline binary is required ({bootstrap_detail})"
        ),
        command_path: None,
        version: None,
        install: None,
    }
}

fn config_report() -> ConfigReport {
    let store = match ConfigStore::user_default() {
        Ok(store) => store,
        Err(error) => {
            return ConfigReport {
                path: None,
                status: ConfigStatus::Error,
                saved_name: None,
                node_id_present: false,
                error: Some(error.to_string()),
            };
        }
    };
    let path = Some(store.path().display().to_string());
    match store.load() {
        Ok(config) if store.path().exists() => ConfigReport {
            path,
            status: ConfigStatus::Ready,
            saved_name: config.name.map(|name| name.to_string()),
            node_id_present: config.node_id.is_some(),
            error: None,
        },
        Ok(config) => ConfigReport {
            path,
            status: ConfigStatus::Missing,
            saved_name: config.name.map(|name| name.to_string()),
            node_id_present: config.node_id.is_some(),
            error: None,
        },
        Err(error) => ConfigReport {
            path,
            status: ConfigStatus::Error,
            saved_name: None,
            node_id_present: false,
            error: Some(error.to_string()),
        },
    }
}

fn setup_notes(platform: Platform, package_manager: Option<&PackageManagerReport>) -> Vec<String> {
    let mut notes = vec![
        "Tor is optional but strongly improves routing when direct and relay paths are blocked."
            .to_string(),
        "pkarr/mainline discovery ships inside peerline, so setup only verifies it is configured."
            .to_string(),
    ];
    if platform == Platform::Windows && package_manager.is_none() {
        notes
            .push("Windows setup expects Chocolatey (`choco`) for dependency installation.".into());
    }
    notes
}

fn tor_install_plan(
    platform: Platform,
    package_manager: Option<&PackageManagerReport>,
) -> Option<InstallPlan> {
    let Some(manager) = package_manager else {
        return Some(manual_tor_install_plan(platform));
    };

    let commands = match manager.kind {
        PackageManager::Brew => vec![InstallCommand::executable("brew", ["install", "tor"])],
        PackageManager::AptGet => vec![
            privileged_command("apt-get", ["update"]),
            privileged_command("apt-get", ["install", "-y", "tor"]),
        ],
        PackageManager::Dnf => {
            vec![privileged_command("dnf", ["install", "-y", "tor"])]
        }
        PackageManager::Yum => {
            vec![privileged_command("yum", ["install", "-y", "tor"])]
        }
        PackageManager::Pacman => vec![privileged_command(
            "pacman",
            ["-S", "--needed", "--noconfirm", "tor"],
        )],
        PackageManager::Zypper => {
            vec![privileged_command("zypper", ["install", "-y", "tor"])]
        }
        PackageManager::Apk => {
            vec![privileged_command("apk", ["add", "tor"])]
        }
        PackageManager::Choco => {
            vec![InstallCommand::executable(
                "choco",
                ["install", "tor", "-y"],
            )]
        }
    };

    let mut notes = Vec::new();
    if matches!(
        manager.kind,
        PackageManager::AptGet
            | PackageManager::Dnf
            | PackageManager::Yum
            | PackageManager::Pacman
            | PackageManager::Zypper
            | PackageManager::Apk
    ) && command_path("sudo").is_none()
    {
        notes.push(
            "Run from a root shell if the package manager reports a permission error.".into(),
        );
    }
    if let Some(family) = manager.kind.linux_family_hint() {
        notes.push(format!(
            "{} is the expected package manager for the {family} family.",
            manager.command
        ));
    }
    if manager.kind == PackageManager::Choco {
        notes
            .push("Run from an elevated Windows shell if Chocolatey asks for admin rights.".into());
    }

    Some(InstallPlan {
        summary: format!("install Tor via {}", manager.label()),
        commands,
        notes,
    })
}

fn manual_tor_install_plan(platform: Platform) -> InstallPlan {
    match platform {
        Platform::Windows => InstallPlan {
            summary: "install Tor with Chocolatey".into(),
            commands: vec![InstallCommand::manual("choco install tor -y")],
            notes: vec!["Install Chocolatey first, then rerun `peerline setup`.".into()],
        },
        Platform::Macos => InstallPlan {
            summary: "install Tor with Homebrew".into(),
            commands: vec![InstallCommand::manual("brew install tor")],
            notes: vec!["Install Homebrew first if `brew` is not on PATH.".into()],
        },
        Platform::Linux => InstallPlan {
            summary: "install Tor with your distro package manager".into(),
            commands: vec![
                InstallCommand::manual("sudo apt-get update && sudo apt-get install -y tor"),
                InstallCommand::manual("sudo dnf install -y tor"),
                InstallCommand::manual("sudo yum install -y tor"),
                InstallCommand::manual("sudo pacman -S --needed tor"),
                InstallCommand::manual("sudo zypper install -y tor"),
                InstallCommand::manual("sudo apk add tor"),
            ],
            notes: vec![
                "Choose the command for your distro: apt-get for Debian/Ubuntu/Raspberry Pi OS, dnf/yum for Fedora/RHEL/CentOS, pacman for Arch/Manjaro, zypper for openSUSE/SLES, apk for Alpine.".into(),
                "Rerun `peerline setup` after installing Tor.".into(),
            ],
        },
        Platform::Other => InstallPlan {
            summary: "install Tor with your OS package manager".into(),
            commands: vec![InstallCommand::manual(
                "install a package that provides `tor`",
            )],
            notes: vec!["Rerun `peerline setup` after the `tor` command is on PATH.".into()],
        },
    }
}

fn privileged_command(
    program: impl Into<String>,
    args: impl IntoIterator<Item = impl Into<String>>,
) -> InstallCommand {
    let program = program.into();
    let args = args.into_iter().map(Into::into).collect::<Vec<_>>();
    if command_path("sudo").is_some() {
        let mut sudo_args = Vec::with_capacity(args.len() + 1);
        sudo_args.push(program);
        sudo_args.extend(args);
        InstallCommand::executable("sudo", sudo_args)
    } else {
        InstallCommand::executable(program, args)
    }
}

async fn command_version(path: &Path) -> Result<String, String> {
    let output = time::timeout(
        VERSION_TIMEOUT,
        TokioCommand::new(path)
            .arg("--version")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output(),
    )
    .await
    .map_err(|_| format!("timed out after {}s", VERSION_TIMEOUT.as_secs()))?
    .map_err(|error| error.to_string())?;

    let mut raw = String::new();
    raw.push_str(&String::from_utf8_lossy(&output.stdout));
    if raw.trim().is_empty() {
        raw.push_str(&String::from_utf8_lossy(&output.stderr));
    }
    let first_line = raw.lines().find(|line| !line.trim().is_empty());
    if output.status.success() {
        Ok(first_line
            .unwrap_or("version output unavailable")
            .trim()
            .to_string())
    } else {
        Err(first_line
            .unwrap_or("version command exited unsuccessfully")
            .trim()
            .to_string())
    }
}

fn env_flag(name: &str) -> bool {
    env::var_os(name).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan_for(kind: PackageManager) -> InstallPlan {
        let report = PackageManagerReport {
            kind,
            command: kind.command().into(),
            path: format!("/usr/bin/{}", kind.command()),
        };
        tor_install_plan(Platform::Linux, Some(&report)).unwrap()
    }

    #[test]
    fn tor_install_plan_uses_chocolatey_on_windows() {
        let plan = plan_for(PackageManager::Choco);

        assert_eq!(plan.commands[0].display, "choco install tor -y");
        assert!(plan.commands[0].program.is_some());
    }

    #[test]
    fn tor_install_plan_uses_homebrew_on_macos() {
        let plan = plan_for(PackageManager::Brew);

        assert_eq!(plan.commands[0].display, "brew install tor");
    }

    #[test]
    fn tor_install_plan_includes_apt_update_then_install() {
        let plan = plan_for(PackageManager::AptGet);
        let display = plan
            .commands
            .iter()
            .map(|command| command.display.as_str())
            .collect::<Vec<_>>();

        assert!(
            display
                .iter()
                .any(|command| command.ends_with("apt-get update"))
        );
        assert!(
            display
                .iter()
                .any(|command| command.ends_with("apt-get install -y tor"))
        );
    }

    #[test]
    fn tor_install_plan_supports_alpine_apk() {
        let plan = plan_for(PackageManager::Apk);

        assert!(plan.commands[0].display.ends_with("apk add tor"));
        assert!(plan.notes.iter().any(|note| note.contains("Alpine family")));
    }

    #[test]
    fn linux_manual_plan_names_common_distro_package_managers() {
        let plan = manual_tor_install_plan(Platform::Linux);
        let commands = plan
            .commands
            .iter()
            .map(|command| command.display.as_str())
            .collect::<Vec<_>>();

        assert!(
            commands
                .iter()
                .any(|command| command.contains("apt-get install"))
        );
        assert!(
            commands
                .iter()
                .any(|command| command.contains("dnf install"))
        );
        assert!(
            commands
                .iter()
                .any(|command| command.contains("yum install"))
        );
        assert!(commands.iter().any(|command| command.contains("pacman -S")));
        assert!(
            commands
                .iter()
                .any(|command| command.contains("zypper install"))
        );
        assert!(commands.iter().any(|command| command.contains("apk add")));
        assert!(
            plan.notes
                .iter()
                .any(|note| note.contains("Debian/Ubuntu") && note.contains("Alpine"))
        );
    }

    #[test]
    fn other_platform_manual_plan_does_not_claim_a_version_command_installs_tor() {
        let plan = manual_tor_install_plan(Platform::Other);

        assert_eq!(
            plan.commands[0].display,
            "install a package that provides `tor`"
        );
    }

    #[test]
    fn mainline_report_says_no_external_binary_is_required() {
        let report = pkarr_mainline_report();

        assert_eq!(report.status, DependencyStatus::Ok);
        assert!(report.detail.contains("no separate bt/mainline binary"));
        assert!(report.install.is_none());
    }
}
