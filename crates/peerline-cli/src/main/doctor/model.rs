use serde::Serialize;
use std::fmt;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct DoctorReport {
    pub(crate) platform: PlatformReport,
    pub(crate) package_manager: Option<PackageManagerReport>,
    pub(crate) config: ConfigReport,
    pub(crate) dependencies: Vec<DependencyReport>,
    pub(crate) notes: Vec<String>,
}

impl DoctorReport {
    pub(crate) fn actionable_dependencies(&self) -> Vec<&DependencyReport> {
        self.dependencies
            .iter()
            .filter(|dependency| dependency.needs_action())
            .collect()
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct PlatformReport {
    pub(crate) os: Platform,
    pub(crate) arch: String,
}

impl PlatformReport {
    pub(crate) fn current() -> Self {
        Self {
            os: Platform::current(),
            arch: std::env::consts::ARCH.to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Platform {
    Macos,
    Linux,
    Windows,
    Other,
}

impl Platform {
    fn current() -> Self {
        match std::env::consts::OS {
            "macos" => Self::Macos,
            "linux" => Self::Linux,
            "windows" => Self::Windows,
            _ => Self::Other,
        }
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Macos => "macOS",
            Self::Linux => "Linux",
            Self::Windows => "Windows",
            Self::Other => std::env::consts::OS,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct PackageManagerReport {
    pub(crate) kind: PackageManager,
    pub(crate) command: String,
    pub(crate) path: String,
}

impl PackageManagerReport {
    pub(crate) fn label(&self) -> &'static str {
        self.kind.label()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PackageManager {
    Brew,
    AptGet,
    Dnf,
    Yum,
    Pacman,
    Zypper,
    Apk,
    Choco,
}

impl PackageManager {
    pub(crate) fn command(self) -> &'static str {
        match self {
            Self::Brew => "brew",
            Self::AptGet => "apt-get",
            Self::Dnf => "dnf",
            Self::Yum => "yum",
            Self::Pacman => "pacman",
            Self::Zypper => "zypper",
            Self::Apk => "apk",
            Self::Choco => "choco",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Brew => "Homebrew",
            Self::AptGet => "apt-get",
            Self::Dnf => "dnf",
            Self::Yum => "yum",
            Self::Pacman => "pacman",
            Self::Zypper => "zypper",
            Self::Apk => "apk",
            Self::Choco => "Chocolatey",
        }
    }

    pub(crate) fn linux_family_hint(self) -> Option<&'static str> {
        match self {
            Self::AptGet => Some("Debian/Ubuntu/Raspberry Pi OS"),
            Self::Dnf => Some("Fedora/RHEL/CentOS"),
            Self::Yum => Some("older RHEL/CentOS"),
            Self::Pacman => Some("Arch/Manjaro"),
            Self::Zypper => Some("openSUSE/SLES"),
            Self::Apk => Some("Alpine"),
            Self::Brew | Self::Choco => None,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ConfigReport {
    pub(crate) path: Option<String>,
    pub(crate) status: ConfigStatus,
    pub(crate) saved_name: Option<String>,
    pub(crate) node_id_present: bool,
    pub(crate) error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ConfigStatus {
    Ready,
    Missing,
    Error,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct DependencyReport {
    pub(crate) key: String,
    pub(crate) label: String,
    pub(crate) status: DependencyStatus,
    pub(crate) detail: String,
    pub(crate) command_path: Option<String>,
    pub(crate) version: Option<String>,
    pub(crate) install: Option<InstallPlan>,
}

impl DependencyReport {
    pub(crate) fn needs_action(&self) -> bool {
        matches!(
            self.status,
            DependencyStatus::Missing | DependencyStatus::Broken
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DependencyStatus {
    Ok,
    Missing,
    Broken,
}

impl DependencyStatus {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Missing => "missing",
            Self::Broken => "broken",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct InstallPlan {
    pub(crate) summary: String,
    pub(crate) commands: Vec<InstallCommand>,
    pub(crate) notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct InstallCommand {
    pub(crate) display: String,
    pub(crate) program: Option<String>,
    pub(crate) args: Vec<String>,
}

impl InstallCommand {
    pub(crate) fn executable(
        program: impl Into<String>,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let program = program.into();
        let args = args.into_iter().map(Into::into).collect::<Vec<_>>();
        let display = shell_display(&program, &args);
        Self {
            display,
            program: Some(program),
            args,
        }
    }

    pub(crate) fn manual(display: impl Into<String>) -> Self {
        Self {
            display: display.into(),
            program: None,
            args: Vec::new(),
        }
    }
}

impl fmt::Display for DependencyStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.label())
    }
}

fn shell_display(program: &str, args: &[String]) -> String {
    std::iter::once(program)
        .chain(args.iter().map(String::as_str))
        .map(shell_quote)
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_quote(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '/' | ':'))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_display_quotes_spaces() {
        assert_eq!(
            shell_display("cmd", &["argument with space".into()]),
            "cmd 'argument with space'"
        );
    }
}
