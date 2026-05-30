use super::model::{PackageManager, PackageManagerReport, Platform};
use std::{
    env,
    path::{Path, PathBuf},
};

pub(super) fn detect_package_manager(platform: Platform) -> Option<PackageManagerReport> {
    package_manager_candidates(platform)
        .into_iter()
        .find_map(|kind| {
            command_path(kind.command()).map(|path| PackageManagerReport {
                kind,
                command: kind.command().into(),
                path: path.display().to_string(),
            })
        })
}

fn package_manager_candidates(platform: Platform) -> Vec<PackageManager> {
    match platform {
        Platform::Macos => vec![PackageManager::Brew],
        Platform::Windows => vec![PackageManager::Choco],
        Platform::Linux => vec![
            PackageManager::AptGet,
            PackageManager::Dnf,
            PackageManager::Yum,
            PackageManager::Pacman,
            PackageManager::Zypper,
            PackageManager::Apk,
            PackageManager::Brew,
        ],
        Platform::Other => vec![PackageManager::Brew],
    }
}

pub(super) fn command_path(command: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    for directory in env::split_paths(&path) {
        for candidate in command_candidates(&directory, command) {
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

fn command_candidates(directory: &Path, command: &str) -> Vec<PathBuf> {
    if cfg!(windows) {
        let extensions = env::var_os("PATHEXT")
            .map(|value| {
                env::split_paths(&value)
                    .filter_map(|path| {
                        path.file_name()
                            .and_then(|value| value.to_str())
                            .map(|value| value.to_ascii_lowercase())
                    })
                    .collect::<Vec<_>>()
            })
            .filter(|extensions| !extensions.is_empty())
            .unwrap_or_else(|| vec![".com".into(), ".exe".into(), ".bat".into(), ".cmd".into()]);
        let mut candidates = vec![directory.join(command)];
        candidates.extend(
            extensions
                .into_iter()
                .map(|extension| directory.join(format!("{command}{extension}"))),
        );
        candidates
    } else {
        vec![directory.join(command)]
    }
}
