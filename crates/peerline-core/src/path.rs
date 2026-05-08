use std::{
    ffi::OsStr,
    path::{Component, Path, PathBuf},
};
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConflictAction {
    OverwriteCurrent,
    OverwriteAll,
    KeepBoth,
    Cancel,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConflictDecision {
    pub action: ConflictAction,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PathError {
    #[error("absolute paths are not allowed in transfer manifests")]
    Absolute,
    #[error("parent path components are not allowed in transfer manifests")]
    ParentComponent,
    #[error("empty paths are not allowed in transfer manifests")]
    Empty,
    #[error("path contains a platform prefix")]
    Prefix,
}

pub fn safe_join_relative(root: &Path, relative: &Path) -> Result<PathBuf, PathError> {
    validate_relative_path(relative)?;
    Ok(root.join(relative))
}

pub fn validate_relative_path(path: &Path) -> Result<(), PathError> {
    if path.as_os_str().is_empty() {
        return Err(PathError::Empty);
    }
    if path.is_absolute() {
        return Err(PathError::Absolute);
    }
    if has_noncanonical_segments(path) {
        return Err(PathError::ParentComponent);
    }
    for component in path.components() {
        match component {
            Component::Normal(part) if part != OsStr::new("") => {}
            Component::CurDir => return Err(PathError::ParentComponent),
            Component::ParentDir => return Err(PathError::ParentComponent),
            Component::RootDir => return Err(PathError::Absolute),
            Component::Prefix(_) => return Err(PathError::Prefix),
            Component::Normal(_) => return Err(PathError::Empty),
        }
    }
    Ok(())
}

fn has_noncanonical_segments(path: &Path) -> bool {
    let separators: &[char] = if cfg!(windows) { &['/', '\\'] } else { &['/'] };
    path.to_string_lossy()
        .split(separators)
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
}

pub fn non_overwriting_path(path: &Path) -> PathBuf {
    if !path.exists() {
        return path.to_path_buf();
    }

    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("file");
    let ext = path.extension().and_then(|value| value.to_str());

    for index in 1.. {
        let file_name = match ext {
            Some(ext) => format!("{stem} ({index}).{ext}"),
            None => format!("{stem} ({index})"),
        };
        let candidate = parent.join(file_name);
        if !candidate.exists() {
            return candidate;
        }
    }

    unreachable!("unbounded loop returns once a free path is found")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unsafe_paths() {
        assert!(validate_relative_path(Path::new("../secret")).is_err());
        assert!(validate_relative_path(Path::new("/tmp/file")).is_err());
        assert!(validate_relative_path(Path::new("folder/./file")).is_err());
        assert!(validate_relative_path(Path::new("folder//file")).is_err());
        assert!(validate_relative_path(Path::new("folder/file")).is_ok());
    }

    #[test]
    fn keeps_both_when_file_exists() {
        let temp = tempfile::tempdir().unwrap();
        let original = temp.path().join("a.txt");
        std::fs::write(&original, "x").unwrap();
        assert_eq!(
            non_overwriting_path(&original),
            temp.path().join("a (1).txt")
        );
    }
}
