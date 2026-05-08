use peerline_core::manifest::{EntryKind, ManifestEntry};
use std::{
    ffi::OsStr,
    io::Read,
    path::{Path, PathBuf},
};
use walkdir::WalkDir;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceEntry {
    pub source_path: PathBuf,
    pub relative_path: PathBuf,
    pub manifest_entry: ManifestEntry,
}

pub fn scan_sources(paths: &[PathBuf]) -> anyhow::Result<Vec<SourceEntry>> {
    let mut entries = Vec::new();
    for path in paths {
        let metadata = std::fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() {
            anyhow::bail!("symbolic links are not supported: {}", path.display());
        }
        if metadata.is_dir() {
            let base_name = transfer_base_name(path)?;
            scan_directory(path, &base_name, &mut entries)?;
        } else if metadata.is_file() {
            let file_name = path
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("file path has no file name: {}", path.display()))?;
            entries.push(scan_file(path, file_name.into())?);
        }
    }
    entries.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(entries)
}

fn scan_directory(
    root: &Path,
    base_name: &Path,
    entries: &mut Vec<SourceEntry>,
) -> anyhow::Result<()> {
    for item in WalkDir::new(root).follow_links(false).sort_by_file_name() {
        let item = item?;
        let path = item.path();
        let relative = if path == root {
            base_name.to_path_buf()
        } else {
            base_name.join(path.strip_prefix(root)?)
        };
        let file_type = item.file_type();
        if file_type.is_symlink() {
            anyhow::bail!("symbolic links are not supported: {}", path.display());
        }
        if file_type.is_dir() {
            entries.push(SourceEntry {
                source_path: path.to_path_buf(),
                relative_path: relative.clone(),
                manifest_entry: ManifestEntry {
                    path: relative,
                    kind: EntryKind::Directory,
                    size: 0,
                    blake3: None,
                },
            });
        } else if file_type.is_file() {
            entries.push(scan_file(path, relative)?);
        } else {
            anyhow::bail!("unsupported filesystem entry: {}", path.display());
        }
    }
    Ok(())
}

fn transfer_base_name(path: &Path) -> anyhow::Result<PathBuf> {
    if let Some(name) = path
        .file_name()
        .filter(|name| !name.is_empty() && *name != OsStr::new("."))
    {
        return Ok(PathBuf::from(name));
    }

    let canonical = std::fs::canonicalize(path)?;
    let name = canonical.file_name().ok_or_else(|| {
        anyhow::anyhow!(
            "directory path has no transferable base name: {}",
            path.display()
        )
    })?;
    Ok(PathBuf::from(name))
}

fn scan_file(path: &Path, relative_path: PathBuf) -> anyhow::Result<SourceEntry> {
    let metadata = std::fs::metadata(path)?;
    let hash = hash_file(path)?;
    Ok(SourceEntry {
        source_path: path.to_path_buf(),
        relative_path: relative_path.clone(),
        manifest_entry: ManifestEntry {
            path: relative_path,
            kind: EntryKind::File,
            size: metadata.len(),
            blake3: Some(hash),
        },
    })
}

fn hash_file(path: &Path) -> anyhow::Result<[u8; 32]> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(*hasher.finalize().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn scans_directory_with_base_name() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("folder");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("a.txt"), "hello").unwrap();
        let entries = scan_sources(&[root]).unwrap();
        assert!(
            entries
                .iter()
                .any(|entry| entry.relative_path == Path::new("folder"))
        );
        assert!(
            entries
                .iter()
                .any(|entry| entry.relative_path == Path::new("folder/a.txt"))
        );
    }

    #[test]
    fn current_directory_has_transferable_base_name() {
        let base_name = transfer_base_name(Path::new(".")).unwrap();
        assert!(!base_name.as_os_str().is_empty());
        assert_ne!(base_name, Path::new("."));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_nested_symlink() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("folder");
        let nested = root.join("nested");
        fs::create_dir(&root).unwrap();
        fs::create_dir(&nested).unwrap();
        fs::write(root.join("target.txt"), "secret").unwrap();
        symlink(root.join("target.txt"), nested.join("link.txt")).unwrap();

        assert!(scan_sources(&[root]).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_top_level_symlink() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("target.txt");
        let link = temp.path().join("link.txt");
        std::fs::write(&target, "secret").unwrap();
        symlink(&target, &link).unwrap();

        assert!(scan_sources(&[link]).is_err());
    }
}
