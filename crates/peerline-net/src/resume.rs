use peerline_core::{NodeId, ResourceId, TransferDescriptor};
use peerline_transfer::resource_id_for_reader;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{Seek, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub(crate) const PARTIAL_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone, Debug)]
pub(crate) struct ResumeState {
    pub(crate) path: PathBuf,
    pub(crate) metadata_path: PathBuf,
    pub(crate) offset: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PartialMetadata {
    source_id: NodeId,
    resource_id: ResourceId,
    archive_bytes: u64,
    logical_bytes: u64,
    files: usize,
    received_bytes: u64,
    updated_at_unix: u64,
}

pub(crate) fn resume_state(
    destination: &Path,
    descriptor: &TransferDescriptor,
) -> anyhow::Result<ResumeState> {
    cleanup_expired(destination)?;
    let dir = partial_dir(destination, descriptor.source_id, descriptor.resource_id);
    fs::create_dir_all(&dir)?;
    let part_path = dir.join("archive.part");
    let metadata_path = dir.join("metadata.json");

    let mut offset = 0;
    if part_path.exists() && metadata_path.exists() {
        match read_metadata(&metadata_path) {
            Ok(metadata) if metadata_matches(&metadata, descriptor) => {
                let part_len = fs::metadata(&part_path)?.len();
                if part_len <= descriptor.archive_bytes && part_len == metadata.received_bytes {
                    offset = part_len;
                } else {
                    remove_partial_paths(&part_path, &metadata_path)?;
                }
            }
            _ => remove_partial_paths(&part_path, &metadata_path)?,
        }
    } else if part_path.exists() || metadata_path.exists() {
        remove_partial_paths(&part_path, &metadata_path)?;
    }

    if offset == 0 {
        write_metadata(
            &metadata_path,
            &PartialMetadata {
                source_id: descriptor.source_id,
                resource_id: descriptor.resource_id,
                archive_bytes: descriptor.archive_bytes,
                logical_bytes: descriptor.logical_bytes,
                files: descriptor.files,
                received_bytes: 0,
                updated_at_unix: now_unix()?,
            },
        )?;
    }

    Ok(ResumeState {
        path: part_path,
        metadata_path,
        offset,
    })
}

pub(crate) fn append_chunk(
    state: &mut ResumeState,
    descriptor: &TransferDescriptor,
    bytes: &[u8],
) -> anyhow::Result<u64> {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&state.path)?;
    file.write_all(bytes)?;
    file.flush()?;
    state.offset = state
        .offset
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| anyhow::anyhow!("resume offset overflow"))?;
    if state.offset > descriptor.archive_bytes {
        anyhow::bail!("received more archive bytes than expected");
    }
    write_metadata(
        &state.metadata_path,
        &PartialMetadata {
            source_id: descriptor.source_id,
            resource_id: descriptor.resource_id,
            archive_bytes: descriptor.archive_bytes,
            logical_bytes: descriptor.logical_bytes,
            files: descriptor.files,
            received_bytes: state.offset,
            updated_at_unix: now_unix()?,
        },
    )?;
    Ok(state.offset)
}

pub(crate) fn complete_partial(
    state: &ResumeState,
    descriptor: &TransferDescriptor,
) -> anyhow::Result<fs::File> {
    let mut file = fs::OpenOptions::new().read(true).open(&state.path)?;
    let len = file.metadata()?.len();
    if len != descriptor.archive_bytes {
        remove_partial(state)?;
        anyhow::bail!("partial archive size mismatch");
    }
    let resource_id = resource_id_for_reader(file.try_clone()?)?;
    if resource_id != descriptor.resource_id {
        remove_partial(state)?;
        anyhow::bail!("partial archive hash mismatch");
    }
    file.rewind()?;
    Ok(file)
}

pub(crate) fn remove_partial(state: &ResumeState) -> anyhow::Result<()> {
    remove_partial_paths(&state.path, &state.metadata_path)
}

fn cleanup_expired(destination: &Path) -> anyhow::Result<()> {
    let root = resume_root(destination);
    let Ok(sources) = fs::read_dir(&root) else {
        return Ok(());
    };
    let cutoff = now_unix()?.saturating_sub(PARTIAL_RETENTION.as_secs());
    for source in sources.flatten() {
        let Ok(resources) = fs::read_dir(source.path()) else {
            continue;
        };
        for resource in resources.flatten() {
            let metadata_path = resource.path().join("metadata.json");
            let stale = read_metadata(&metadata_path)
                .map(|metadata| metadata.updated_at_unix < cutoff)
                .unwrap_or(true);
            if stale {
                let _ = fs::remove_dir_all(resource.path());
            }
        }
        if fs::read_dir(source.path())
            .map(|mut entries| entries.next().is_none())
            .unwrap_or(false)
        {
            let _ = fs::remove_dir(source.path());
        }
    }
    Ok(())
}

fn partial_dir(destination: &Path, source_id: NodeId, resource_id: ResourceId) -> PathBuf {
    resume_root(destination)
        .join(source_id.hex())
        .join(resource_id.hex())
}

fn resume_root(destination: &Path) -> PathBuf {
    destination.join(".peerline-resume")
}

fn metadata_matches(metadata: &PartialMetadata, descriptor: &TransferDescriptor) -> bool {
    metadata.source_id == descriptor.source_id
        && metadata.resource_id == descriptor.resource_id
        && metadata.archive_bytes == descriptor.archive_bytes
        && metadata.logical_bytes == descriptor.logical_bytes
        && metadata.files == descriptor.files
}

fn read_metadata(path: &Path) -> anyhow::Result<PartialMetadata> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn write_metadata(path: &Path, metadata: &PartialMetadata) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temp_path = path.with_extension("json.tmp");
    let payload = serde_json::to_vec_pretty(metadata)?;
    fs::write(&temp_path, payload)?;
    fs::rename(temp_path, path)?;
    Ok(())
}

fn remove_partial_paths(part_path: &Path, metadata_path: &Path) -> anyhow::Result<()> {
    match fs::remove_file(part_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    match fs::remove_file(metadata_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    prune_empty_partial_dirs(part_path);
    Ok(())
}

fn prune_empty_partial_dirs(part_path: &Path) {
    let Some(resource_dir) = part_path.parent() else {
        return;
    };
    let Some(source_dir) = resource_dir.parent() else {
        return;
    };
    let Some(root_dir) = source_dir.parent() else {
        return;
    };

    for dir in [resource_dir, source_dir, root_dir] {
        if fs::read_dir(dir)
            .map(|mut entries| entries.next().is_none())
            .unwrap_or(false)
        {
            let _ = fs::remove_dir(dir);
        }
    }
}

fn now_unix() -> anyhow::Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use peerline_core::NodeId;
    use peerline_transfer::create_archive;

    fn descriptor_for_archive(
        archive: &peerline_transfer::Archive,
        source_id: NodeId,
    ) -> TransferDescriptor {
        TransferDescriptor {
            source_id,
            resource_id: archive.resource_id,
            archive_bytes: archive.len(),
            logical_bytes: archive.manifest.total_bytes,
            files: archive
                .manifest
                .entries
                .iter()
                .filter(|entry| entry.blake3.is_some())
                .count(),
            compression: archive.compression,
        }
    }

    #[test]
    fn resume_state_reuses_existing_partial_for_same_source_and_resource() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("source.txt");
        fs::write(&src, "hello peerline").unwrap();
        let archive =
            create_archive(std::slice::from_ref(&src), peerline_core::Compression::None).unwrap();
        let descriptor = descriptor_for_archive(&archive, NodeId::random());

        let mut first = resume_state(temp.path(), &descriptor).unwrap();
        assert_eq!(first.offset, 0);
        append_chunk(&mut first, &descriptor, b"abc").unwrap();

        let second = resume_state(temp.path(), &descriptor).unwrap();
        assert_eq!(second.offset, 3);
        assert_eq!(second.path, first.path);
        assert_eq!(second.metadata_path, first.metadata_path);
    }

    #[test]
    fn resume_state_keeps_partials_separate_by_source_or_resource() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("source.txt");
        fs::write(&src, "hello peerline").unwrap();
        let archive =
            create_archive(std::slice::from_ref(&src), peerline_core::Compression::None).unwrap();
        let source_a = NodeId::random();
        let source_b = NodeId::random();
        let descriptor_a = descriptor_for_archive(&archive, source_a);
        let descriptor_b = descriptor_for_archive(&archive, source_b);

        let mut state_a = resume_state(temp.path(), &descriptor_a).unwrap();
        append_chunk(&mut state_a, &descriptor_a, b"abc").unwrap();

        let state_b = resume_state(temp.path(), &descriptor_b).unwrap();
        assert_eq!(state_b.offset, 0);
        assert_ne!(state_a.path, state_b.path);

        let mut altered = descriptor_for_archive(&archive, source_a);
        altered.resource_id = peerline_core::ResourceId::from_bytes([9u8; 32]);
        let state_c = resume_state(temp.path(), &altered).unwrap();
        assert_eq!(state_c.offset, 0);
        assert_ne!(state_a.path, state_c.path);
    }

    #[test]
    fn expired_partials_are_cleaned_before_resume() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("source.txt");
        fs::write(&src, "hello peerline").unwrap();
        let archive =
            create_archive(std::slice::from_ref(&src), peerline_core::Compression::None).unwrap();
        let descriptor = descriptor_for_archive(&archive, NodeId::random());

        let state = resume_state(temp.path(), &descriptor).unwrap();
        let metadata = PartialMetadata {
            source_id: descriptor.source_id,
            resource_id: descriptor.resource_id,
            archive_bytes: descriptor.archive_bytes,
            logical_bytes: descriptor.logical_bytes,
            files: descriptor.files,
            received_bytes: 4,
            updated_at_unix: 0,
        };
        fs::write(
            &state.metadata_path,
            serde_json::to_vec_pretty(&metadata).unwrap(),
        )
        .unwrap();
        fs::write(&state.path, b"abcd").unwrap();

        let refreshed = resume_state(temp.path(), &descriptor).unwrap();
        assert_eq!(refreshed.offset, 0);
        assert!(!refreshed.path.exists());
        assert!(refreshed.metadata_path.exists());
    }

    #[test]
    fn corrupt_partial_is_deleted_when_completion_fails() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("source.txt");
        fs::write(&src, "hello peerline").unwrap();
        let archive =
            create_archive(std::slice::from_ref(&src), peerline_core::Compression::None).unwrap();
        let descriptor = descriptor_for_archive(&archive, NodeId::random());

        let mut state = resume_state(temp.path(), &descriptor).unwrap();
        append_chunk(&mut state, &descriptor, b"abcd").unwrap();
        fs::write(&state.path, b"wxyz").unwrap();

        assert!(complete_partial(&state, &descriptor).is_err());
        assert!(!state.path.exists());
        assert!(!state.metadata_path.exists());
    }
}
