use peerline_core::{NodeId, ResourceId, TransferDescriptor};
use peerline_transfer::resource_id_for_reader;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{self, Seek, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub(crate) const PARTIAL_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

// One batch on disk's blocking worker and one filling from the network.
const BATCH_BYTES: usize = 1024 * 1024;
const CHECKPOINT_BYTES: u64 = 8 * 1024 * 1024;
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub(crate) struct ResumeState {
    path: PathBuf,
    metadata_path: PathBuf,
    pub(crate) offset: u64,
    file: Option<Arc<PartialFile>>,
    buffer: Vec<u8>,
    pending: Option<tokio::task::JoinHandle<anyhow::Result<Vec<u8>>>>,
    last_checkpoint: Instant,
    last_checkpoint_offset: u64,
}

#[derive(Debug)]
struct PartialFile {
    archive: fs::File,
    // Kept by both the session and any in-flight blocking write.
    lease: fs::File,
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
    let path = dir.join("archive.part");
    let metadata_path = dir.join("metadata.json");
    let offset = checkpoint_offset(&path, &metadata_path, descriptor)?;
    Ok(ResumeState {
        path,
        metadata_path,
        offset,
        file: None,
        buffer: Vec::new(),
        pending: None,
        last_checkpoint: Instant::now(),
        last_checkpoint_offset: offset,
    })
}

fn checkpoint_offset(
    path: &Path,
    metadata_path: &Path,
    descriptor: &TransferDescriptor,
) -> anyhow::Result<u64> {
    let Some(metadata) = read_metadata(metadata_path)? else {
        return Ok(0);
    };
    if !metadata_matches(&metadata, descriptor)
        || metadata.received_bytes > descriptor.archive_bytes
    {
        return Ok(0);
    }
    match fs::metadata(path) {
        Ok(info) if info.len() >= metadata.received_bytes => Ok(metadata.received_bytes),
        Ok(_) => Ok(0),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(e.into()),
    }
}

fn lock_partial(dir: &Path) -> anyhow::Result<fs::File> {
    let path = dir.join("writer.lock");
    let file = fs::File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)?;
    file.try_lock()
        .map_err(|e| anyhow::anyhow!("partial archive writer unavailable: {e}"))?;
    // A finisher may have unlinked this lock between open and try_lock.
    anyhow::ensure!(
        same_file::Handle::from_path(path)? == same_file::Handle::from_file(file.try_clone()?)?,
        "partial archive was replaced"
    );
    Ok(file)
}

impl ResumeState {
    fn open_writer(&mut self, descriptor: &TransferDescriptor) -> anyhow::Result<()> {
        let dir = self.path.parent().expect("partial directory");
        fs::create_dir_all(dir)?;
        let lease = lock_partial(dir)?;
        anyhow::ensure!(
            checkpoint_offset(&self.path, &self.metadata_path, descriptor)? == self.offset,
            "partial archive changed during handshake"
        );
        let mut archive = fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.path)?;
        // A process may die between the data write and metadata replacement.
        if self.offset == 0 {
            // Retire an unusable checkpoint before reusing its file for new bytes.
            unlink_if_present(&self.metadata_path)?;
        }
        archive.set_len(self.offset)?;
        archive.seek(io::SeekFrom::Start(self.offset))?;
        self.file = Some(Arc::new(PartialFile { archive, lease }));
        Ok(())
    }

    async fn wait_for_write(&mut self) -> anyhow::Result<Vec<u8>> {
        match self.pending.take() {
            Some(task) => task.await?,
            None => Ok(Vec::new()),
        }
    }

    async fn write_batch(
        &mut self,
        descriptor: &TransferDescriptor,
        checkpoint: bool,
    ) -> anyhow::Result<()> {
        let spare = self.wait_for_write().await?;
        let mut buffer = std::mem::replace(&mut self.buffer, spare);
        let file = Arc::clone(self.file.as_ref().expect("open partial"));
        let metadata_path = self.metadata_path.clone();
        let metadata = PartialMetadata {
            source_id: descriptor.source_id,
            resource_id: descriptor.resource_id,
            archive_bytes: descriptor.archive_bytes,
            logical_bytes: descriptor.logical_bytes,
            files: descriptor.files,
            received_bytes: self.offset,
            updated_at_unix: now_unix()?,
        };
        self.pending = Some(tokio::task::spawn_blocking(move || {
            (&file.archive).write_all(&buffer)?;
            if checkpoint {
                file.archive.sync_data()?;
                write_metadata(&metadata_path, &metadata)?;
            }
            buffer.clear();
            Ok(buffer)
        }));
        if checkpoint {
            self.last_checkpoint = Instant::now();
            self.last_checkpoint_offset = self.offset;
        }
        Ok(())
    }
}

pub(crate) async fn append_chunk(
    state: &mut ResumeState,
    descriptor: &TransferDescriptor,
    mut bytes: &[u8],
) -> anyhow::Result<u64> {
    anyhow::ensure!(
        bytes.len() as u64 <= descriptor.archive_bytes - state.offset,
        "received more archive bytes than expected"
    );
    let mut first = state.file.is_none();
    if first {
        state.open_writer(descriptor)?;
    }
    while !bytes.is_empty() {
        let n = bytes.len().min(BATCH_BYTES - state.buffer.len());
        if state.buffer.capacity() < state.buffer.len() + n {
            let capacity = (state.buffer.len() + n)
                .next_power_of_two()
                .min(BATCH_BYTES);
            state.buffer.reserve_exact(capacity - state.buffer.len());
        }
        state.buffer.extend_from_slice(&bytes[..n]);
        bytes = &bytes[n..];
        state.offset += n as u64;
        let checkpoint = first
            || state.offset == descriptor.archive_bytes
            || state.offset - state.last_checkpoint_offset >= CHECKPOINT_BYTES
            || state.last_checkpoint.elapsed() >= CHECKPOINT_INTERVAL;
        if checkpoint || state.buffer.len() == BATCH_BYTES {
            state.write_batch(descriptor, checkpoint).await?;
            if first {
                // Give even a short transfer a restart point before reporting progress.
                state.buffer = state.wait_for_write().await?;
                first = false;
            }
        }
    }
    Ok(state.offset)
}

pub(crate) async fn complete_partial(
    state: &mut ResumeState,
    descriptor: &TransferDescriptor,
) -> anyhow::Result<fs::File> {
    anyhow::ensure!(
        state.offset == descriptor.archive_bytes,
        "partial archive size mismatch"
    );
    if state.file.is_none() {
        state.open_writer(descriptor)?;
    }
    if !state.buffer.is_empty() {
        state.write_batch(descriptor, true).await?;
    }
    state.wait_for_write().await?;
    let mut file = fs::File::open(&state.path)?;
    anyhow::ensure!(
        file.metadata()?.len() == descriptor.archive_bytes,
        "partial archive size mismatch"
    );
    if resource_id_for_reader(&mut file)? != descriptor.resource_id {
        drop(file);
        remove_partial(state)?;
        anyhow::bail!("partial archive hash mismatch");
    }
    file.rewind()?;
    Ok(file)
}

pub(crate) fn remove_partial(state: &mut ResumeState) -> anyhow::Result<()> {
    // All callers own the writer lease and have joined the last checkpoint.
    let file =
        Arc::into_inner(state.file.take().expect("open partial")).expect("checkpoint finished");
    drop(file.archive);
    remove_partial_paths(&state.path, &state.metadata_path, file.lease)
}

fn cleanup_expired(destination: &Path) -> anyhow::Result<()> {
    let root = resume_root(destination);
    let Ok(sources) = fs::read_dir(&root) else {
        return Ok(());
    };
    let cutoff = now_unix()?.saturating_sub(PARTIAL_RETENTION.as_secs());
    for source in sources.flatten() {
        if !source.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        let Ok(resources) = fs::read_dir(source.path()) else {
            continue;
        };
        for resource in resources.flatten() {
            if !resource.file_type().is_ok_and(|kind| kind.is_dir()) {
                continue;
            }
            let dir = resource.path();
            let metadata_path = dir.join("metadata.json");
            let stale = || -> anyhow::Result<bool> {
                Ok(read_metadata(&metadata_path)?.is_none_or(|m| m.updated_at_unix < cutoff))
            };
            if stale().unwrap_or(false)
                && let Ok(lease) = lock_partial(&dir)
                && stale().unwrap_or(false)
            {
                let _ = remove_partial_paths(&dir.join("archive.part"), &metadata_path, lease);
            }
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

fn read_metadata(path: &Path) -> anyhow::Result<Option<PartialMetadata>> {
    match fs::read(path) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn write_metadata(path: &Path, metadata: &PartialMetadata) -> anyhow::Result<()> {
    let temp_path = path.with_extension("json.tmp");
    let mut file = fs::File::create(&temp_path)?;
    serde_json::to_writer(&mut file, metadata)?;
    file.sync_all()?;
    drop(file);
    fs::rename(temp_path, path)?;
    #[cfg(unix)]
    fs::File::open(path.parent().expect("partial directory"))?.sync_all()?;
    Ok(())
}

fn unlink_if_present(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

fn remove_partial_paths(
    part_path: &Path,
    metadata_path: &Path,
    lease: fs::File,
) -> anyhow::Result<()> {
    let dir = part_path.parent().expect("partial directory");
    for path in [
        part_path,
        metadata_path,
        &metadata_path.with_extension("json.tmp"),
        &dir.join("writer.lock"),
    ] {
        unlink_if_present(path)?;
    }
    drop(lease);
    for parent in dir.ancestors().take(3) {
        let _ = fs::remove_dir(parent);
    }
    Ok(())
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

    #[tokio::test]
    async fn resume_state_reuses_existing_partial_for_same_source_and_resource() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("source.txt");
        fs::write(&src, "hello peerline").unwrap();
        let archive =
            create_archive(std::slice::from_ref(&src), peerline_core::Compression::None).unwrap();
        let descriptor = descriptor_for_archive(&archive, NodeId::random());

        let mut first = resume_state(temp.path(), &descriptor).unwrap();
        assert_eq!(first.offset, 0);
        append_chunk(&mut first, &descriptor, b"abc").await.unwrap();

        let second = resume_state(temp.path(), &descriptor).unwrap();
        assert_eq!(second.offset, 3);
        assert_eq!(second.path, first.path);
        assert_eq!(second.metadata_path, first.metadata_path);
    }

    #[tokio::test]
    async fn resume_state_keeps_partials_separate_by_source_or_resource() {
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
        append_chunk(&mut state_a, &descriptor_a, b"abc")
            .await
            .unwrap();

        let state_b = resume_state(temp.path(), &descriptor_b).unwrap();
        assert_eq!(state_b.offset, 0);
        assert_ne!(state_a.path, state_b.path);

        let mut altered = descriptor_for_archive(&archive, source_a);
        altered.resource_id = peerline_core::ResourceId::from_bytes([9u8; 32]);
        let state_c = resume_state(temp.path(), &altered).unwrap();
        assert_eq!(state_c.offset, 0);
        assert_ne!(state_a.path, state_c.path);
    }

    #[tokio::test]
    async fn retry_discards_uncommitted_tail_and_preserves_checkpoint() {
        use std::io::Read;
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("payload");
        fs::write(&src, vec![9; BATCH_BYTES * 3]).unwrap();
        let archive = create_archive(&[src], peerline_core::Compression::None).unwrap();
        let descriptor = descriptor_for_archive(&archive, NodeId::random());
        let mut bytes = Vec::new();
        archive.reader().unwrap().read_to_end(&mut bytes).unwrap();
        let mut state = resume_state(temp.path(), &descriptor).unwrap();
        append_chunk(&mut state, &descriptor, &bytes[..64])
            .await
            .unwrap();
        append_chunk(
            &mut state,
            &descriptor,
            &bytes[64..(64 + BATCH_BYTES + 512)],
        )
        .await
        .unwrap();
        // A competing route sees only the published prefix and cannot change it.
        let mut other = resume_state(temp.path(), &descriptor).unwrap();
        assert_eq!(other.offset, 64);
        assert!(append_chunk(&mut other, &descriptor, b"x").await.is_err());
        state.wait_for_write().await.unwrap();
        assert_eq!(
            fs::metadata(&state.path).unwrap().len(),
            64 + BATCH_BYTES as u64
        );
        drop(state);
        // Simulate the other crash window: disk write completed before metadata rename.
        fs::File::options()
            .append(true)
            .open(&other.path)
            .unwrap()
            .write_all(b"uncommitted")
            .unwrap();
        let mut retry = resume_state(temp.path(), &descriptor).unwrap();
        assert_eq!(retry.offset, 64);
        assert!(append_chunk(&mut retry, &descriptor, &bytes).await.is_err());
        assert_eq!(fs::read(&retry.path).unwrap().len(), 64 + BATCH_BYTES + 11);
        append_chunk(&mut retry, &descriptor, &bytes[64..])
            .await
            .unwrap();
        let mut restored = Vec::new();
        complete_partial(&mut retry, &descriptor)
            .await
            .unwrap()
            .read_to_end(&mut restored)
            .unwrap();
        assert_eq!(restored, bytes);
    }

    #[tokio::test]
    async fn failed_final_checkpoint_keeps_last_published_offset() {
        use std::io::Read;
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("data");
        fs::write(&src, "slow transfer").unwrap();
        let archive = create_archive(&[src], peerline_core::Compression::None).unwrap();
        let descriptor = descriptor_for_archive(&archive, NodeId::random());
        let mut bytes = Vec::new();
        archive.reader().unwrap().read_to_end(&mut bytes).unwrap();

        let mut state = resume_state(temp.path(), &descriptor).unwrap();
        append_chunk(&mut state, &descriptor, &bytes[..3])
            .await
            .unwrap();
        state.last_checkpoint = Instant::now() - CHECKPOINT_INTERVAL;
        append_chunk(&mut state, &descriptor, &bytes[3..7])
            .await
            .unwrap();
        state.wait_for_write().await.unwrap();
        let temp_metadata = state.metadata_path.with_extension("json.tmp");
        fs::create_dir(&temp_metadata).unwrap();
        append_chunk(&mut state, &descriptor, &bytes[7..])
            .await
            .unwrap();
        assert!(complete_partial(&mut state, &descriptor).await.is_err());
        drop(state);
        fs::remove_dir(temp_metadata).unwrap();

        let mut retry = resume_state(temp.path(), &descriptor).unwrap();
        assert_eq!(retry.offset, 7);
        append_chunk(&mut retry, &descriptor, &bytes[7..])
            .await
            .unwrap();
        complete_partial(&mut retry, &descriptor).await.unwrap();
    }

    #[tokio::test]
    async fn short_partial_cannot_advertise_missing_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("source.txt");
        fs::write(&src, "hello").unwrap();
        let archive = create_archive(&[src], peerline_core::Compression::None).unwrap();
        let descriptor = descriptor_for_archive(&archive, NodeId::random());
        let mut state = resume_state(temp.path(), &descriptor).unwrap();
        append_chunk(&mut state, &descriptor, b"prefix")
            .await
            .unwrap();
        let path = state.path.clone();
        drop(state);
        fs::write(path, b"p").unwrap();
        assert_eq!(resume_state(temp.path(), &descriptor).unwrap().offset, 0);
    }

    #[tokio::test]
    async fn expired_partials_are_cleaned_before_resume() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("source.txt");
        fs::write(&src, "hello peerline").unwrap();
        let archive =
            create_archive(std::slice::from_ref(&src), peerline_core::Compression::None).unwrap();
        let descriptor = descriptor_for_archive(&archive, NodeId::random());

        let mut state = resume_state(temp.path(), &descriptor).unwrap();
        append_chunk(&mut state, &descriptor, b"abcd")
            .await
            .unwrap();
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

        drop(state);
        let refreshed = resume_state(temp.path(), &descriptor).unwrap();
        assert_eq!(refreshed.offset, 0);
        assert!(!refreshed.path.exists());
        assert!(!refreshed.metadata_path.exists());
    }

    #[tokio::test]
    async fn corrupt_partial_is_deleted_when_completion_fails() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("source.txt");
        fs::write(&src, "hello peerline").unwrap();
        let archive =
            create_archive(std::slice::from_ref(&src), peerline_core::Compression::None).unwrap();
        let descriptor = descriptor_for_archive(&archive, NodeId::random());

        let mut state = resume_state(temp.path(), &descriptor).unwrap();
        let wrong = vec![0; descriptor.archive_bytes as usize];
        append_chunk(&mut state, &descriptor, &wrong).await.unwrap();

        assert!(complete_partial(&mut state, &descriptor).await.is_err());
        assert!(!state.path.exists());
        assert!(!state.metadata_path.exists());
    }
}
