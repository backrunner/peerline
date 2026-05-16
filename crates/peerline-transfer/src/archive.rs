use crate::{
    compression::resolved_compression_for_size,
    scan::{SourceEntry, scan_sources},
};
use anyhow::Context;
use peerline_core::{
    Compression, Manifest, ManifestEntry, ResourceId, TransferId,
    manifest::EntryKind,
    path::{non_overwriting_path, safe_join_relative},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::{
    fs,
    io::{self, BufReader, Cursor, Read, Write},
    path::{Path, PathBuf},
};
use tempfile::NamedTempFile;
use zstd::stream::write::Encoder as ZstdEncoder;

const ARCHIVE_FILE_CHUNK_SIZE: usize = 64 * 1024;
const ZSTD_COMPRESSION_LEVEL: i32 = 3;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArchiveFrame {
    Manifest(Manifest),
    Directory {
        path: PathBuf,
    },
    FileStart {
        path: PathBuf,
        size: u64,
        blake3: [u8; 32],
    },
    FileChunk {
        bytes: Vec<u8>,
    },
    FileEnd,
}

#[derive(Debug)]
pub struct Archive {
    pub manifest: Manifest,
    pub compression: Compression,
    pub resource_id: ResourceId,
    file: NamedTempFile,
    len: u64,
}

impl Archive {
    pub fn reader(&self) -> anyhow::Result<fs::File> {
        Ok(self.file.reopen()?)
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn from_existing(
        manifest: Manifest,
        compression: Compression,
        resource_id: ResourceId,
        mut reader: fs::File,
        len: u64,
    ) -> anyhow::Result<Self> {
        let mut file = NamedTempFile::new()?;
        io::copy(&mut reader, file.as_file_mut())?;
        file.as_file_mut().flush()?;
        Ok(Self {
            manifest,
            compression,
            resource_id,
            file,
            len,
        })
    }
}

pub fn create_archive(paths: &[PathBuf], compression: Compression) -> anyhow::Result<Archive> {
    let sources = scan_sources(paths)?;
    let manifest_entries = sources
        .iter()
        .map(|source| source.manifest_entry.clone())
        .collect::<Vec<_>>();
    let mut manifest = Manifest::new(compression, manifest_entries);
    let body = create_archive_body(&sources)?;
    let raw_len = archive_frame_len(&ArchiveFrame::Manifest(manifest.clone()))?
        + body.as_file().metadata()?.len() as usize;
    let actual_compression = resolved_compression_for_size(compression, raw_len as u64);
    manifest.compression = actual_compression;
    manifest.id = deterministic_manifest_id(actual_compression, &manifest.entries);

    let raw = create_raw_archive(&manifest, &body)?;
    let file = compress_raw_archive(actual_compression, raw)?;
    let len = file.as_file().metadata()?.len();
    let resource_id = resource_id_for_file(&file)?;

    Ok(Archive {
        manifest,
        compression: actual_compression,
        resource_id,
        file,
        len,
    })
}

pub fn resource_id_for_reader(mut reader: impl Read) -> anyhow::Result<ResourceId> {
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(ResourceId::from_bytes(*hasher.finalize().as_bytes()))
}

fn resource_id_for_file(file: &NamedTempFile) -> anyhow::Result<ResourceId> {
    resource_id_for_reader(file.reopen()?)
}

fn deterministic_manifest_id(compression: Compression, entries: &[ManifestEntry]) -> TransferId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"peerline:manifest-id:v1");
    hasher.update(&[compression_tag(compression)]);
    for entry in entries {
        hasher.update(&[entry_kind_tag(entry.kind)]);
        let path = entry.path.to_string_lossy();
        hasher.update(&(path.len() as u64).to_be_bytes());
        hasher.update(path.as_bytes());
        hasher.update(&entry.size.to_be_bytes());
        if let Some(hash) = entry.blake3 {
            hasher.update(&[1]);
            hasher.update(&hash);
        } else {
            hasher.update(&[0]);
        }
    }
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash.as_bytes()[..16]);
    TransferId::from_bytes(id)
}

fn compression_tag(compression: Compression) -> u8 {
    match compression {
        Compression::Auto => 0,
        Compression::None => 1,
        Compression::Zstd => 2,
        Compression::Lzma => 3,
    }
}

fn entry_kind_tag(kind: EntryKind) -> u8 {
    match kind {
        EntryKind::File => 1,
        EntryKind::Directory => 2,
    }
}

fn create_archive_body(sources: &[SourceEntry]) -> anyhow::Result<NamedTempFile> {
    let mut raw = NamedTempFile::new()?;
    for source in sources {
        match source.manifest_entry.kind {
            EntryKind::Directory => {
                write_frame(
                    raw.as_file_mut(),
                    &ArchiveFrame::Directory {
                        path: source.relative_path.clone(),
                    },
                )?;
            }
            EntryKind::File => {
                let mut file = fs::File::open(&source.source_path)?;
                let size = source.manifest_entry.size;
                let hash = source
                    .manifest_entry
                    .blake3
                    .ok_or_else(|| anyhow::anyhow!("file entry missing hash"))?;
                write_frame(
                    raw.as_file_mut(),
                    &ArchiveFrame::FileStart {
                        path: source.relative_path.clone(),
                        size,
                        blake3: hash,
                    },
                )?;
                let mut buffer = [0u8; ARCHIVE_FILE_CHUNK_SIZE];
                loop {
                    let read = file.read(&mut buffer)?;
                    if read == 0 {
                        break;
                    }
                    write_frame(
                        raw.as_file_mut(),
                        &ArchiveFrame::FileChunk {
                            bytes: buffer[..read].to_vec(),
                        },
                    )?;
                }
                write_frame(raw.as_file_mut(), &ArchiveFrame::FileEnd)?;
            }
        }
    }
    raw.as_file_mut().flush()?;
    Ok(raw)
}

fn create_raw_archive(manifest: &Manifest, body: &NamedTempFile) -> anyhow::Result<NamedTempFile> {
    let mut raw = NamedTempFile::new()?;
    write_frame(raw.as_file_mut(), &ArchiveFrame::Manifest(manifest.clone()))?;
    let mut reader = BufReader::new(body.reopen()?);
    io::copy(&mut reader, raw.as_file_mut())?;
    raw.as_file_mut().flush()?;
    Ok(raw)
}

fn compress_raw_archive(
    compression: Compression,
    raw: NamedTempFile,
) -> anyhow::Result<NamedTempFile> {
    match compression {
        Compression::None => Ok(raw),
        Compression::Zstd | Compression::Auto => {
            let mut output = NamedTempFile::new()?;
            {
                let input = raw.reopen()?;
                let mut reader = BufReader::new(input);
                let writer = output.as_file_mut();
                let mut encoder = ZstdEncoder::new(writer, ZSTD_COMPRESSION_LEVEL)?;
                io::copy(&mut reader, &mut encoder)?;
                let writer = encoder.finish()?;
                writer.flush()?;
            }
            Ok(output)
        }
        Compression::Lzma => {
            let mut output = NamedTempFile::new()?;
            {
                let input = raw.reopen()?;
                let mut reader = BufReader::new(input);
                lzma_rs::lzma_compress(&mut reader, output.as_file_mut())?;
                output.as_file_mut().flush()?;
            }
            Ok(output)
        }
    }
}

pub fn unpack_archive(
    destination: &Path,
    compression: Compression,
    bytes: &[u8],
    overwrite: bool,
) -> anyhow::Result<Manifest> {
    unpack_archive_from_reader(destination, compression, Cursor::new(bytes), overwrite)
}

pub fn unpack_archive_from_reader<R: Read>(
    destination: &Path,
    compression: Compression,
    reader: R,
    overwrite: bool,
) -> anyhow::Result<Manifest> {
    match compression {
        Compression::None => unpack_raw_archive(destination, reader, overwrite),
        Compression::Zstd | Compression::Auto => {
            let decoder = zstd::stream::read::Decoder::new(reader)?;
            unpack_raw_archive(destination, decoder, overwrite)
        }
        Compression::Lzma => {
            fs::create_dir_all(destination)?;
            let mut decoded = NamedTempFile::new_in(destination)?;
            {
                let mut input = io::BufReader::new(reader);
                lzma_rs::lzma_decompress(&mut input, decoded.as_file_mut())?;
                decoded.as_file_mut().flush()?;
            }
            unpack_raw_archive(destination, decoded.reopen()?, overwrite)
        }
    }
}

fn unpack_raw_archive<R: Read>(
    destination: &Path,
    mut reader: R,
    overwrite: bool,
) -> anyhow::Result<Manifest> {
    let first = read_frame(&mut reader)?
        .ok_or_else(|| anyhow::anyhow!("archive does not contain a manifest"))?;
    let manifest = match first {
        ArchiveFrame::Manifest(manifest) => manifest,
        _ => anyhow::bail!("archive must start with a manifest"),
    };
    validate_manifest_entries(&manifest.entries)?;
    let mut remaining_entries = expected_manifest_entries(&manifest)?;

    let mut current_file: Option<ActiveFile> = None;
    while let Some(frame) = read_frame(&mut reader)? {
        match frame {
            ArchiveFrame::Manifest(_) => anyhow::bail!("duplicate manifest frame"),
            ArchiveFrame::Directory { path } => {
                let entry = take_expected_manifest_entry(
                    &mut remaining_entries,
                    &path,
                    EntryKind::Directory,
                )?;
                if entry.size != 0 || entry.blake3.is_some() {
                    anyhow::bail!(
                        "directory manifest entry carried file metadata for {}",
                        path.display()
                    );
                }
                let path = safe_join_relative(destination, &path)
                    .with_context(|| format!("invalid directory path {}", path.display()))?;
                fs::create_dir_all(path)?;
            }
            ArchiveFrame::FileStart { path, size, blake3 } => {
                if current_file.is_some() {
                    anyhow::bail!("nested file frame");
                }
                let entry = expected_manifest_entry(&remaining_entries, &path, EntryKind::File)?;
                if entry.size != size || entry.blake3 != Some(blake3) {
                    anyhow::bail!("file frame metadata mismatch for {}", path.display());
                }
                fs::create_dir_all(destination)?;
                current_file = Some(ActiveFile {
                    relative: path,
                    temp_file: NamedTempFile::new_in(destination)?,
                    expected_size: size,
                    expected_hash: blake3,
                    hasher: blake3::Hasher::new(),
                    bytes_written: 0,
                });
            }
            ArchiveFrame::FileChunk { bytes } => {
                let Some(active) = current_file.as_mut() else {
                    anyhow::bail!("file chunk without file start");
                };
                active.temp_file.as_file_mut().write_all(&bytes)?;
                active.hasher.update(&bytes);
                active.bytes_written += bytes.len() as u64;
                if active.bytes_written > active.expected_size {
                    anyhow::bail!(
                        "file larger than manifest size for {}",
                        active.relative.display()
                    );
                }
            }
            ArchiveFrame::FileEnd => {
                let Some(mut active) = current_file.take() else {
                    anyhow::bail!("file end without file start");
                };
                active.temp_file.as_file_mut().sync_all()?;
                if active.bytes_written != active.expected_size {
                    anyhow::bail!("file size mismatch for {}", active.relative.display());
                }
                if *active.hasher.finalize().as_bytes() != active.expected_hash {
                    anyhow::bail!("file hash mismatch for {}", active.relative.display());
                }
                let mut path = safe_join_relative(destination, &active.relative)
                    .with_context(|| format!("invalid file path {}", active.relative.display()))?;
                if !overwrite {
                    path = non_overwriting_path(&path);
                }
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                active.temp_file.persist(&path)?;
                if remaining_entries.remove(&active.relative).is_none() {
                    anyhow::bail!(
                        "file manifest entry already consumed or missing for {}",
                        active.relative.display()
                    );
                }
            }
        }
    }

    if current_file.is_some() {
        anyhow::bail!("archive ended inside a file");
    }

    if !remaining_entries.is_empty() {
        let missing = remaining_entries
            .keys()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        anyhow::bail!("archive is missing manifest entries: {missing}");
    }

    Ok(manifest)
}

fn archive_frame_len(frame: &ArchiveFrame) -> anyhow::Result<usize> {
    Ok(postcard::to_allocvec(frame)?.len())
}

struct ActiveFile {
    relative: PathBuf,
    temp_file: NamedTempFile,
    expected_size: u64,
    expected_hash: [u8; 32],
    hasher: blake3::Hasher,
    bytes_written: u64,
}

fn validate_manifest_entries(entries: &[ManifestEntry]) -> anyhow::Result<()> {
    for entry in entries {
        peerline_core::path::validate_relative_path(&entry.path)
            .with_context(|| format!("invalid manifest path {}", entry.path.display()))?;
        match entry.kind {
            EntryKind::File if entry.blake3.is_none() => {
                anyhow::bail!(
                    "file manifest entry missing hash for {}",
                    entry.path.display()
                )
            }
            EntryKind::Directory if entry.blake3.is_some() || entry.size != 0 => {
                anyhow::bail!(
                    "directory manifest entry must not carry file metadata for {}",
                    entry.path.display()
                )
            }
            _ => {}
        }
    }
    Ok(())
}

fn expected_manifest_entries(
    manifest: &Manifest,
) -> anyhow::Result<HashMap<PathBuf, ManifestEntry>> {
    let mut entries = HashMap::new();
    for entry in &manifest.entries {
        if entries.insert(entry.path.clone(), entry.clone()).is_some() {
            anyhow::bail!("duplicate manifest entry for {}", entry.path.display());
        }
    }
    Ok(entries)
}

fn take_expected_manifest_entry(
    entries: &mut HashMap<PathBuf, ManifestEntry>,
    path: &Path,
    kind: EntryKind,
) -> anyhow::Result<ManifestEntry> {
    let Some(entry) = entries.get(path) else {
        anyhow::bail!("unexpected archive entry for {}", path.display());
    };
    if entry.kind != kind {
        anyhow::bail!("manifest entry kind mismatch for {}", path.display());
    }
    Ok(entries
        .remove(path)
        .expect("entry must still exist after successful lookup"))
}

fn expected_manifest_entry<'a>(
    entries: &'a HashMap<PathBuf, ManifestEntry>,
    path: &Path,
    kind: EntryKind,
) -> anyhow::Result<&'a ManifestEntry> {
    let Some(entry) = entries.get(path) else {
        anyhow::bail!("unexpected archive entry for {}", path.display());
    };
    if entry.kind != kind {
        anyhow::bail!("manifest entry kind mismatch for {}", path.display());
    }
    Ok(entry)
}

pub fn write_frame(mut writer: impl Write, frame: &ArchiveFrame) -> anyhow::Result<()> {
    let payload = postcard::to_allocvec(frame)?;
    writer.write_all(&(payload.len() as u32).to_be_bytes())?;
    writer.write_all(&payload)?;
    Ok(())
}

pub fn read_frame(mut reader: impl Read) -> anyhow::Result<Option<ArchiveFrame>> {
    let mut len = [0u8; 4];
    match reader.read_exact(&mut len) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    }
    let len = u32::from_be_bytes(len) as usize;
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload)?;
    Ok(Some(postcard::from_bytes(&payload)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    struct ChunkedReader<R> {
        inner: R,
        max_chunk: usize,
    }

    impl<R: Read> ChunkedReader<R> {
        fn new(inner: R, max_chunk: usize) -> Self {
            Self { inner, max_chunk }
        }
    }

    impl<R: Read> Read for ChunkedReader<R> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let limit = buf.len().min(self.max_chunk.max(1));
            self.inner.read(&mut buf[..limit])
        }
    }

    #[test]
    fn archives_and_unpacks_folder() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("src");
        let dst = temp.path().join("dst");
        fs::create_dir(&src).unwrap();
        fs::write(src.join("a.txt"), "hello").unwrap();
        let archive = create_archive(std::slice::from_ref(&src), Compression::Zstd).unwrap();
        let manifest =
            unpack_archive_from_reader(&dst, archive.compression, archive.reader().unwrap(), false)
                .unwrap();
        assert_eq!(manifest.entries.len(), 2);
        assert_eq!(fs::read_to_string(dst.join("src/a.txt")).unwrap(), "hello");
    }

    #[test]
    fn does_not_overwrite_by_default() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("a.txt");
        let dst = temp.path().join("dst");
        fs::create_dir(&dst).unwrap();
        fs::write(&src, "new").unwrap();
        fs::write(dst.join("a.txt"), "old").unwrap();
        let archive = create_archive(&[src], Compression::None).unwrap();
        unpack_archive_from_reader(&dst, archive.compression, archive.reader().unwrap(), false)
            .unwrap();
        assert_eq!(fs::read_to_string(dst.join("a.txt")).unwrap(), "old");
        assert_eq!(fs::read_to_string(dst.join("a (1).txt")).unwrap(), "new");
    }

    #[test]
    fn auto_compression_manifest_matches_resolved_compression() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("tiny.txt");
        let dst = temp.path().join("dst");
        fs::create_dir(&dst).unwrap();
        fs::write(&src, "tiny").unwrap();

        let archive = create_archive(&[src], Compression::Auto).unwrap();
        assert_eq!(archive.compression, Compression::None);
        assert_eq!(archive.manifest.compression, Compression::None);

        let manifest =
            unpack_archive_from_reader(&dst, archive.compression, archive.reader().unwrap(), false)
                .unwrap();
        assert_eq!(manifest.compression, Compression::None);
    }

    #[test]
    fn current_directory_root_name_is_stable() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path().join("src");
        fs::create_dir(&cwd).unwrap();
        fs::write(cwd.join("a.txt"), "hello").unwrap();

        let archive = create_archive(&[cwd.join(".")], Compression::None).unwrap();
        assert!(
            archive
                .manifest
                .entries
                .iter()
                .any(|entry| entry.path == Path::new("src/a.txt"))
        );
    }

    #[test]
    fn large_files_are_split_into_archive_chunks() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("large.bin");
        fs::write(&src, vec![7u8; ARCHIVE_FILE_CHUNK_SIZE * 2 + 11]).unwrap();

        let archive = create_archive(&[src], Compression::None).unwrap();
        let mut cursor = archive.reader().unwrap();
        let mut chunk_sizes = Vec::new();
        while let Some(frame) = read_frame(&mut cursor).unwrap() {
            if let ArchiveFrame::FileChunk { bytes } = frame {
                chunk_sizes.push(bytes.len());
            }
        }

        assert_eq!(
            chunk_sizes,
            vec![ARCHIVE_FILE_CHUNK_SIZE, ARCHIVE_FILE_CHUNK_SIZE, 11]
        );
    }

    #[test]
    fn archive_resource_id_is_stable_for_same_inputs() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("stable.txt");
        fs::write(&src, "same content").unwrap();

        let first = create_archive(std::slice::from_ref(&src), Compression::None).unwrap();
        let second = create_archive(std::slice::from_ref(&src), Compression::None).unwrap();

        assert_eq!(first.manifest.id, second.manifest.id);
        assert_eq!(first.resource_id, second.resource_id);
    }

    #[test]
    fn archive_resource_id_changes_when_content_changes() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("stable.txt");
        fs::write(&src, "first").unwrap();
        let first = create_archive(std::slice::from_ref(&src), Compression::None).unwrap();

        fs::write(&src, "second").unwrap();
        let second = create_archive(std::slice::from_ref(&src), Compression::None).unwrap();

        assert_ne!(first.resource_id, second.resource_id);
    }

    #[test]
    fn archive_resource_id_changes_when_path_changes() {
        let temp = tempfile::tempdir().unwrap();
        let left = temp.path().join("left.txt");
        let right = temp.path().join("right.txt");
        fs::write(&left, "same content").unwrap();
        fs::write(&right, "same content").unwrap();

        let first = create_archive(std::slice::from_ref(&left), Compression::None).unwrap();
        let second = create_archive(std::slice::from_ref(&right), Compression::None).unwrap();

        assert_ne!(first.resource_id, second.resource_id);
    }

    #[test]
    fn archive_resource_id_changes_when_compression_changes() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("stable.txt");
        fs::write(&src, vec![5u8; ARCHIVE_FILE_CHUNK_SIZE + 32]).unwrap();

        let uncompressed = create_archive(std::slice::from_ref(&src), Compression::None).unwrap();
        let compressed = create_archive(std::slice::from_ref(&src), Compression::Zstd).unwrap();

        assert_ne!(uncompressed.resource_id, compressed.resource_id);
    }

    #[test]
    fn archive_reader_can_resume_from_offset() {
        use std::io::{Read, Seek};

        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("large.bin");
        fs::write(&src, vec![3u8; ARCHIVE_FILE_CHUNK_SIZE + 17]).unwrap();
        let archive = create_archive(std::slice::from_ref(&src), Compression::None).unwrap();
        let mut full = Vec::new();
        archive.reader().unwrap().read_to_end(&mut full).unwrap();

        let offset = 13usize;
        let mut reader = archive.reader().unwrap();
        reader
            .seek(std::io::SeekFrom::Start(offset as u64))
            .unwrap();
        let mut suffix = Vec::new();
        reader.read_to_end(&mut suffix).unwrap();

        assert_eq!(suffix, full[offset..]);
    }

    #[test]
    fn large_files_roundtrip_through_tempfile_unpack() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("large.bin");
        let dst = temp.path().join("dst");
        let payload = vec![9u8; ARCHIVE_FILE_CHUNK_SIZE * 2 + 11];
        fs::create_dir(&dst).unwrap();
        fs::write(&src, &payload).unwrap();

        let archive = create_archive(&[src], Compression::None).unwrap();
        unpack_archive_from_reader(&dst, archive.compression, archive.reader().unwrap(), false)
            .unwrap();

        assert_eq!(fs::read(dst.join("large.bin")).unwrap(), payload);
    }

    #[test]
    fn fragmented_reader_roundtrips_compressed_archive() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("nested");
        let dst = temp.path().join("dst");
        fs::create_dir(&src).unwrap();
        fs::write(
            src.join("hello.txt"),
            vec![5u8; ARCHIVE_FILE_CHUNK_SIZE + 37],
        )
        .unwrap();
        fs::create_dir(&dst).unwrap();

        let archive = create_archive(std::slice::from_ref(&src), Compression::Zstd).unwrap();
        let reader = ChunkedReader::new(archive.reader().unwrap(), 7);
        let manifest =
            unpack_archive_from_reader(&dst, archive.compression, reader, false).unwrap();

        assert_eq!(manifest.entries.len(), 2);
        assert_eq!(
            fs::read(dst.join("nested/hello.txt")).unwrap(),
            vec![5u8; ARCHIVE_FILE_CHUNK_SIZE + 37]
        );
    }

    #[test]
    fn rejects_missing_manifest_entries() {
        let temp = tempfile::tempdir().unwrap();
        let dst = temp.path().join("dst");
        fs::create_dir(&dst).unwrap();

        let manifest = Manifest::new(
            Compression::None,
            vec![ManifestEntry::file("file.txt", 4, [1u8; 32])],
        );
        let mut archive = NamedTempFile::new().unwrap();
        write_frame(archive.as_file_mut(), &ArchiveFrame::Manifest(manifest)).unwrap();
        archive.as_file_mut().flush().unwrap();

        assert!(
            unpack_archive_from_reader(&dst, Compression::None, archive.reopen().unwrap(), false)
                .is_err()
        );
    }

    #[test]
    fn rejects_duplicate_manifest_entries() {
        let temp = tempfile::tempdir().unwrap();
        let dst = temp.path().join("dst");
        fs::create_dir(&dst).unwrap();

        let mut manifest = Manifest::new(
            Compression::None,
            vec![ManifestEntry::file("file.txt", 4, [1u8; 32])],
        );
        let duplicate = manifest.entries[0].clone();
        manifest.entries.push(duplicate);
        let mut archive = NamedTempFile::new().unwrap();
        write_frame(archive.as_file_mut(), &ArchiveFrame::Manifest(manifest)).unwrap();
        archive.as_file_mut().flush().unwrap();

        assert!(
            unpack_archive_from_reader(&dst, Compression::None, archive.reopen().unwrap(), false)
                .is_err()
        );
    }

    #[test]
    fn rejects_extra_manifest_entries() {
        let temp = tempfile::tempdir().unwrap();
        let dst = temp.path().join("dst");
        fs::create_dir(&dst).unwrap();

        let manifest = Manifest::new(
            Compression::None,
            vec![ManifestEntry::file("file.txt", 4, [1u8; 32])],
        );
        let mut archive = NamedTempFile::new().unwrap();
        write_frame(archive.as_file_mut(), &ArchiveFrame::Manifest(manifest)).unwrap();
        write_frame(
            archive.as_file_mut(),
            &ArchiveFrame::Directory {
                path: PathBuf::from("extra"),
            },
        )
        .unwrap();
        archive.as_file_mut().flush().unwrap();

        assert!(
            unpack_archive_from_reader(&dst, Compression::None, archive.reopen().unwrap(), false)
                .is_err()
        );
    }

    #[test]
    fn rejects_mismatched_file_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let dst = temp.path().join("dst");
        fs::create_dir(&dst).unwrap();

        let manifest = Manifest::new(
            Compression::None,
            vec![ManifestEntry::file("file.txt", 4, [1u8; 32])],
        );
        let mut archive = NamedTempFile::new().unwrap();
        write_frame(archive.as_file_mut(), &ArchiveFrame::Manifest(manifest)).unwrap();
        write_frame(
            archive.as_file_mut(),
            &ArchiveFrame::FileStart {
                path: PathBuf::from("file.txt"),
                size: 5,
                blake3: [2u8; 32],
            },
        )
        .unwrap();
        write_frame(
            archive.as_file_mut(),
            &ArchiveFrame::FileChunk {
                bytes: vec![1, 2, 3, 4, 5],
            },
        )
        .unwrap();
        write_frame(archive.as_file_mut(), &ArchiveFrame::FileEnd).unwrap();
        archive.as_file_mut().flush().unwrap();

        assert!(
            unpack_archive_from_reader(&dst, Compression::None, archive.reopen().unwrap(), false)
                .is_err()
        );
    }
}
