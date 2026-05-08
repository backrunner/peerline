use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferId([u8; 16]);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Compression {
    #[default]
    Auto,
    None,
    Zstd,
    Lzma,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub id: TransferId,
    pub compression: Compression,
    pub entries: Vec<ManifestEntry>,
    pub total_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub path: PathBuf,
    pub kind: EntryKind,
    pub size: u64,
    pub blake3: Option<[u8; 32]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryKind {
    File,
    Directory,
}

impl TransferId {
    pub fn random() -> Self {
        let mut bytes = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut bytes);
        Self(bytes)
    }

    pub fn bytes(&self) -> [u8; 16] {
        self.0
    }
}

impl Manifest {
    pub fn new(compression: Compression, entries: Vec<ManifestEntry>) -> Self {
        let total_bytes = entries
            .iter()
            .filter(|entry| entry.kind == EntryKind::File)
            .map(|entry| entry.size)
            .sum();
        Self {
            id: TransferId::random(),
            compression,
            entries,
            total_bytes,
        }
    }
}

impl ManifestEntry {
    pub fn file(path: impl AsRef<Path>, size: u64, blake3: [u8; 32]) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            kind: EntryKind::File,
            size,
            blake3: Some(blake3),
        }
    }

    pub fn directory(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            kind: EntryKind::Directory,
            size: 0,
            blake3: None,
        }
    }
}
