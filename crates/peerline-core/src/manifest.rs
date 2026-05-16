use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferId([u8; 16]);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId([u8; 16]);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceId([u8; 32]);

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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferDescriptor {
    pub source_id: NodeId,
    pub resource_id: ResourceId,
    pub archive_bytes: u64,
    pub logical_bytes: u64,
    pub files: usize,
    pub compression: Compression,
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

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub fn bytes(&self) -> [u8; 16] {
        self.0
    }
}

impl NodeId {
    pub fn random() -> Self {
        let mut bytes = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut bytes);
        Self(bytes)
    }

    pub fn bytes(&self) -> [u8; 16] {
        self.0
    }

    pub fn hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl ResourceId {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn bytes(&self) -> [u8; 32] {
        self.0
    }

    pub fn hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.hex())
    }
}

impl fmt::Display for ResourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.hex())
    }
}

impl FromStr for NodeId {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(value)?;
        let bytes: [u8; 16] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("node id must be 16 bytes"))?;
        Ok(Self(bytes))
    }
}

impl FromStr for ResourceId {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(value)?;
        let bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("resource id must be 32 bytes"))?;
        Ok(Self(bytes))
    }
}

impl Manifest {
    pub fn new(compression: Compression, entries: Vec<ManifestEntry>) -> Self {
        Self::with_id(TransferId::random(), compression, entries)
    }

    pub fn with_id(id: TransferId, compression: Compression, entries: Vec<ManifestEntry>) -> Self {
        let total_bytes = entries
            .iter()
            .filter(|entry| entry.kind == EntryKind::File)
            .map(|entry| entry.size)
            .sum();
        Self {
            id,
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
