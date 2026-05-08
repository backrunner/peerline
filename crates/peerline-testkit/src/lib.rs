use peerline_core::{HumanCode, HumanName};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

pub struct PeerlineFixture {
    temp: TempDir,
}

impl PeerlineFixture {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            temp: tempfile::tempdir()?,
        })
    }

    pub fn root(&self) -> &Path {
        self.temp.path()
    }

    pub fn write_file(&self, relative: &str, contents: &str) -> anyhow::Result<PathBuf> {
        let path = self.root().join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, contents)?;
        Ok(path)
    }

    pub fn identity(&self) -> (HumanName, HumanCode) {
        (
            HumanName::parse("river-mango-42").unwrap(),
            HumanCode::parse("rose-lime-iris-jade-1234").unwrap(),
        )
    }
}
