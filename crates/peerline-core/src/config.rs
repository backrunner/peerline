use crate::identity::HumanName;
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    pub name: Option<HumanName>,
}

#[derive(Clone, Debug)]
pub struct ConfigStore {
    path: PathBuf,
}

impl ConfigStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn default_path() -> anyhow::Result<PathBuf> {
        let dirs = ProjectDirs::from("dev", "peerline", "peerline")
            .ok_or_else(|| anyhow::anyhow!("cannot resolve user config directory"))?;
        Ok(dirs.config_dir().join("config.toml"))
    }

    pub fn user_default() -> anyhow::Result<Self> {
        Ok(Self::new(Self::default_path()?))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> anyhow::Result<Config> {
        if !self.path.exists() {
            return Ok(Config::default());
        }
        let raw = fs::read_to_string(&self.path)?;
        Ok(toml::from_str(&raw)?)
    }

    pub fn save(&self, config: &Config) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&self.path, toml::to_string_pretty(config)?)?;
        Ok(())
    }

    pub fn set_name(&self, name: HumanName) -> anyhow::Result<()> {
        let mut config = self.load()?;
        config.name = Some(name);
        self.save(&config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saves_and_loads_name() {
        let temp = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(temp.path().join("config.toml"));
        store
            .set_name(HumanName::parse("river-mango-42").unwrap())
            .unwrap();
        let config = store.load().unwrap();
        assert_eq!(config.name.unwrap().as_str(), "river-mango-42");
    }
}
