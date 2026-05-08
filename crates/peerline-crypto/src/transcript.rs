use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transcript {
    parts: Vec<Vec<u8>>,
}

impl Transcript {
    pub fn new(label: impl AsRef<[u8]>) -> Self {
        Self {
            parts: vec![label.as_ref().to_vec()],
        }
    }

    pub fn append(mut self, label: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Self {
        self.parts.push(label.as_ref().to_vec());
        self.parts.push(value.as_ref().to_vec());
        self
    }

    pub fn hash(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        for part in &self.parts {
            hasher.update(&(part.len() as u64).to_le_bytes());
            hasher.update(part);
        }
        *hasher.finalize().as_bytes()
    }
}
