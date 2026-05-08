use argon2::Argon2;
use rand::{Rng, seq::SliceRandom};
use serde::{Deserialize, Serialize};
use std::{fmt, net::SocketAddr, str::FromStr};
use thiserror::Error;

pub const DEFAULT_DIRECT_PORT: u16 = 43_117;

const WORDS: &[&str] = &[
    "amber", "anchor", "apple", "april", "ash", "atlas", "aurora", "autumn", "basil", "beacon",
    "berry", "birch", "bloom", "brave", "brook", "cable", "cactus", "canvas", "cedar", "charm",
    "cherry", "cinder", "civic", "cloud", "clover", "cobalt", "comet", "copper", "coral", "cosmic",
    "crane", "crisp", "delta", "dawn", "denim", "dove", "dream", "ember", "falcon", "fern",
    "field", "fig", "flame", "forest", "frost", "garden", "ginger", "glade", "glass", "gold",
    "grape", "harbor", "hazel", "honey", "iris", "ivory", "jade", "jasmine", "jolly", "juniper",
    "kiwi", "lagoon", "lantern", "leaf", "lemon", "lilac", "linen", "lotus", "lunar", "maple",
    "marble", "meadow", "mint", "moss", "nectar", "nova", "olive", "onyx", "opal", "orbit",
    "orchid", "peach", "pearl", "pepper", "pine", "plum", "polar", "poppy", "prairie", "quartz",
    "quiet", "rain", "raven", "river", "robin", "rose", "ruby", "saffron", "sage", "satin",
    "shell", "silver", "sky", "snow", "solar", "spruce", "stone", "summer", "sunny", "swift",
    "teal", "thistle", "tiger", "topaz", "tulip", "umber", "velvet", "violet", "walnut", "willow",
    "winter", "wool", "yellow", "zebra", "zenith",
];

#[derive(Debug, Error, PartialEq, Eq)]
pub enum IdentityError {
    #[error("value cannot be empty")]
    Empty,
    #[error("value is too long")]
    TooLong,
    #[error("only lowercase letters, digits, and hyphens are allowed")]
    InvalidCharacter,
    #[error("value must not start or end with a hyphen")]
    BadHyphen,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HumanName(String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HumanCode(String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NameCode {
    pub name: HumanName,
    pub code: HumanCode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LookupKey([u8; 32]);

impl HumanName {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, IdentityError> {
        normalize_token(value.as_ref(), 64).map(Self)
    }

    pub fn generate() -> Self {
        let mut rng = rand::thread_rng();
        let first = WORDS.choose(&mut rng).expect("word list is non-empty");
        let second = WORDS.choose(&mut rng).expect("word list is non-empty");
        let number: u16 = rng.gen_range(10..=99);
        Self(format!("{first}-{second}-{number}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl HumanCode {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, IdentityError> {
        normalize_token(value.as_ref(), 96).map(Self)
    }

    pub fn generate() -> Self {
        let mut rng = rand::thread_rng();
        let parts = (0..4)
            .map(|_| *WORDS.choose(&mut rng).expect("word list is non-empty"))
            .collect::<Vec<_>>();
        let number: u16 = rng.gen_range(1000..=9999);
        Self(format!(
            "{}-{}-{}-{}-{number}",
            parts[0], parts[1], parts[2], parts[3]
        ))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn entropy_bits_estimate(&self) -> f64 {
        code_entropy_bits(self.as_str())
    }

    pub fn is_low_entropy(&self) -> bool {
        self.entropy_bits_estimate() < 40.0
    }
}

impl NameCode {
    pub fn new(name: HumanName, code: HumanCode) -> Self {
        Self { name, code }
    }

    pub fn lookup_key(&self) -> LookupKey {
        lookup_key(&self.name, &self.code)
    }
}

impl LookupKey {
    pub fn bytes(&self) -> [u8; 32] {
        self.0
    }

    pub fn hex(&self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

impl fmt::Display for HumanName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for HumanCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for HumanName {
    type Err = IdentityError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl FromStr for HumanCode {
    type Err = IdentityError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

pub fn parse_ip_endpoint(value: &str) -> Option<SocketAddr> {
    if let Ok(addr) = value.parse::<SocketAddr>() {
        return Some(addr);
    }

    if let Ok(ip) = value.parse::<std::net::IpAddr>() {
        return Some(SocketAddr::new(ip, DEFAULT_DIRECT_PORT));
    }

    None
}

pub fn code_entropy_bits(value: &str) -> f64 {
    let parts = value
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    let word_bits = parts
        .iter()
        .filter(|part| part.chars().all(|ch| ch.is_ascii_lowercase()))
        .count() as f64
        * (WORDS.len() as f64).log2();
    let digit_bits = parts
        .iter()
        .filter(|part| part.chars().all(|ch| ch.is_ascii_digit()))
        .map(|part| 10f64.powi(part.len() as i32).log2())
        .sum::<f64>();
    word_bits + digit_bits
}

fn normalize_token(value: &str, max_len: usize) -> Result<String, IdentityError> {
    let trimmed = value.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return Err(IdentityError::Empty);
    }
    if trimmed.len() > max_len {
        return Err(IdentityError::TooLong);
    }
    if trimmed.starts_with('-') || trimmed.ends_with('-') || trimmed.contains("--") {
        return Err(IdentityError::BadHyphen);
    }
    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
    {
        return Err(IdentityError::InvalidCharacter);
    }
    Ok(trimmed)
}

fn lookup_key(name: &HumanName, code: &HumanCode) -> LookupKey {
    let mut salt_hasher = blake3::Hasher::new();
    salt_hasher.update(b"peerline:dht:salt:v1");
    salt_hasher.update(name.as_str().as_bytes());
    let salt = salt_hasher.finalize();
    let salt = &salt.as_bytes()[..16];
    let argon = Argon2::default();
    let mut stretched = [0u8; 32];
    argon
        .hash_password_into(code.as_str().as_bytes(), salt, &mut stretched)
        .expect("argon2 hashing should not fail for bounded input");
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"peerline:dht:v1");
    hasher.update(name.as_str().as_bytes());
    hasher.update(&stretched);
    LookupKey(*hasher.finalize().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_name_and_code() {
        assert_eq!(
            HumanName::parse(" River-Mango-42 ").unwrap().as_str(),
            "river-mango-42"
        );
        assert!(HumanCode::parse("river mango").is_err());
        assert!(HumanCode::parse("-river").is_err());
    }

    #[test]
    fn generated_code_meets_entropy_target() {
        let code = HumanCode::generate();
        assert!(code.entropy_bits_estimate() >= 40.0, "{code}");
    }

    #[test]
    fn parses_default_ip_port() {
        let endpoint = parse_ip_endpoint("127.0.0.1").unwrap();
        assert_eq!(endpoint.port(), DEFAULT_DIRECT_PORT);
    }

    #[test]
    fn parses_ipv6_default_ip_port() {
        let endpoint = parse_ip_endpoint("::1").unwrap();
        assert_eq!(endpoint.port(), DEFAULT_DIRECT_PORT);
        assert!(endpoint.is_ipv6());
    }
}
