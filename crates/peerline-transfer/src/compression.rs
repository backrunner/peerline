use peerline_core::Compression;
use std::io::Cursor;

pub fn encode_payload(compression: Compression, input: &[u8]) -> anyhow::Result<Vec<u8>> {
    match effective_compression(compression, input) {
        Compression::None => Ok(input.to_vec()),
        Compression::Zstd | Compression::Auto => {
            Ok(zstd::stream::encode_all(Cursor::new(input), 3)?)
        }
        Compression::Lzma => {
            let mut output = Vec::new();
            lzma_rs::lzma_compress(&mut Cursor::new(input), &mut output)?;
            Ok(output)
        }
    }
}

pub fn decode_payload(compression: Compression, input: &[u8]) -> anyhow::Result<Vec<u8>> {
    match compression {
        Compression::None => Ok(input.to_vec()),
        Compression::Zstd | Compression::Auto => Ok(zstd::stream::decode_all(Cursor::new(input))?),
        Compression::Lzma => {
            let mut output = Vec::new();
            lzma_rs::lzma_decompress(&mut Cursor::new(input), &mut output)?;
            Ok(output)
        }
    }
}

fn effective_compression(compression: Compression, input: &[u8]) -> Compression {
    match compression {
        Compression::Auto if input.len() < 1024 => Compression::None,
        Compression::Auto => Compression::Zstd,
        other => other,
    }
}

pub fn resolved_compression(compression: Compression, input: &[u8]) -> Compression {
    effective_compression(compression, input)
}

pub fn resolved_compression_for_size(compression: Compression, size: u64) -> Compression {
    match compression {
        Compression::Auto if size < 1024 => Compression::None,
        Compression::Auto => Compression::Zstd,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zstd_roundtrip() {
        let input = b"hello hello hello hello hello";
        let encoded = encode_payload(Compression::Zstd, input).unwrap();
        let decoded = decode_payload(Compression::Zstd, &encoded).unwrap();
        assert_eq!(decoded, input);
    }

    #[test]
    fn lzma_roundtrip() {
        let input = b"hello hello hello hello hello";
        let encoded = encode_payload(Compression::Lzma, input).unwrap();
        let decoded = decode_payload(Compression::Lzma, &encoded).unwrap();
        assert_eq!(decoded, input);
    }
}
