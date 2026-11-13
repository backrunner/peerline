//! Strict streaming CPIO extraction into a private directory. No archive-supplied
//! links, ownership, permissions, device nodes or executable bits are restored.
use anyhow::{Context, ensure};
use std::{
    collections::HashSet,
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};

pub(super) fn extract(
    source: std::fs::File,
    root: &Path,
    approved: &[String],
    limit: u64,
    cancelled: &AtomicBool,
) -> anyhow::Result<()> {
    let mut source = BufReader::new(source);
    let gzip = source.fill_buf()?.starts_with(&[0x1f, 0x8b]);
    if gzip {
        let mut decoder = flate2::bufread::GzDecoder::new(source);
        extract_stream(&mut decoder, root, approved, limit, cancelled)?;
        // libarchive may pad the *compressed* output to its block size. Accept
        // only zero padding, never an additional gzip member or hidden archive.
        let mut tail = decoder.into_inner().take(16 * 1024 * 1024);
        zero_tail(&mut tail, cancelled)?;
        ensure!(tail.limit() > 0, "Excessive compressed padding");
        Ok(())
    } else {
        extract_stream(source, root, approved, limit, cancelled)
    }
}

fn zero_tail(reader: &mut impl Read, cancelled: &AtomicBool) -> anyhow::Result<()> {
    let mut tail = [0; 4096];
    loop {
        ensure!(
            !cancelled.load(Ordering::Relaxed),
            "AirDrop transfer cancelled"
        );
        let n = reader.read(&mut tail)?;
        if n == 0 {
            return Ok(());
        }
        ensure!(tail[..n].iter().all(|b| *b == 0), "Trailing archive data");
    }
}

fn extract_stream(
    reader: impl Read,
    root: &Path,
    approved: &[String],
    limit: u64,
    cancelled: &AtomicBool,
) -> anyhow::Result<()> {
    let mut reader = reader.take(
        limit
            .checked_add(16 * 1024 * 1024)
            .context("archive limit overflow")?,
    );
    let mut total = 0u64;
    let mut entries = 0usize;
    let mut seen = HashSet::new();
    let mut roots = HashSet::new();
    loop {
        ensure!(
            !cancelled.load(Ordering::Relaxed),
            "AirDrop transfer cancelled"
        );
        let mut magic = [0; 6];
        reader.read_exact(&mut magic)?;
        let (mode, links, size, name_len, alignment, header_len) = match &magic {
            b"070707" => {
                let mut rest = [0; 70];
                reader.read_exact(&mut rest)?;
                (
                    number(&rest[12..18], 8)?,
                    number(&rest[30..36], 8)?,
                    number(&rest[59..70], 8)?,
                    number(&rest[53..59], 8)?,
                    1,
                    76,
                )
            }
            b"070701" => {
                let mut rest = [0; 104];
                reader.read_exact(&mut rest)?;
                (
                    number(&rest[8..16], 16)?,
                    number(&rest[32..40], 16)?,
                    number(&rest[48..56], 16)?,
                    number(&rest[88..96], 16)?,
                    4,
                    110,
                )
            }
            _ => anyhow::bail!("Unsupported CPIO format"),
        };
        ensure!((1..=4096).contains(&name_len), "Invalid CPIO path length");
        let mut name = vec![0; name_len as usize];
        reader.read_exact(&mut name)?;
        ensure!(
            name.pop() == Some(0) && !name.contains(&0),
            "Invalid CPIO path"
        );
        padding(&mut reader, header_len + name_len, alignment)?;
        let name = std::str::from_utf8(&name).context("CPIO path is not UTF-8")?;
        if name == "TRAILER!!!" {
            ensure!(size == 0, "Invalid CPIO trailer");
            // Consume the gzip footer/CRC and require only zero archive padding.
            zero_tail(&mut reader, cancelled)?;
            ensure!(reader.limit() > 0, "Expanded archive limit exceeded");
            ensure!(
                approved.iter().all(|name| roots.contains(name)),
                "Archive does not contain all approved items"
            );
            return Ok(());
        }
        entries += 1;
        ensure!(entries <= 10000, "Too many archive entries");
        if matches!(name, "." | "./") {
            ensure!(
                mode & 0o170000 == 0o040000 && size == 0,
                "Invalid archive root"
            );
            continue;
        }
        let name = name.strip_prefix("./").unwrap_or(name);
        // Directory names may have one conventional trailing separator; file
        // names and internal empty path segments remain invalid.
        let name = if mode & 0o170000 == 0o040000 {
            name.strip_suffix('/').unwrap_or(name)
        } else {
            name
        };
        ensure!(
            !name.contains('\\') && !name.contains(':'),
            "Unsafe CPIO path"
        );
        peerline_core::path::validate_relative_path(Path::new(name))?;
        ensure!(seen.insert(name.to_owned()), "Duplicate archive path");
        let top = name.split('/').next().context("Empty CPIO path")?;
        ensure!(
            approved.iter().any(|name| name == top),
            "Archive contains an unapproved item"
        );
        roots.insert(top.to_owned());
        let path = root.join(name);
        total = total.checked_add(size).context("Archive size overflow")?;
        ensure!(total <= limit, "Expanded archive limit exceeded");
        match mode & 0o170000 {
            0o040000 => {
                ensure!(size == 0, "Directory contains data");
                fs::create_dir_all(&path)?;
            }
            0o100000 => {
                ensure!(links == 1, "Hard links are not supported");
                fs::create_dir_all(path.parent().context("No parent")?)?;
                let mut options = OpenOptions::new();
                options.write(true).create_new(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.mode(0o600);
                }
                let mut file = options.open(&path)?;
                let mut remaining = size;
                let mut data = [0; 65536];
                while remaining > 0 {
                    ensure!(
                        !cancelled.load(Ordering::Relaxed),
                        "AirDrop transfer cancelled"
                    );
                    let count = remaining.min(data.len() as u64) as usize;
                    reader.read_exact(&mut data[..count])?;
                    file.write_all(&data[..count])?;
                    remaining -= count as u64;
                }
            }
            _ => anyhow::bail!("Links and special files are not supported"),
        }
        padding(&mut reader, size, alignment)?;
    }
}

fn number(bytes: &[u8], radix: u32) -> anyhow::Result<u64> {
    Ok(u64::from_str_radix(std::str::from_utf8(bytes)?, radix)?)
}
fn padding(reader: &mut impl Read, length: u64, alignment: u64) -> anyhow::Result<()> {
    let mut bytes = [0; 3];
    let count = ((alignment - length % alignment) % alignment) as usize;
    reader.read_exact(&mut bytes[..count])?;
    Ok(())
}

#[cfg(test)]
pub(super) fn cpio(name: &str, mode: u64, data: &[u8]) -> Vec<u8> {
    fn entry(out: &mut Vec<u8>, name: &str, mode: u64, data: &[u8]) {
        out.extend_from_slice(format!("070707{dev:06o}{ino:06o}{mode:06o}{uid:06o}{gid:06o}{links:06o}{rdev:06o}{mtime:011o}{names:06o}{size:011o}", dev=0,ino=1,uid=0,gid=0,links=1,rdev=0,mtime=0,names=name.len()+1,size=data.len()).as_bytes());
        out.extend_from_slice(name.as_bytes());
        out.push(0);
        out.extend_from_slice(data);
    }
    let mut out = Vec::new();
    entry(&mut out, name, mode, data);
    entry(&mut out, "TRAILER!!!", 0, &[]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn libarchive_directory_preserves_children_and_empty_directories() {
        use std::io::{Seek, SeekFrom};
        let root = tempfile::tempdir().unwrap();
        let mut source = tempfile::tempfile().unwrap();
        source
            .write_all(include_bytes!(
                "../../tests/fixtures/airdrop-directory.cpio.gz"
            ))
            .unwrap();
        source.seek(SeekFrom::Start(0)).unwrap();
        extract(
            source,
            root.path(),
            &["folder".into()],
            1024,
            &AtomicBool::new(false),
        )
        .unwrap();
        assert!(root.path().join("folder/empty").is_dir());
        assert_eq!(
            fs::read(root.path().join("folder/notes.txt")).unwrap(),
            b"folder fixture\n"
        );
    }

    #[test]
    fn cpio_directory_separators_are_normalized_without_allowing_traversal() {
        for (name, ok) in [
            ("photo.jpg/", true),
            ("./photo.jpg/", true),
            ("photo.jpg//", false),
            ("../photo.jpg/", false),
        ] {
            let root = tempfile::tempdir().unwrap();
            assert_eq!(
                extract_bytes(&cpio(name, 0o040700, b""), root.path()).is_ok(),
                ok,
                "{name}"
            );
        }
    }
    fn extract_bytes(bytes: &[u8], root: &Path) -> anyhow::Result<()> {
        use std::io::{Seek, SeekFrom};
        let mut source = tempfile::tempfile()?;
        source.write_all(bytes)?;
        source.seek(SeekFrom::Start(0))?;
        extract(
            source,
            root,
            &["photo.jpg".into()],
            1024,
            &AtomicBool::new(false),
        )
    }

    #[test]
    fn libarchive_odc_and_newc_gzip_fixtures_validate_crc_and_padding() {
        for bytes in [
            include_bytes!("../../tests/fixtures/airdrop-odc.cpio.gz").as_slice(),
            include_bytes!("../../tests/fixtures/airdrop-newc.cpio.gz").as_slice(),
        ] {
            let root = tempfile::tempdir().unwrap();
            extract_bytes(bytes, root.path()).unwrap();
            assert_eq!(
                fs::read(root.path().join("photo.jpg")).unwrap(),
                b"AirDrop fixture\n"
            );
            let root = tempfile::tempdir().unwrap();
            let mut corrupted = bytes.to_vec();
            // The last nonzero byte is the low byte of gzip ISIZE for these
            // small fixtures. Tampering must fail gzip integrity verification.
            let last = corrupted.iter().rposition(|b| *b != 0).unwrap();
            corrupted[last] ^= 1;
            assert!(extract_bytes(&corrupted, root.path()).is_err());
            let root = tempfile::tempdir().unwrap();
            assert!(extract_bytes(&bytes[..last], root.path()).is_err());
        }
    }

    #[test]
    fn truncated_or_concatenated_archives_are_rejected() {
        let archive = cpio("photo.jpg", 0o100600, b"photo");
        let root = tempfile::tempdir().unwrap();
        assert!(extract_bytes(&archive[..archive.len() - 1], root.path()).is_err());
        let root = tempfile::tempdir().unwrap();
        let mut appended = archive.clone();
        appended.extend_from_slice(&archive);
        assert!(extract_bytes(&appended, root.path()).is_err());
    }
    #[test]
    fn validates_archives_before_publication() {
        for (name, mode, data, limit, ok) in [
            ("./photo.jpg", 0o100644, b"photo".as_slice(), 100, true),
            ("../photo.jpg", 0o100644, b"photo".as_slice(), 100, false),
            ("/photo.jpg", 0o100644, b"photo".as_slice(), 100, false),
            ("photo.jpg", 0o120777, b"/etc/passwd".as_slice(), 100, false),
            ("surprise.jpg", 0o100644, b"photo".as_slice(), 100, false),
            ("photo.jpg", 0o100644, b"photo".as_slice(), 2, false),
        ] {
            let root = tempfile::tempdir().unwrap();
            let mut input = tempfile::tempfile().unwrap();
            input.write_all(&cpio(name, mode, data)).unwrap();
            use std::io::{Seek, SeekFrom};
            input.seek(SeekFrom::Start(0)).unwrap();
            assert_eq!(
                extract(
                    input,
                    root.path(),
                    &["photo.jpg".into()],
                    limit,
                    &AtomicBool::new(false)
                )
                .is_ok(),
                ok,
                "{name}"
            );
        }
    }
}
