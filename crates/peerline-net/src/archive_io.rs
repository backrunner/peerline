use peerline_transfer::Archive;
use std::io::{self, SeekFrom};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    sync::mpsc,
};

const CHUNK_BYTES: usize = 64 * 1024;
const PREFETCH_CHUNKS: usize = 8;

/// Leave disk reads in flight while the transport encrypts and sends previous chunks.
pub(crate) fn prefetch(
    archive: &Archive,
    offset: u64,
) -> anyhow::Result<mpsc::Receiver<io::Result<Vec<u8>>>> {
    let mut remaining = archive
        .len()
        .checked_sub(offset)
        .ok_or_else(|| anyhow::anyhow!("invalid archive offset"))?;
    let mut file = tokio::fs::File::from_std(archive.reader()?);
    let (tx, rx) = mpsc::channel(PREFETCH_CHUNKS);
    tokio::spawn(async move {
        if let Err(error) = file.seek(SeekFrom::Start(offset)).await {
            let _ = tx.send(Err(error)).await;
            return;
        }
        while remaining > 0 {
            let Ok(slot) = tx.reserve().await else { break };
            let len = remaining.min(CHUNK_BYTES as u64) as usize;
            let mut bytes = vec![0; len];
            if let Err(error) = file.read_exact(&mut bytes).await {
                slot.send(Err(error));
                break;
            }
            slot.send(Ok(bytes));
            remaining -= len as u64;
        }
    });
    Ok(rx)
}
