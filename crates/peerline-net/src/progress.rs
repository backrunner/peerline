use peerline_core::{PeerlineEvent, TransferId};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;

#[derive(Default)]
pub(crate) struct Progress {
    last: Option<Instant>,
}

impl Progress {
    pub(crate) fn report(
        &mut self,
        events: &Option<UnboundedSender<PeerlineEvent>>,
        id: TransferId,
        bytes_done: u64,
        bytes_total: u64,
    ) {
        let Some(events) = events else { return };
        let now = Instant::now();
        if bytes_done != bytes_total
            && self
                .last
                .is_some_and(|last| now.duration_since(last) < Duration::from_millis(100))
        {
            return;
        }
        self.last = Some(now);
        let _ = events.send(PeerlineEvent::Progress {
            id,
            bytes_done,
            bytes_total,
        });
    }
}
