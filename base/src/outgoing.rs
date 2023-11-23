use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

use anyhow::{bail, Result};
use futures::FutureExt;
use hashlink::LinkedHashMap;
use tokio::{sync::oneshot, time::timeout};

use crate::message::MessageId;

#[derive(Debug)]
pub(crate) struct Outgoing<Res> {
    queue: Mutex<LinkedHashMap<MessageId, QueueItem<Res>>>,
    ttl: Duration,
}

#[derive(Debug)]
struct QueueItem<R> {
    tx: oneshot::Sender<R>,
    started_at: Instant,
}

impl<R> Outgoing<R> {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            queue: Mutex::new(LinkedHashMap::new()),
            ttl,
        }
    }

    pub(crate) fn push(&self, request_id: MessageId) -> PendingResponse<R> {
        let now = Instant::now();

        let (tx, rx) = oneshot::channel();
        let mut queue = self.queue.lock().expect("lock panic");
        queue.insert(
            request_id,
            QueueItem {
                tx,
                started_at: now,
            },
        );
        while matches!(queue.front(), Some((_, item)) if now - item.started_at > self.ttl) {
            queue.pop_front();
        }
        PendingResponse { rx, ttl: self.ttl }
    }

    pub(crate) fn complete(&self, request_id: MessageId, response: R) -> Result<()> {
        let Some(item) = self.queue.lock().expect("lock panic").remove(&request_id) else {
            bail!("pending request not found {request_id:?}");
        };

        // Ignore send error - it just means that nobody is going to read this response.
        let _ = item.tx.send(response);
        Ok(())
    }
}

pub(crate) struct PendingResponse<R> {
    rx: oneshot::Receiver<R>,
    ttl: Duration,
}

impl<R> PendingResponse<R> {
    /// Returns `None` if request timed out.
    pub(crate) async fn wait(self) -> Option<R> {
        timeout(
            self.ttl,
            // Sender may be dropped if request gets evited due to TTL.
            self.rx.map(Result::ok),
        )
        .await
        .ok()
        .flatten()
    }
}
