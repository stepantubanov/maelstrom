use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use crate::message::{Message, MessageBody, MessageId};

// Builds messages (assigns correct src/dst node ids, issues messages ids)
#[derive(Debug, Clone)]
pub struct Node {
    inner: Arc<NodeInner>,
}

#[derive(Debug)]
struct NodeInner {
    node_id: NodeId,
    node_ids: Vec<NodeId>,
    autoincrement: Autoincrement,
}

impl Node {
    pub(crate) fn new(node_id: NodeId, node_ids: Vec<NodeId>) -> Self {
        Self {
            inner: Arc::new(NodeInner {
                node_id,
                node_ids,
                autoincrement: Autoincrement::new(),
            }),
        }
    }

    pub fn node_id(&self) -> &NodeId {
        &self.inner.node_id
    }

    pub fn node_ids(&self) -> &[NodeId] {
        &self.inner.node_ids
    }

    pub(crate) fn build_message_to<P>(
        &self,
        dest: NodeId,
        in_reply_to: Option<MessageId>,
        payload: P,
    ) -> (Message<P>, MessageId) {
        let msg_id = self.inner.autoincrement.new_message_id();
        let message = Message {
            src: self.inner.node_id.clone(),
            dest,
            body: MessageBody {
                msg_id: Some(msg_id),
                in_reply_to,
                payload,
            },
        };
        (message, msg_id)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
pub struct NodeId(String);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}

impl NodeId {
    pub fn lin_kv() -> Self {
        Self("lin-kv".into())
    }
}

#[derive(Debug)]
struct Autoincrement {
    next: AtomicU64,
}

impl Autoincrement {
    fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    fn new_message_id(&self) -> MessageId {
        MessageId(self.next.fetch_add(1, Ordering::Relaxed))
    }
}
