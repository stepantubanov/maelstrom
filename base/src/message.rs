use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use crate::node::NodeId;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message<P> {
    pub src: NodeId,
    pub dest: NodeId,
    pub body: MessageBody<P>,
}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Debug, Copy, Clone)]
pub struct MessageId(pub(crate) u64);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MessageBody<P> {
    pub msg_id: Option<MessageId>,
    pub in_reply_to: Option<MessageId>,

    #[serde(flatten)]
    pub payload: P,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum MessagePayload<Req, Res> {
    Request(Req),
    Response(Res),
}

impl<P> Message<P> {
    pub(crate) fn replace_payload<U>(self, payload: U) -> (P, Message<U>) {
        let prev = self.body.payload;
        let new = Message {
            src: self.src,
            dest: self.dest,
            body: MessageBody {
                msg_id: self.body.msg_id,
                in_reply_to: self.body.in_reply_to,
                payload,
            },
        };

        (prev, new)
    }

    pub(crate) fn with_payload<U>(self, payload: U) -> Message<U> {
        let (_, msg) = self.replace_payload(payload);
        msg
    }
}
