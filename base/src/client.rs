use anyhow::{anyhow, bail, ensure, Result};
use serde::{de::DeserializeOwned, Serialize};
use tokio::time::sleep;

use crate::{
    io::send_message,
    message::{Message, MessageId, MessagePayload},
    node::{Node, NodeId},
    outgoing::Outgoing,
    serve::MessageHandler,
};
use std::{marker::PhantomData, sync::Arc, time::Duration};

#[derive(Debug)]
pub struct Client<Req, Res> {
    inner: Arc<ClientInner<Req, Res>>,
}

#[derive(Debug)]
struct ClientInner<Req, Res> {
    node: Node,
    outgoing: Outgoing<Res>,
    // Send + Sync, covariant with Req
    _marker: PhantomData<fn() -> Req>,
}

impl<Req, Res> Clone for Client<Req, Res> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<Req, Res> Client<Req, Res> {
    pub fn new(node: &Node) -> Self {
        const REQUEST_TTL: Duration = Duration::from_secs(3);

        Self {
            inner: Arc::new(ClientInner {
                node: node.clone(),
                outgoing: Outgoing::new(REQUEST_TTL),
                _marker: PhantomData,
            }),
        }
    }

    pub fn node(&self) -> &Node {
        &self.inner.node
    }

    pub(crate) fn complete(&self, request_id: MessageId, response: Res) -> Result<()> {
        self.inner.outgoing.complete(request_id, response)
    }
}

impl<Req, Res> Client<Req, Res>
where
    Req: Serialize + DeserializeOwned,
    Res: Serialize + DeserializeOwned,
{
    pub async fn send(&self, to: NodeId, request: Req) -> Result<Res> {
        ensure!(
            *self.inner.node.node_id() != to,
            "can't send message to self"
        );

        let (message, request_id) = self
            .inner
            .node
            .build_message_to::<MessagePayload<Req, Res>>(
                to,
                None,
                MessagePayload::Request(request),
            );

        let pending = self.inner.outgoing.push(request_id);
        send_message::<MessagePayload<Req, Res>>(&message)?;
        pending
            .wait()
            .await
            .ok_or_else(|| anyhow!("request timed out"))
    }

    pub async fn send_no_reply(&self, to: NodeId, request: Req) -> Result<()> {
        let (message, _) = self
            .inner
            .node
            .build_message_to::<MessagePayload<Req, Res>>(
                to,
                None,
                MessagePayload::Request(request),
            );
        send_message::<MessagePayload<Req, Res>>(&message)?;
        Ok(())
    }

    pub async fn send_with_retry(
        &self,
        max_attempts: usize,
        to: NodeId,
        request: Req,
    ) -> Result<Res> {
        const DELAY: Duration = Duration::from_secs(1);

        let (message, msg_id) = self
            .inner
            .node
            .build_message_to::<MessagePayload<Req, Res>>(
                to,
                None,
                MessagePayload::Request(request),
            );

        for _ in 0..max_attempts {
            let pending = self.inner.outgoing.push(msg_id);
            send_message::<MessagePayload<Req, Res>>(&message)?;
            if let Some(response) = pending.wait().await {
                return Ok(response);
            }

            sleep(DELAY).await;
        }

        bail!("retries exhaused")
    }
}

impl<Req, Res> MessageHandler for Client<Req, Res>
where
    Res: DeserializeOwned + Send + 'static,
{
    type MessagePayload = Res;

    fn handle(&self, message: Message<Self::MessagePayload>) -> Result<()> {
        let Some(request_id) = message.body.in_reply_to else {
            bail!("message does not contain `in_reply_to`");
        };

        self.complete(request_id, message.body.payload)
    }
}
