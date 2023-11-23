use std::sync::Arc;

use anyhow::{bail, Result};
use futures::{
    future::{ready, BoxFuture},
    FutureExt, StreamExt,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    io::{recv_messages, send_message},
    message::Message,
    node::{Node, NodeId},
    utils::async_spawn,
};

pub trait MessageHandler {
    type MessagePayload: DeserializeOwned + Send + 'static;

    fn handle(&self, message: Message<Self::MessagePayload>) -> Result<()>;
}

pub async fn serve<H: MessageHandler>(handler: H) -> Result<()> {
    let mut incoming = recv_messages::<H::MessagePayload>();
    while let Some(message) = incoming.next().await.transpose()? {
        if let Err(error) = handler.handle(message) {
            log::error!("error processing message: {error:?}");
        }
    }
    Ok(())
}

macro_rules! impl_tuple_handler {
    ($payload:ident, $($idx:tt => $t:ident),+) => {
        #[derive(serde::Deserialize)]
        #[serde(untagged)]
        pub enum $payload<$($t),+>{
            $( $t($t) ),+
        }

        impl<$($t),+> MessageHandler for ($($t),+)
        where $($t: MessageHandler),+
        {
            type MessagePayload = $payload<$($t::MessagePayload),+>;

            fn handle(&self, message: Message<Self::MessagePayload>) -> Result<()> {
                let (payload, message) = message.replace_payload(());
                match payload {
                    $($payload::$t(p) => self.$idx.handle(message.with_payload(p))),+
                }
            }
        }
    };
}

impl_tuple_handler!(TuplePayload2, 0 => T1, 1 => T2);
impl_tuple_handler!(TuplePayload3, 0 => T1, 1 => T2, 2 => T3);

pub trait RequestHandler: Send + Sync + 'static {
    type Request: DeserializeOwned + Send + 'static;
    type Response: Serialize + Send + 'static;

    fn handle<'a>(
        self: &'a Arc<Self>,
        from: NodeId,
        request: Self::Request,
    ) -> BoxFuture<'a, Result<Option<Self::Response>>>;
}

#[derive(Debug)]
pub struct Service<H: ?Sized> {
    request_handler: Arc<H>,
    node: Node,
}

impl<H: ?Sized> Clone for Service<H> {
    fn clone(&self) -> Self {
        Self {
            request_handler: Arc::clone(&self.request_handler),
            node: self.node.clone(),
        }
    }
}

impl<H> Service<H> {
    pub fn new(node: &Node, request_handler: Arc<H>) -> Self {
        Self {
            request_handler,
            node: node.clone(),
        }
    }
}

impl<H> Service<H>
where
    H: RequestHandler + ?Sized,
{
    async fn handle_request(&self, message: Message<H::Request>) -> Result<()> {
        if message.body.in_reply_to.is_some() {
            bail!("service request `in_reply_to` isn't none");
        }

        if let Some(response) = self
            .request_handler
            .handle(message.src.clone(), message.body.payload)
            .await?
        {
            let (reply, _) = self
                .node
                .build_message_to(message.src, message.body.msg_id, response);
            send_message(&reply)?;
        }
        Ok(())
    }
}

impl<H> MessageHandler for Service<H>
where
    H: RequestHandler + Send + ?Sized,
{
    type MessagePayload = H::Request;

    fn handle(&self, message: Message<Self::MessagePayload>) -> Result<()> {
        let service = self.clone();
        async_spawn(async move { service.handle_request(message).await });
        Ok(())
    }
}

impl<Req, Res> RequestHandler for dyn Fn(Req) -> Result<Res> + Send + Sync
where
    Self: Send + Sync,
    Req: DeserializeOwned + Send + 'static,
    Res: Serialize + Send + 'static,
{
    type Request = Req;
    type Response = Res;

    fn handle<'a>(
        self: &'a Arc<Self>,
        _from: NodeId,
        request: Self::Request,
    ) -> BoxFuture<'a, Result<Option<Self::Response>>> {
        let result = Some((self)(request)).transpose();
        ready(result).boxed()
    }
}

pub fn make_service<Req, Res, F>(
    node: Node,
    f: F,
) -> Service<dyn Fn(Req) -> Result<Res> + Send + Sync + 'static>
where
    F: Fn(Req) -> Result<Res> + Send + Sync + 'static,
{
    Service {
        request_handler: Arc::new(f),
        node,
    }
}
