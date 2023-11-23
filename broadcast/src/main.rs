use anyhow::{anyhow, Context, Result};
use base::{
    client::Client,
    init::recv_init,
    node::NodeId,
    serve::{serve, RequestHandler, Service},
    utils::{async_spawn, init_log},
};
use futures::{future::BoxFuture, FutureExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
};

struct BroadcastService {
    state: Mutex<BroadcastState>,
    client: Client<Request, Response>,
}

#[derive(Default)]
struct BroadcastState {
    neighbors_except_self: Vec<NodeId>,
    messages: HashSet<u64>,
}

impl BroadcastService {
    fn new(client: &Client<Request, Response>) -> Self {
        Self {
            state: Mutex::default(),
            client: client.clone(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, BroadcastState> {
        self.state.lock().expect("lock failed")
    }

    fn node_id(&self) -> &NodeId {
        self.client.node().node_id()
    }

    fn update_topology(&self, mut topology: HashMap<NodeId, Vec<NodeId>>) -> anyhow::Result<()> {
        let neighbors = topology.remove(self.node_id()).ok_or_else(|| {
            anyhow!(
                "topology does not contain this node's neighbors {}",
                self.node_id()
            )
        })?;
        let neighbors_except_self = neighbors
            .into_iter()
            .filter(|neighbor| neighbor != self.node_id())
            .collect();

        self.lock().neighbors_except_self = neighbors_except_self;
        Ok(())
    }

    async fn broadcast(&self, except_node_id: &NodeId, message: u64) -> Result<()> {
        let targets = {
            let mut state = self.lock();

            let is_new_message = state.messages.insert(message);
            if is_new_message {
                state
                    .neighbors_except_self
                    .iter()
                    .filter(|target| *target != except_node_id)
                    .cloned()
                    .collect::<Vec<_>>()
            } else {
                return Ok(());
            }
        };

        for target in targets {
            let client = self.client.clone();
            async_spawn(async move {
                const MAX_ATTEMPTS: usize = 10;

                client
                    .send_with_retry(MAX_ATTEMPTS, target.clone(), Request::Broadcast { message })
                    .await
                    .with_context(|| format!("failed to broadcast msg {message} to {target}"))?;

                log::info!("broadcast msg {message} to {target}: OK");
                Ok(())
            });
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Request {
    Topology {
        topology: HashMap<NodeId, Vec<NodeId>>,
    },
    Broadcast {
        message: u64,
    },
    Read,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Response {
    TopologyOk,
    BroadcastOk,
    ReadOk { messages: HashSet<u64> },
}

impl RequestHandler for BroadcastService {
    type Request = Request;
    type Response = Response;

    fn handle<'a>(
        self: &'a Arc<Self>,
        sender: NodeId,
        request: Request,
    ) -> BoxFuture<'a, Result<Option<Response>>> {
        async move {
            match request {
                Request::Topology { topology } => {
                    self.update_topology(topology)?;
                    Ok(Some(Response::TopologyOk))
                }
                Request::Broadcast { message } => {
                    self.broadcast(&sender, message).await?;
                    Ok(Some(Response::BroadcastOk))
                }
                Request::Read => Ok(Some(Response::ReadOk {
                    messages: self.lock().messages.clone(),
                })),
            }
        }
        .boxed()
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_log()?;
    let node = recv_init()?;
    let client = Client::new(&node);
    let service = Service::new(&node, Arc::new(BroadcastService::new(&client)));
    serve((service, client)).await
}
