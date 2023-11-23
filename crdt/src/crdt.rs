use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use anyhow::Result;
use base::{
    client::Client,
    init::recv_init,
    node::NodeId,
    serve::{serve, RequestHandler, Service},
    utils::every,
};
use futures::{future::BoxFuture, FutureExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub trait Crdt: Default + Send + 'static {
    type Add: Serialize + DeserializeOwned + Send;
    type State: Serialize + DeserializeOwned + Clone + Send;
    type Query: Serialize + DeserializeOwned + Send;

    fn add(&mut self, node_id: &NodeId, add: Self::Add) -> Result<()>;
    fn merge(&mut self, other: Self::State) -> Result<()>;
    fn state(&self) -> Self::State;
    fn query(&self) -> Self::Query;
}

#[allow(type_alias_bounds)]
pub type CrdtClient<C: Crdt> = Client<Request<C::Add, C::State>, Response<C::Query>>;

pub struct CrdtService<C: Crdt> {
    crdt: Mutex<C>,
    client: CrdtClient<C>, //Client<Request<C::Add, C::State>, Response<C::Query>>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request<A, S> {
    Add(A),
    Read,
    Replicate(S),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response<Q> {
    AddOk,
    ReadOk(Q),
}

impl<C: Crdt> CrdtService<C> {
    fn new(client: &CrdtClient<C>) -> Self {
        Self {
            crdt: Mutex::default(),
            client: client.clone(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, C> {
        self.crdt.lock().expect("lock panic")
    }

    fn start_replicating(self: &Arc<Self>) {
        const REPLICATION_INTERVAL: Duration = Duration::from_secs(5);

        let service = self.clone();
        let weak = Arc::downgrade(&service);

        every(REPLICATION_INTERVAL, move || {
            let weak = weak.clone();
            async move {
                let Some(service) = weak.upgrade() else {
                    // Service got dropped.
                    return Ok(());
                };
                service.replicate().await
            }
        });
    }

    async fn replicate(&self) -> Result<()> {
        let state = self.lock().state();
        let client = &self.client;
        for node_id in client.node().node_ids() {
            if node_id == client.node().node_id() {
                continue;
            }
            client
                .send_no_reply(node_id.clone(), Request::Replicate(state.clone()))
                .await?;
        }
        Ok(())
    }

    pub async fn run() -> Result<()> {
        let node = recv_init()?;
        let client = Client::new(&node);
        let service = Arc::new(Self::new(&client));

        service.start_replicating();
        serve((Service::new(&node, service), client)).await
    }
}

impl<C: Crdt> RequestHandler for CrdtService<C> {
    type Request = Request<C::Add, C::State>;
    type Response = Response<C::Query>;

    fn handle<'a>(
        self: &'a Arc<Self>,
        sender: NodeId,
        request: Self::Request,
    ) -> BoxFuture<'a, Result<Option<Self::Response>>> {
        async move {
            match request {
                Request::Add(add) => {
                    self.lock().add(&sender, add)?;
                    Ok(Some(Response::AddOk))
                }
                Request::Read => {
                    let query = self.lock().query();
                    Ok(Some(Response::ReadOk(query)))
                }
                Request::Replicate(state) => {
                    self.lock().merge(state)?;
                    Ok(None)
                }
            }
        }
        .boxed()
    }
}
