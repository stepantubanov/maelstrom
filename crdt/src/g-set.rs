use std::collections::HashSet;

use anyhow::Result;
use base::{node::NodeId, utils::init_log};
use crdt::crdt::{Crdt, CrdtService};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Add {
    element: u64,
}

#[derive(Default, Clone, Serialize, Deserialize)]
struct State {
    value: HashSet<u64>,
}

#[derive(Default)]
struct GSet(State);

impl Crdt for GSet {
    type Add = Add;
    type Query = State;
    type State = State;

    fn add(&mut self, _sender: &NodeId, add: Self::Add) -> Result<()> {
        self.0.value.insert(add.element);
        Ok(())
    }

    fn merge(&mut self, other: Self::State) -> Result<()> {
        self.0.value.extend(other.value);
        Ok(())
    }

    fn query(&self) -> Self::Query {
        self.0.clone()
    }

    fn state(&self) -> Self::State {
        self.0.clone()
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_log()?;
    CrdtService::<GSet>::run().await
}
