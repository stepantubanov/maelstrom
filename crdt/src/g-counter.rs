use std::{cmp, collections::HashMap};

use anyhow::Result;
use base::{node::NodeId, utils::init_log};
use crdt::crdt::{Crdt, CrdtService};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Add {
    delta: u64,
}

#[derive(Default, Clone, Serialize, Deserialize)]
struct Query {
    value: u64,
}

#[derive(Default, Clone, Serialize, Deserialize)]
struct State {
    counters: HashMap<NodeId, u64>,
}

#[derive(Default)]
struct GCounter(State);

impl Crdt for GCounter {
    type Add = Add;
    type Query = Query;
    type State = State;

    fn add(&mut self, sender: &NodeId, add: Self::Add) -> Result<()> {
        self.0
            .counters
            .entry(sender.clone())
            .and_modify(|v| *v += add.delta)
            .or_insert(add.delta);
        Ok(())
    }

    fn merge(&mut self, other: Self::State) -> Result<()> {
        let counters = &mut self.0.counters;
        for (node_id, value) in other.counters {
            counters
                .entry(node_id)
                .and_modify(|v| *v = cmp::max(*v, value))
                .or_insert(value);
        }
        Ok(())
    }

    fn query(&self) -> Self::Query {
        Query {
            value: self.0.counters.values().copied().sum(),
        }
    }

    fn state(&self) -> Self::State {
        self.0.clone()
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_log()?;
    CrdtService::<GCounter>::run().await
}
