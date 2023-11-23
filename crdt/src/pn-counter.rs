use std::{cmp, collections::HashMap};

use anyhow::Result;
use base::{node::NodeId, utils::init_log};
use crdt::crdt::{Crdt, CrdtService};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Add {
    delta: i64,
}

#[derive(Default, Clone, Serialize, Deserialize)]
struct Query {
    value: i64,
}

#[derive(Default, Clone, Serialize, Deserialize)]
struct State {
    counters: HashMap<NodeId, (i64, i64)>,
}

#[derive(Default)]
struct PnCounter(State);

impl Crdt for PnCounter {
    type Add = Add;
    type Query = Query;
    type State = State;

    fn add(&mut self, sender: &NodeId, add: Self::Add) -> Result<()> {
        if add.delta == 0 {
            return Ok(());
        }

        let pos = cmp::max(add.delta, 0);
        let neg = cmp::min(add.delta, 0);

        self.0
            .counters
            .entry(sender.clone())
            .and_modify(|(p, n)| {
                *p += pos;
                *n += neg;
            })
            .or_insert((pos, neg));
        Ok(())
    }

    fn merge(&mut self, other: Self::State) -> Result<()> {
        let counters = &mut self.0.counters;
        for (node_id, (other_p, other_n)) in other.counters {
            counters
                .entry(node_id)
                .and_modify(|(p, n)| {
                    *p = cmp::max(*p, other_p);
                    *n = cmp::min(*n, other_n);
                })
                .or_insert((other_p, other_n));
        }
        Ok(())
    }

    fn query(&self) -> Self::Query {
        Query {
            value: self.0.counters.values().map(|(p, n)| *p + *n).sum(),
        }
    }

    fn state(&self) -> Self::State {
        self.0.clone()
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_log()?;
    CrdtService::<PnCounter>::run().await
}
