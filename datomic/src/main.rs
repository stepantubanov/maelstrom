use std::{collections::HashMap, num::ParseIntError, sync::Arc};

use anyhow::{bail, Result};
use base::{
    init::recv_init,
    node::NodeId,
    serve::{serve, RequestHandler, Service},
    utils::init_log,
};
use futures::{future::BoxFuture, FutureExt};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::linkv::{CasParams, LinKvClient};

mod linkv;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Request {
    Txn(Txn),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Response {
    TxnOk(Txn),
    Error {
        code: ServiceErrorCode,
        text: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Txn {
    txn: Vec<(Op, Key, Option<Value>)>,
}

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug)]
#[repr(u32)]
pub(crate) enum ServiceErrorCode {
    TxnConflict = 30,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
enum Op {
    #[serde(rename = "r")]
    Read,
    #[serde(rename = "append")]
    Append,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Hash, Eq, PartialEq)]
#[serde(try_from = "IntOrString")]
struct Key(u64);

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
enum IntOrString {
    Int(u64),
    String(String),
}

impl TryFrom<IntOrString> for Key {
    type Error = ParseIntError;

    fn try_from(value: IntOrString) -> Result<Self, Self::Error> {
        match value {
            IntOrString::String(s) => Ok(Self(s.parse()?)),
            IntOrString::Int(v) => Ok(Self(v)),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
enum Value {
    Int(u64),
    List(Vec<u64>),
}

impl Value {
    fn empty() -> Self {
        Self::List(Vec::new())
    }

    fn append(&self, other: &Value) -> Self {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => Self::List(vec![*a, *b]),
            (Self::Int(a), Self::List(b)) => Self::List([[*a].as_slice(), &b.as_slice()].concat()),
            (Self::List(a), Self::Int(b)) => Self::List([a.as_slice(), &[*b]].concat()),
            (Self::List(a), Self::List(b)) => Self::List([a.as_slice(), b.as_slice()].concat()),
        }
    }
}

struct Datomic {
    lin_kv: LinKvClient<u32, Tree>,
}

type Tree = HashMap<Key, Value>;

impl Datomic {
    const ROOT_KEY: u32 = 0;

    fn new(lin_kv: &LinKvClient<u32, Tree>) -> Self {
        Self {
            lin_kv: lin_kv.clone(),
        }
    }

    async fn execute(&self, txn: &mut Txn) -> Result<bool> {
        let prev_root = self.lin_kv.read(Self::ROOT_KEY).await?.unwrap_or_default();
        let mut root = prev_root.clone();

        for (op, op_key, op_value) in &mut txn.txn {
            match op {
                Op::Read => {
                    *op_value = root.get(op_key).cloned();
                }
                Op::Append => {
                    let Some(to_append) = op_value else {
                        bail!("missing txn value for append op");
                    };

                    let value = root.get(op_key).cloned().unwrap_or(Value::empty());
                    let appended = value.append(to_append);
                    root.insert(*op_key, appended);
                }
            }
        }

        self.lin_kv
            .cas(
                Self::ROOT_KEY,
                CasParams {
                    from: prev_root,
                    to: root,
                    create_if_not_exists: true,
                },
            )
            .await
    }
}

impl RequestHandler for Datomic {
    type Request = Request;
    type Response = Response;

    fn handle<'a>(
        self: &'a Arc<Self>,
        _sender: NodeId,
        request: Self::Request,
    ) -> BoxFuture<'a, Result<Option<Self::Response>>> {
        async move {
            let Request::Txn(txn) = request;

            for _ in 0..3 {
                let mut txn = txn.clone();
                if self.execute(&mut txn).await? {
                    return Ok(Some(Response::TxnOk(txn)));
                }
            }

            Ok(Some(Response::Error {
                code: ServiceErrorCode::TxnConflict,
                text: "txn conflict".into(),
            }))
        }
        .boxed()
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_log()?;
    let node = recv_init()?;
    let lin_kv_client = LinKvClient::new(&node);
    let datomic_service = Arc::new(Datomic::new(&lin_kv_client));
    serve((Service::new(&node, datomic_service), lin_kv_client.client())).await
}
