use anyhow::{bail, Result};
use base::{
    client::Client,
    node::{Node, NodeId},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum LinKvRequest<K, V> {
    Read {
        key: K,
    },
    Write {
        key: K,
        value: V,
    },
    Cas {
        key: K,
        #[serde(flatten)]
        params: CasParams<V>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CasParams<V> {
    pub from: V,
    pub to: V,
    pub create_if_not_exists: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum LinKvResponse<V> {
    ReadOk { value: V },
    WriteOk,
    CasOk,
    Error { code: LinKvErrorCode, text: String },
}

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug)]
#[repr(u32)]
pub(crate) enum LinKvErrorCode {
    KeyDoesNotExist = 20,
    PreconditionFailed = 22,
}

#[derive(Debug)]
pub(crate) struct LinKvClient<K, V> {
    client: Client<LinKvRequest<K, V>, LinKvResponse<V>>,
}

impl<K, V> Clone for LinKvClient<K, V> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
        }
    }
}

impl<K, V> LinKvClient<K, V>
where
    K: Serialize + DeserializeOwned + std::fmt::Debug,
    V: Serialize + DeserializeOwned + std::fmt::Debug,
{
    pub(crate) fn new(node: &Node) -> Self {
        Self {
            client: Client::new(node),
        }
    }

    pub(crate) async fn read(&self, key: K) -> Result<Option<V>> {
        match self
            .client
            .send(NodeId::lin_kv(), LinKvRequest::Read { key })
            .await?
        {
            LinKvResponse::ReadOk { value } => Ok(Some(value)),
            LinKvResponse::Error {
                code: LinKvErrorCode::KeyDoesNotExist,
                ..
            } => Ok(None),
            response => bail!("unexpected response from lin-kv: {response:?}"),
        }
    }

    pub(crate) async fn cas(&self, key: K, params: CasParams<V>) -> Result<bool> {
        match self
            .client
            .send(NodeId::lin_kv(), LinKvRequest::Cas { key, params })
            .await?
        {
            LinKvResponse::CasOk {} => Ok(true),
            LinKvResponse::Error {
                code: LinKvErrorCode::PreconditionFailed,
                ..
            } => Ok(false),
            response => bail!("lin-kv CAS failed: {response:?}"),
        }
    }

    pub(crate) fn client(&self) -> Client<LinKvRequest<K, V>, LinKvResponse<V>> {
        self.client.clone()
    }
}
