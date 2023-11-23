use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::{
    io::{recv_one_message, send_message},
    node::{Node, NodeId},
};

pub fn recv_init() -> Result<Node> {
    let received = recv_one_message::<InitRequest>()
        .context("failed to receive 'init' message")?
        .ok_or_else(|| anyhow!("EOF during init"))?;

    let InitRequest::Init { node_id, node_ids } = received.body.payload;
    if received.dest != node_id {
        bail!("init message has invalid node_id");
    }

    let node = Node::new(node_id, node_ids);
    let (reply, _) =
        node.build_message_to(received.src, received.body.msg_id, InitResponse::InitOk);
    send_message(&reply)?;
    Ok(node)
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InitRequest {
    Init {
        // Node ID to assign to the initialized node.
        node_id: NodeId,
        // All nodes in the cluster, including the initialized node.
        node_ids: Vec<NodeId>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InitResponse {
    InitOk,
}
