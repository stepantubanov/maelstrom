use anyhow::Result;
use base::{
    init::recv_init,
    serve::{make_service, serve},
    utils::init_log,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Request {
    Echo { echo: String },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Response {
    EchoOk { echo: String },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_log()?;
    let node = recv_init()?;
    let service = make_service(node, |Request::Echo { echo }| -> Result<Response> {
        Ok(Response::EchoOk { echo })
    });

    serve(service).await
}
