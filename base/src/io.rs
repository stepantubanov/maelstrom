use crate::message::Message;
use anyhow::{Context, Result};
use futures::{stream, Stream};
use serde::{de::DeserializeOwned, Serialize};

pub(crate) fn send_message<P: Serialize>(message: &Message<P>) -> Result<()> {
    let json = serde_json::to_string(message).context("failed to serialize into JSON")?;
    println!("{json}");
    Ok(())
}

pub(crate) fn recv_messages<P: Send + DeserializeOwned + 'static>(
) -> impl Stream<Item = Result<Message<P>>> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    std::thread::spawn(move || {
        let mut buf = String::with_capacity(1024);
        loop {
            let item = read_message::<P>(&mut buf).transpose();
            if tx.blocking_send(item).is_err() {
                break;
            }
        }
    });

    stream::poll_fn(move |ctx| rx.poll_recv(ctx).map(Option::flatten))
}

pub(crate) fn recv_one_message<P: DeserializeOwned>() -> Result<Option<Message<P>>> {
    let mut buf = String::with_capacity(1024);
    read_message(&mut buf)
}

fn read_message<P: DeserializeOwned>(buf: &mut String) -> Result<Option<Message<P>>> {
    buf.clear();
    std::io::stdin().read_line(buf)?;

    let message = serde_json::from_str(buf)
        .with_context(|| format!("failed to deserialize from JSON: '{buf}'"))?;
    Ok(message)
}
