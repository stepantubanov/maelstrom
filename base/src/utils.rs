use std::time::Duration;

use anyhow::{Context, Result};
use futures::Future;
use tokio::time::{interval, MissedTickBehavior};

pub fn init_log() -> Result<()> {
    stderrlog::new()
        .verbosity(stderrlog::LogLevelNum::Trace)
        .init()
        .context("failed to init log")
}

pub fn async_spawn<F>(future: F)
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(error) = future.await {
            log::error!("async task error: {error:?}");
        }
    });
}

pub fn every<F, Fut>(period: Duration, mut f: F)
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send,
{
    async_spawn(async move {
        let mut interval = interval(period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        interval.reset(); // Skip first immediate tick.

        loop {
            interval.tick().await;
            f().await?;
        }
    });
}
