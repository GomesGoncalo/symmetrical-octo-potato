use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use symmetrical_octo_potato::message::Message;
use symmetrical_octo_potato::sender::Sender;
use symmetrical_octo_potato::stdout_writer::StdOutWriter;
use symmetrical_octo_potato::{Init, Initable, Messages};
use tokio::sync::broadcast::Receiver as BroadcastReceiver;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use ulid::Ulid;

#[derive(Default, Clone)]
struct UniqueIdsState {}

impl Initable for UniqueIdsState {
    fn with_init(_init: Init) -> Self {
        Self {}
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let layer = fmt::layer()
        .with_writer(std::io::stderr)
        .with_thread_ids(true)
        .with_ansi(false)
        .pretty();
    tracing_subscriber::registry().with(layer).init();
    let (tx, _) = tokio::sync::broadcast::channel(16);
    let output = Arc::new(Mutex::new(Sender::default()));
    symmetrical_octo_potato::init_stdin(tx.clone());
    let _ = symmetrical_octo_potato::init_parser::<UniqueIdsState, StdOutWriter>(
        tx.subscribe(),
        output.clone(),
    )
    .await?;
    let _ = init_unique_ids(tx.subscribe(), output.clone()).await;
    Ok(())
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum UniqueIdMessage {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
}

fn handle_message(
    input: Message<UniqueIdMessage>,
    output: Arc<Mutex<Sender<StdOutWriter>>>,
) -> Result<()> {
    match input.body.msg_type {
        UniqueIdMessage::Generate { .. } => {
            let _ = output.lock().unwrap().reply(
                input,
                UniqueIdMessage::GenerateOk {
                    guid: Ulid::new().to_string(),
                },
            );
        }
        UniqueIdMessage::GenerateOk { .. } => {
            bail!("not expecting this");
        }
    };
    Ok(())
}

async fn init_unique_ids(
    mut rx: BroadcastReceiver<Messages>,
    output: Arc<Mutex<Sender<StdOutWriter>>>,
) -> Result<()> {
    while let Ok(input) = rx.recv().await {
        match input {
            Messages::Stdin(value) => {
                let input: Message<UniqueIdMessage> = match serde_json::from_value(value) {
                    Ok(msg) => msg,
                    Err(_) => {
                        continue;
                    }
                };

                handle_message(input, output.clone())?;
            }
        };
    }
    Ok(())
}
