use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use symmetrical_octo_potato::{
    init_state::{init_parser, Init, Initable},
    message::Message,
    sender::Sender,
    stdout_writer::StdOutWriter,
    wait_for_message_then,
};
use tracing_subscriber::{fmt, prelude::*};
use ulid::Ulid;

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
    let (tx, mut rx) = tokio::sync::broadcast::channel(16);
    let output = Arc::new(Mutex::new(Sender::default()));
    symmetrical_octo_potato::init_stdin(tx.clone());
    let _ = init_parser::<UniqueIdsState, StdOutWriter>(tx.subscribe(), output.clone()).await?;
    let _ = wait_for_message_then(&mut rx, |msg| handle_message(msg, output.clone())).await;
    Ok(())
}

#[derive(Serialize, Deserialize, Clone)]
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
