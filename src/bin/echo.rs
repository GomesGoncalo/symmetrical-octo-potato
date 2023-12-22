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

struct EchoState {}

impl Initable for EchoState {
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
    let _ = init_parser::<EchoState, StdOutWriter>(tx.subscribe(), output.clone()).await?;
    let _ = wait_for_message_then(&mut rx, |msg| handle_message(&msg, &output)).await;
    Ok(())
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoMessage {
    Echo { echo: String },
    EchoOk { echo: String },
}

fn handle_message(
    input: &Message<EchoMessage>,
    output: &Arc<Mutex<Sender<StdOutWriter>>>,
) -> Result<()> {
    match input.body.msg_type {
        EchoMessage::Echo { ref echo } => {
            let _ = output.lock().unwrap().reply(
                input.clone(),
                EchoMessage::EchoOk {
                    echo: echo.to_string(),
                },
            );
        }
        EchoMessage::EchoOk { .. } => {
            bail!("do not handle this")
        }
    };
    Ok(())
}
