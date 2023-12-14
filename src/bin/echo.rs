use std::sync::{Arc, Mutex};

use anyhow::bail;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use symmetrical_octo_potato::message::Message;
use symmetrical_octo_potato::sender::Sender;
use symmetrical_octo_potato::stdout_writer::StdOutWriter;
use symmetrical_octo_potato::Init;
use symmetrical_octo_potato::Initable;
use symmetrical_octo_potato::Messages;
use tokio::sync::broadcast::Receiver as BroadcastReceiver;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

#[derive(Clone)]
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
    let (tx, _) = tokio::sync::broadcast::channel(16);
    let output = Arc::new(Mutex::new(Sender::default()));
    symmetrical_octo_potato::init_stdin(tx.clone());
    let _ = symmetrical_octo_potato::init_parser::<EchoState, StdOutWriter>(
        tx.subscribe(),
        output.clone(),
    )
    .await?;
    let _ = init_echo(tx.subscribe(), output.clone()).await;
    Ok(())
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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

async fn init_echo(
    mut rx: BroadcastReceiver<Messages>,
    output: Arc<Mutex<Sender<StdOutWriter>>>,
) -> Result<()> {
    while let Ok(input) = rx.recv().await {
        match input {
            Messages::Stdin(value) => {
                let input: Message<EchoMessage> = match serde_json::from_value(value) {
                    Ok(msg) => msg,
                    Err(_) => {
                        continue;
                    }
                };

                handle_message(&input, &output)?;
            }
        };
    }
    Ok(())
}
