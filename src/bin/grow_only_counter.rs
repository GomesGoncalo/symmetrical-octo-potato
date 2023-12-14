use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use symmetrical_octo_potato::init_state::InitState;
use symmetrical_octo_potato::log::Log;
use symmetrical_octo_potato::message::Message;
use symmetrical_octo_potato::sender::Sender;
use symmetrical_octo_potato::stdout_writer::StdOutWriter;
use symmetrical_octo_potato::traits::store::Store;
use symmetrical_octo_potato::Messages;
use symmetrical_octo_potato::{gossip, Initable};
use tokio::sync::broadcast::Receiver as BroadcastReceiver;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

#[derive(Clone)]
struct GrowOnlyState {
    operations: Log<usize>,
}

impl Store<usize> for GrowOnlyState {
    fn get_log(&self) -> &Log<usize> {
        &self.operations
    }

    fn get_log_mut(&mut self) -> &mut Log<usize> {
        &mut self.operations
    }
}

impl Initable for GrowOnlyState {
    fn with_init(init: symmetrical_octo_potato::Init) -> Self {
        Self {
            operations: Log::with_init(init),
        }
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
    let state = symmetrical_octo_potato::init_parser::<GrowOnlyState, StdOutWriter>(
        tx.subscribe(),
        output.clone(),
    )
    .await?;
    loop {
        tokio::select! {
            () = gossip::handle(
                tx.subscribe(),
                output.clone(),
                state.clone(),
                Duration::from_millis(100)) => {}
            Err(_) = init_grow_only_counter(tx.subscribe(), output.clone(), state.clone()) => { break Ok(()) }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum GrowOnlyMessage {
    Add { delta: usize },
    Read { key: Option<String> },
    AddOk,
    ReadOk { value: usize },
}

fn handle_message(
    input: &Message<GrowOnlyMessage>,
    output: &Arc<Mutex<Sender<StdOutWriter>>>,
    state: &Arc<Mutex<InitState<GrowOnlyState>>>,
) -> Result<()> {
    match input.body.msg_type {
        GrowOnlyMessage::Add { delta } => {
            let _ = output
                .lock()
                .unwrap()
                .reply(input.clone(), GrowOnlyMessage::AddOk);

            state
                .lock()
                .unwrap()
                .get_inner_mut()
                .operations
                .insert(delta);
        }
        GrowOnlyMessage::Read { key: None } => {
            let _ = output.lock().unwrap().reply(
                input.clone(),
                GrowOnlyMessage::ReadOk {
                    value: state
                        .lock()
                        .unwrap()
                        .get_inner()
                        .operations
                        .values()
                        .sum::<usize>(),
                },
            );
        }
        GrowOnlyMessage::Read { key: Some(_) }
        | GrowOnlyMessage::AddOk
        | GrowOnlyMessage::ReadOk { value: _ } => {
            bail!("Not handled")
        }
    };
    Ok(())
}

async fn init_grow_only_counter(
    mut channel: BroadcastReceiver<Messages>,
    output: Arc<Mutex<Sender<StdOutWriter>>>,
    state: Arc<Mutex<InitState<GrowOnlyState>>>,
) -> Result<()> {
    while let Ok(input) = channel.recv().await {
        match input {
            Messages::Stdin(value) => {
                let input: Message<GrowOnlyMessage> = match serde_json::from_value(value) {
                    Ok(msg) => msg,
                    Err(_) => {
                        continue;
                    }
                };

                handle_message(&input, &output, &state)?;
            }
        };
    }
    Ok(())
}
