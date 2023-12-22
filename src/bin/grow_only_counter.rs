use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
    time::Duration,
};
use symmetrical_octo_potato::{
    gossip,
    init_state::{init_parser, Init, InitState, Initable},
    log::Log,
    message::Message,
    sender::Sender,
    stdout_writer::StdOutWriter,
    traits::store::Store,
    wait_for_message_then,
};
use tracing_subscriber::{fmt, prelude::*};

struct GrowOnlyState {
    operations: Log<usize>,
}

impl Store<usize> for GrowOnlyState {
    fn new_value(&mut self, new: &usize) {
        tracing::info!(new = %new, "received");
    }
}

impl Initable for GrowOnlyState {
    fn with_init(init: Init) -> Self {
        Self {
            operations: Log::with_init(init),
        }
    }
}

impl Deref for GrowOnlyState {
    type Target = Log<usize>;

    fn deref(&self) -> &Self::Target {
        &self.operations
    }
}
impl DerefMut for GrowOnlyState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.operations
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
    let state = init_parser::<GrowOnlyState, StdOutWriter>(tx.subscribe(), output.clone()).await?;

    let tx_clone = tx.clone();
    let output_clone = output.clone();
    let state_clone = state.clone();
    tokio::spawn(async move {
        gossip::handle(
            tx_clone.subscribe(),
            output_clone,
            state_clone,
            Duration::from_millis(100),
        )
        .await;
    });

    let _ = wait_for_message_then(&mut rx, |msg| handle_message(&msg, &output, &state)).await;
    Ok(())
}

#[derive(Serialize, Deserialize, Clone)]
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

            let mut state = state.lock().unwrap();
            if let Some(val) = state.operations.insert(&delta) {
                state.new_value(val);
            }
        }
        GrowOnlyMessage::Read { key: None } => {
            let _ = output.lock().unwrap().reply(
                input.clone(),
                GrowOnlyMessage::ReadOk {
                    value: state.lock().unwrap().operations.values().sum::<usize>(),
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
