use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
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

struct BroadcastState {
    messages: Log<usize>,
}

impl Initable for BroadcastState {
    fn with_init(init: Init) -> Self {
        Self {
            messages: Log::with_init(init),
        }
    }
}

impl Deref for BroadcastState {
    type Target = Log<usize>;

    fn deref(&self) -> &Self::Target {
        &self.messages
    }
}
impl DerefMut for BroadcastState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.messages
    }
}

impl Store<usize> for BroadcastState {
    fn new_value(&mut self, new: &usize) {
        tracing::info!(new = %new, "received");
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
    let state = init_parser::<BroadcastState, StdOutWriter>(tx.subscribe(), output.clone()).await?;

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

    let _ = wait_for_message_then(&mut rx, |msg| handle_message(msg, &output, &state)).await;
    Ok(())
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum BroadcastMessage {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

fn handle_message(
    input: Message<BroadcastMessage>,
    output: &Arc<Mutex<Sender<StdOutWriter>>>,
    state: &Arc<Mutex<InitState<BroadcastState>>>,
) -> Result<()> {
    match input.body.msg_type {
        BroadcastMessage::Broadcast { message } => {
            let _ = output
                .lock()
                .unwrap()
                .reply(input.clone(), BroadcastMessage::BroadcastOk);

            let mut state = state.lock().unwrap();
            if let Some(val) = state.messages.insert(&message) {
                state.new_value(val);
            }
        }
        BroadcastMessage::Read => {
            let messages = state.lock().unwrap().messages.values().copied().collect();
            let _ = output
                .lock()
                .unwrap()
                .reply(input, BroadcastMessage::ReadOk { messages });
        }
        BroadcastMessage::Topology { ref topology } => {
            let _ = output
                .lock()
                .unwrap()
                .reply(input.clone(), BroadcastMessage::TopologyOk);

            let mut state = state.lock().unwrap();
            let node = state.get_init().node_id.clone();
            topology
                .get(&node)
                .expect("We are a node")
                .clone()
                .iter()
                .for_each(|n| {
                    state.update_neighbor(n);
                });
        }
        BroadcastMessage::ReadOk { messages: _ }
        | BroadcastMessage::TopologyOk
        | BroadcastMessage::BroadcastOk => {
            bail!("This is not expected");
        }
    };
    Ok(())
}
