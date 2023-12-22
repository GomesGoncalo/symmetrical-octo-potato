use std::collections::{HashMap, HashSet};
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
use symmetrical_octo_potato::{gossip, wait_for_message_then, Initable};
use symmetrical_octo_potato::{Init, Messages};
use tokio::sync::broadcast::Receiver as BroadcastReceiver;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

#[derive(Clone)]
struct BroadcastState {
    messages: Log<usize>,
}

impl Store<usize> for BroadcastState {
    fn get_log(&self) -> &Log<usize> {
        &self.messages
    }

    fn get_log_mut(&mut self) -> &mut Log<usize> {
        &mut self.messages
    }
}

impl Initable for BroadcastState {
    fn with_init(init: Init) -> Self {
        Self {
            messages: Log::with_init(init),
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
    let state = symmetrical_octo_potato::init_parser::<BroadcastState, StdOutWriter>(
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
            Err(_) = init_broadcast(tx.subscribe(), output.clone(), state.clone()) => {break Ok(())}
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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
            state.get_inner_mut().messages.insert(message);
        }
        BroadcastMessage::Read => {
            let messages = state
                .lock()
                .unwrap()
                .get_inner()
                .messages
                .values()
                .copied()
                .collect();
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

async fn init_broadcast(
    mut rx: BroadcastReceiver<Messages>,
    output: Arc<Mutex<Sender<StdOutWriter>>>,
    state: Arc<Mutex<InitState<BroadcastState>>>,
) -> Result<()> {
    wait_for_message_then(&mut rx, |msg| handle_message(msg, &output, &state)).await
}
