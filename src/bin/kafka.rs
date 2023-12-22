use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use symmetrical_octo_potato::init_state::InitState;
use symmetrical_octo_potato::log::Log;
use symmetrical_octo_potato::message::Message;
use symmetrical_octo_potato::sender::Sender;
use symmetrical_octo_potato::stdout_writer::StdOutWriter;
use symmetrical_octo_potato::traits::store::Store;
use symmetrical_octo_potato::{gossip, Initable};
use symmetrical_octo_potato::{wait_for_message_then, Messages};
use tokio::sync::broadcast::Receiver as BroadcastReceiver;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

#[derive(Clone)]
struct KafkaState {
    operations: Log<HashMap<String, usize>>,
}

impl Store<HashMap<String, usize>> for KafkaState {
    fn get_log(&self) -> &Log<HashMap<String, usize>> {
        &self.operations
    }

    fn get_log_mut(&mut self) -> &mut Log<HashMap<String, usize>> {
        &mut self.operations
    }
}

impl Initable for KafkaState {
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
    let state = symmetrical_octo_potato::init_parser::<KafkaState, StdOutWriter>(
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
            Err(_) = init_kafka(tx.subscribe(), output.clone(), state.clone()) => {break Ok(())}
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum KafkaMessage {
    Send {
        key: String,
        msg: usize,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PoolOk {
        msgs: HashMap<String, HashSet<(usize, usize)>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: HashSet<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
}

fn handle_message(
    input: &Message<KafkaMessage>,
    _output: &Arc<Mutex<Sender<StdOutWriter>>>,
    _state: &Arc<Mutex<InitState<KafkaState>>>,
) -> Result<()> {
    match &input.body.msg_type {
        KafkaMessage::Send { key, msg } => {
            tracing::info!(key, msg, "Received Send");
        }
        KafkaMessage::SendOk { offset } => {
            tracing::info!(offset, "Received SendOk");
        }
        KafkaMessage::Poll { offsets } => {
            tracing::info!(?offsets, "Received Poll");
        }
        KafkaMessage::PoolOk { msgs } => {
            tracing::info!(?msgs, "Received PollOk");
        }
        KafkaMessage::CommitOffsets { offsets } => {
            tracing::info!(?offsets, "Received CommitOffsets");
        }
        KafkaMessage::CommitOffsetsOk => {
            tracing::info!("Received CommitOffsetsOk");
        }
        KafkaMessage::ListCommittedOffsets { keys } => {
            tracing::info!(?keys, "Received ListCommittedOffsets");
        }
        KafkaMessage::ListCommittedOffsetsOk { offsets } => {
            tracing::info!(?offsets, "Received ListCommittedOffsetsOk");
        }
    };
    Ok(())
}

async fn init_kafka(
    mut rx: BroadcastReceiver<Messages>,
    output: Arc<Mutex<Sender<StdOutWriter>>>,
    state: Arc<Mutex<InitState<KafkaState>>>,
) -> Result<()> {
    wait_for_message_then(&mut rx, |msg| handle_message(&msg, &output, &state)).await
}
