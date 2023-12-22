use anyhow::Result;
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

struct KafkaState {
    operations: Log<HashMap<String, usize>>,
}

impl Store<HashMap<String, usize>> for KafkaState {
    fn new_value(&mut self, new: &HashMap<String, usize>) {
        tracing::info!(new = ?new, "received");
    }
}

impl Initable for KafkaState {
    fn with_init(init: Init) -> Self {
        Self {
            operations: Log::with_init(init),
        }
    }
}

impl Deref for KafkaState {
    type Target = Log<HashMap<String, usize>>;

    fn deref(&self) -> &Self::Target {
        &self.operations
    }
}
impl DerefMut for KafkaState {
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
    let state = init_parser::<KafkaState, StdOutWriter>(tx.subscribe(), output.clone()).await?;
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

    let _ = wait_for_message_then(&mut rx, |msg| {
        handle_message(&msg, &output, &state);
        Ok(())
    })
    .await;
    Ok(())
}

#[derive(Serialize, Deserialize, Clone)]
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
) {
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
}
