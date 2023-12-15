pub mod gossip;
pub mod init_state;
pub mod log;
pub mod message;
pub mod sender;
pub mod stdout_writer;
pub mod traits;

use std::{
    collections::HashSet,
    io::Write,
    sync::{Arc, Mutex},
};

use anyhow::{bail, Context, Result};
use init_state::InitState;
use message::Message;
use sender::Sender;
use serde_json::Value;

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::Receiver as BroadcastReceiver;
use tokio::sync::broadcast::Sender as BroadcastSender;

#[derive(Clone, Debug)]
pub enum Messages {
    Stdin(Value),
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Init {
    pub node_id: String,
    pub node_ids: HashSet<String>,
}

pub trait Initable {
    fn with_init(init: Init) -> Self;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitMessage {
    Init(Init),
    InitOk,
}

/// # Panics
///
/// - if locks are poisoned
///
/// # Errors
///
/// - Did not receive Init
/// - Channel not receiving
/// - Error replying to Init
pub async fn init_parser<StateImpl, W>(
    mut rx: BroadcastReceiver<Messages>,
    output: Arc<Mutex<Sender<W>>>,
) -> Result<Arc<Mutex<InitState<StateImpl>>>>
where
    W: Write,
    StateImpl: Initable + Send + 'static,
{
    let Messages::Stdin(value) = rx.recv().await.context("Error receiving")?;
    let input =
        serde_json::from_value::<Message<InitMessage>>(value).context("Parsing init message")?;

    let InitMessage::Init(ref init) = input.body.msg_type else {
        bail!("Received a message that does not match Init (InitOk?)")
    };

    output
        .lock()
        .unwrap()
        .reply(input.clone(), InitMessage::InitOk)
        .context("Confirm init message")?;

    tracing::info!(init = ?init, "Got init message");
    Ok(Arc::new(Mutex::new(InitState::<StateImpl>::from_init(
        init.clone(),
    ))))
}

struct Wrapper<T: Iterator> {
    input: T,
}

impl<T: Iterator> Wrapper<T> {
    fn next(&mut self) -> Option<<T as Iterator>::Item> {
        self.input.next()
    }
}

unsafe impl<T: Iterator> Send for Wrapper<T> {}

/// # Panics
///
/// - if inner locks become poisoned
pub fn init_stdin(tx: BroadcastSender<Messages>) {
    tokio::spawn(async move {
        let stdin = std::io::stdin().lock();
        let inputs = Arc::new(Mutex::new(Wrapper {
            input: serde_json::Deserializer::from_reader(stdin).into_iter::<Value>(),
        }));

        loop {
            let input = match inputs.lock().unwrap().next() {
                Some(Ok(input)) => input,
                None => {
                    break;
                }
                _ => continue,
            };

            if tx.send(Messages::Stdin(input)).is_err() {
                break;
            }
        }
    });
}
