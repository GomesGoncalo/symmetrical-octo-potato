use crate::{message::Message, sender::Sender};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashSet,
    io::Write,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};
use tokio::sync::broadcast::Receiver;

pub struct InitState<T: Initable> {
    init: Init,
    state: T,
    neighborhood: HashSet<String>,
}

impl<T: Initable> InitState<T> {
    #[must_use]
    pub fn from_init(init: Init) -> Self {
        Self {
            init: init.clone(),
            state: T::with_init(init.clone()),
            neighborhood: init.node_ids,
        }
    }

    pub fn get_neighbors(&self) -> &HashSet<String> {
        &self.neighborhood
    }

    pub fn update_neighbor(&mut self, val: &str) {
        self.neighborhood.insert(val.to_string());
    }

    pub fn get_init(&self) -> &Init {
        &self.init
    }
}

impl<T: Initable> Deref for InitState<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<T: Initable> DerefMut for InitState<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Init {
    pub node_id: String,
    pub node_ids: HashSet<String>,
}

pub trait Initable {
    fn with_init(init: Init) -> Self;
}

#[derive(Serialize, Deserialize, Clone)]
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
    mut rx: Receiver<Value>,
    output: Arc<Mutex<Sender<W>>>,
) -> Result<Arc<Mutex<InitState<StateImpl>>>>
where
    W: Write,
    StateImpl: Initable + Send + 'static,
{
    let value = rx.recv().await.context("Error receiving")?;
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

    Ok(Arc::new(Mutex::new(InitState::<StateImpl>::from_init(
        init.clone(),
    ))))
}
