use crate::init_state::InitState;
use crate::message::{Body, Message};
use crate::sender::Sender;
use crate::traits::store::Store;
use crate::{wait_for_message_then, Initable, Messages};

use anyhow::Result;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::broadcast::Receiver as BroadcastReceiver;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum GossipMessages<T: Serialize + Clone + Eq> {
    Gossip { seen: HashMap<String, T> },
    GossipOk { seen: HashMap<String, T> },
}

struct GossipState<T> {
    known: HashMap<String, HashMap<String, T>>,
}

impl<T> GossipState<T> {
    fn from_state<V: Initable>(state: &InitState<V>) -> Self {
        Self {
            known: state
                .get_init()
                .node_ids
                .iter()
                .map(|node| (node.clone(), HashMap::new()))
                .collect(),
        }
    }
}

fn handle_msg<T, StoreImpl, W>(
    input: &Message<GossipMessages<T>>,
    output: &Arc<Mutex<Sender<W>>>,
    state: &Arc<Mutex<InitState<StoreImpl>>>,
    known_ctx: &Arc<Mutex<GossipState<T>>>,
) -> Result<()>
where
    W: Write,
    T: Clone + Serialize + Eq + DeserializeOwned + Debug + Send + 'static,
    StoreImpl: Store<T> + Send + Initable + 'static,
{
    match input.body.msg_type {
        GossipMessages::Gossip { ref seen } => {
            let mut state = state.lock().unwrap();
            let known_by_me = state.get_inner().get_log().clone();
            seen.clone()
                .into_iter()
                .filter(|(key, _)| !known_by_me.contains_key(key))
                .for_each(|(key, val)| {
                    state
                        .get_inner_mut()
                        .get_log_mut()
                        .insert_with_key(&key, val);
                });
            std::mem::drop(state);
            let mut known_ctx = known_ctx.lock().unwrap();
            if !known_ctx.known.contains_key(&input.src) {
                known_ctx.known.insert(input.src.clone(), HashMap::new());
            }
            known_ctx
                .known
                .get_mut(&input.src)
                .expect("got gossip")
                .extend(seen.clone());
            let mut ack_messages: HashMap<String, _> = known_by_me
                .clone()
                .into_iter()
                .filter(|(x, _)| !known_ctx.known[&input.src].contains_key(x))
                .collect();
            std::mem::drop(known_ctx);
            ack_messages.extend(seen.clone());
            let _ = output.lock().unwrap().reply(
                input.clone(),
                GossipMessages::GossipOk { seen: ack_messages },
            );
        }
        GossipMessages::GossipOk { ref seen } => {
            let mut state = state.lock().unwrap();
            let known_by_me = state.get_inner().get_log().clone();
            seen.clone()
                .into_iter()
                .filter(|(key, _)| !known_by_me.contains_key(key))
                .for_each(|(key, val)| {
                    state
                        .get_inner_mut()
                        .get_log_mut()
                        .insert_with_key(&key, val);
                });
            std::mem::drop(state);
            let mut known_ctx = known_ctx.lock().unwrap();
            if !known_ctx.known.contains_key(&input.src) {
                known_ctx.known.insert(input.src.clone(), HashMap::new());
            }
            known_ctx
                .known
                .get_mut(&input.src)
                .expect("got gossipok")
                .extend(seen.clone());
        }
    };
    Ok(())
}

fn gossip<T, StoreImpl, W>(
    output: &Arc<Mutex<Sender<W>>>,
    state: &Arc<Mutex<InitState<StoreImpl>>>,
    known_ctx: &Arc<Mutex<GossipState<T>>>,
) where
    W: Write,
    T: Clone + Serialize + Eq + DeserializeOwned + Debug + Send + 'static,
    StoreImpl: Store<T> + Send + Initable + 'static,
{
    let state = state.lock().unwrap();
    let node = &state.get_init().node_id;
    let known_by_me = state.get_inner().get_log();
    for n in state.get_neighbors() {
        let mut known_ctx = known_ctx.lock().unwrap();
        if !known_ctx.known.contains_key(n) {
            known_ctx.known.insert(n.clone(), HashMap::new());
        }
        let seen: HashMap<_, _> = known_by_me
            .clone()
            .into_iter()
            .filter(|(ref x, _)| !known_ctx.known[n].contains_key(x))
            .collect();

        std::mem::drop(known_ctx);

        if seen.is_empty() {
            tracing::trace!("Nothing to gossip");
            continue;
        }

        let _ = output.lock().unwrap().send(
            Message {
                src: node.clone(),
                dest: n.clone(),
                body: Body {
                    msg_id: None,
                    in_reply_to: None,
                    msg_type: GossipMessages::Gossip { seen },
                },
            },
            true,
        );
    }
}

/// # Panics
///
/// - if locks are poisoned
pub async fn handle<T, StoreImpl, W>(
    mut rx: BroadcastReceiver<Messages>,
    output: Arc<Mutex<Sender<W>>>,
    state: Arc<Mutex<InitState<StoreImpl>>>,
    gossip_periodicity: Duration,
) where
    W: Write,
    T: Clone + Serialize + Eq + DeserializeOwned + Debug + Send + 'static,
    StoreImpl: Store<T> + Send + Initable + 'static,
{
    let known_ctx = Arc::new(Mutex::new(GossipState::from_state(&state.lock().unwrap())));
    loop {
        tokio::select! {
            result = wait_for_message_then(&mut rx, |msg| handle_msg(&msg, &output, &state, &known_ctx)) => {
                match result {
                    Ok(()) => {continue;},
                    Err(_) => {break;}
                }
            }
            () = tokio::time::sleep(gossip_periodicity) => {
                gossip(&output, &state, &known_ctx);
            }
        }
    }
}
