pub mod gossip;
pub mod init_state;
pub mod log;
pub mod message;
pub mod sender;
pub mod stdout_writer;
pub mod traits;

use anyhow::Result;
use message::Message;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast::{error::RecvError, Receiver, Sender};

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
pub fn init_stdin(tx: Sender<Value>) {
    tokio::task::spawn_blocking(move || {
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

            if tx.send(input).is_err() {
                break;
            }
        }
    });
}

/// # Errors
///
/// - When stream is closed
pub async fn wait_for_message_then<T, F>(rx: &mut Receiver<Value>, callable: F) -> Result<()>
where
    T: DeserializeOwned,
    F: Fn(Message<T>) -> Result<()>,
{
    loop {
        match rx.recv().await {
            Ok(value) => {
                let input: Message<T> = match serde_json::from_value(value) {
                    Ok(msg) => msg,
                    Err(_) => {
                        continue;
                    }
                };

                callable(input)?;
            }
            Err(RecvError::Closed) => {
                break Err(RecvError::Closed.into());
            }
            Err(_) => continue,
        }
    }
}
