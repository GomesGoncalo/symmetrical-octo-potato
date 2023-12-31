use crate::message::{Body, Message};
use anyhow::{bail, Context, Result};
use serde::Serialize;
use std::io::Write;

#[derive(Default)]
pub struct Sender<W: Write> {
    id: usize,
    writer: W,
}

impl<W: Write> Sender<W> {
    /// # Errors
    ///
    /// - failed to `send()`
    pub fn reply<T: Serialize>(&mut self, request: Message<T>, msg_type: T) -> Result<()> {
        let Some(in_reply_to) = request.body.msg_id else {
            bail!("not possible to reply");
        };
        self.send(
            Message {
                src: request.dest,
                dest: request.src,
                body: Body {
                    msg_id: None,
                    in_reply_to: Some(in_reply_to),
                    msg_type,
                },
            },
            true,
        )
    }

    /// # Errors
    ///
    /// - failed to serialize structure
    /// - failed to write and/or flush
    pub fn send<T: Serialize>(&mut self, mut message: Message<T>, include_id: bool) -> Result<()> {
        if include_id {
            message.body.msg_id = Some(self.id);
        }
        serde_json::to_writer(&mut self.writer, &message).context("Failed to serialize / write")?;
        self.writer.flush().context("Failed to flush")?;
        self.id += 1;
        Ok(())
    }

    #[must_use]
    pub fn get_id(&self) -> usize {
        self.id
    }
}
