use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Body<Type> {
    pub msg_id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub msg_type: Type,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message<Type> {
    pub src: String,
    pub dest: String,
    pub body: Body<Type>,
}
