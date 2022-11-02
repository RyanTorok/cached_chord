use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use crate::chord::FindResult;
use crate::{Address, ContentId, NodeId, Value};
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ChordMessage {
    pub src: (NodeId, Address),
    pub dest: Address,
    pub content: MessageContent
}

impl ChordMessage {
    pub fn new(src: (NodeId, Address), dest: Address, content: MessageContent) -> ChordMessage {
        ChordMessage { src, dest, content }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MessageContent {
    NewRequest(ContentId, FutureValue), // Represents a new request given by the user
    Find(ContentId),
    FindResponse(ContentId, FindResult),
    JoinToMaster,
    JoinToMasterResponse(NodeId, Address), // Returns successor
    JoinToSuccessor,
//    JoinToSuccessorAck,
    SuccessorHeartbeat,
    SuccessorHeartbeatAck,
    SuccessorHeartbeatNewSuccessor(NodeId, Address),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FutureValue {
    val: Option<Value>
}

impl FutureValue {

    pub fn new() -> FutureValue {
        FutureValue {
            val: None
        }
    }

    pub fn complete(&mut self, v: Value) {
        self.val = Some(v);
    }
}

impl Future for FutureValue {
    type Output = Value;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.val {
            None => Poll::Pending,
            Some(v) => Poll::Ready(v)
        }
    }
}
