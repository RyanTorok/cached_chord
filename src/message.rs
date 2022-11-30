use crate::node::FindResult;
use crate::{Address, ContentId, NodeId, RequestId, Value};
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
    ClientRequest(RequestId, ContentId, ClientOperation), // Represents a new request given by the client.
    Find(ContentId),
    FindResponse(ContentId, FindResult),
    PutValue(ContentId, Value),
    JoinToMaster,
    JoinToMasterResponse(NodeId, Address), // Returns successor
    JoinToSuccessor,
    JoinToSuccessorAck((NodeId, Address)),
    SuccessorHeartbeat,
    SuccessorHeartbeatAck,
    SuccessorHeartbeatNewSuccessor(NodeId, Address),
    HeartbeatTimerExpired,
    FixFingerTimerExpired(NodeId),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ClientOperation {
    Get(RequestId),
    Put(Value)
}