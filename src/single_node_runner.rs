
use std::collections::hash_map::Entry;
use std::collections::{HashMap};
use std::collections::btree_map::BTreeMap;
use tokio::sync::mpsc;
use crate::{Address, CacheType, ContentId, Distribution, MASTER_NODE, NodeId, RequestId};
use crate::chord::{FindResult, Node};
use crate::message::{ChordMessage, FutureValue, MessageContent};

pub const BUFFER_SIZE: usize = 128;

pub struct SingleNodeRunner {
    node: Node,
    request_map: HashMap<ContentId, Vec<FutureValue>>,
    // Used only for Master
    successor_table: BTreeMap<NodeId, (Address, (NodeId, Address))>,
    incoming: mpsc::Receiver<ChordMessage>,
    outgoing: mpsc::Sender<ChordMessage>,
    requests: u64,
    distribution: Distribution,
    zipf_param: f64,
}

impl SingleNodeRunner {
    pub fn new(n: u32, cache: CacheType, cache_size: usize, requests: RequestId, distribution: Distribution, zipf_param: f64) -> SingleNodeRunner {
        let ip = Default::default(); // TODO
        let (outgoing, incoming) = mpsc::channel(BUFFER_SIZE);
        SingleNodeRunner {
            node: Node::new(n, ip),
            request_map: HashMap::new(),
            successor_table: BTreeMap::new(),
            incoming,
            outgoing,
            requests,
            distribution,
            zipf_param,
        }
    }
}

#[tokio::main]
pub async fn run_single_node(r: SingleNodeRunner) {
    run_single_node_inner(r).await
}

pub async fn run_single_node_inner(mut r: SingleNodeRunner) {
    let r = &mut r;
    loop {
        match r.incoming.recv().await {
            None => break,
            Some(msg) => {
                match msg.content {
                    MessageContent::NewRequest(key, out) => {
                        match r.request_map.entry(key) {
                            Entry::Occupied(mut e) => {
                                // Add another future waiting for this key
                                e.get_mut().push(out);
                            }
                            Entry::Vacant(e) => {
                                // Nobody waiting for this key yet. Add a new hash entry
                                e.insert(vec![out]);
                            }
                        }
                        match r.node.next_finger(key) {
                            FindResult::Value(val) => {
                                // Complete all futures waiting on that key
                                if let Some(futures) = r.request_map.remove(&key) {
                                    futures.into_iter().for_each(|mut f| f.complete(val));
                                }
                            }
                            FindResult::Redirect(next) => {
                                if let Err(e) = r.outgoing.send(ChordMessage::new(r.node.msg_id(), next, MessageContent::Find(key))).await {
                                    eprintln!("Error: Could not send Find message because of error '{}'", e);
                                }
                            }
                            FindResult::Error(e) => {
                                eprintln!("Find error (Node = {}, Requested key = (node: {}, stub: {})): {}", r.node.id(), key.0, key.1, e);
                            }
                        }
                    },
                    MessageContent::Find(key) => {
                        // Find the key (returns either the value or a redirect address), and send that back
                        if let Err(e) = r.outgoing.send(ChordMessage::new(r.node.msg_id(), msg.src.1, MessageContent::FindResponse(key, r.node.next_finger(key)))).await {
                            eprintln!("Error: Could not send FindResponse message because of error '{}'", e);
                        }
                    },
                    MessageContent::FindResponse(key, result) => {
                        match result {
                            FindResult::Value(val) => {
                                // Found it! Respond to the request by placing the value in the future.
                                if let Some(futures) = r.request_map.remove(&key) {
                                    futures.into_iter().for_each(|mut f| f.complete(val));
                                }
                            }
                            FindResult::Redirect(next) => {
                                if let Err(e) = r.outgoing.send(ChordMessage::new(r.node.msg_id(), next, MessageContent::Find(key))).await {
                                    eprintln!("Error: Could not send Find message because of error '{}'", e);
                                }
                            }
                            FindResult::Error(e) => {
                                eprintln!("Find error (Node = {}, Requested key = (node: {}, stub: {})): {}", r.node.id(), key.0, key.1, e);
                            }
                        }
                    },
                    MessageContent::JoinToMaster => {
                        if r.node.id() != MASTER_NODE {
                            eprintln!("Error: Non master node (node {}) received JoinToMaster message. The master node is {}.", r.node.id(), MASTER_NODE)
                        } else {
                            let new_node_id = msg.src.0;

                            // Add new node's successor
                            let (s_node, s_addr) = match r.successor_table.iter().find(|(&i_node, _)| i_node > new_node_id) {
                                None => {
                                    // No larger key. The next key is the lowest key in the ring.
                                    // Unwrap is safe here because there must be at least one key (the master must exist if it's
                                    // responding to this request, and so must the requesting node).
                                    let min_entry = r.successor_table.iter().min().unwrap();
                                    (*min_entry.0, min_entry.1.0)
                                }
                                Some((s_node, (s_addr, ..))) => (*s_node, *s_addr)
                            };
                            r.successor_table.insert(msg.src.0, (msg.src.1, (s_node, s_addr)));

                            // Add previous node's successor
                            let (p_node, p_addr) = match r.successor_table.iter().rev().find(|(&i_node, _)| i_node < new_node_id) {
                                None => {
                                    // No smaller key. The predecessor is the largest key in the ring.
                                    let max_entry = r.successor_table.iter().max().unwrap();
                                    (*max_entry.0, max_entry.1.0)
                                }
                                Some((p_node, (p_addr, ..))) => (*p_node, *p_addr)
                            };
                            r.successor_table.insert(p_node, (p_addr, msg.src));

                            // Tell the sender who its successor is
                            if let Err(e) = r.outgoing.send(ChordMessage::new(r.node.msg_id(), msg.src.1, MessageContent::JoinToMasterResponse(s_node, s_addr))).await {
                                eprintln!("Error: Could not send JoinToMasterResponse message because of error '{}'", e);
                            }
                        }
                    },
                    MessageContent::JoinToMasterResponse(s_node, s_addr) => {
                        // Mark the new node as our successor
                        r.node.add_successor((s_node, s_addr));
                        // Tell new successor about us
                        if let Err(e) = r.outgoing.send(ChordMessage::new(r.node.msg_id(), s_addr, MessageContent::JoinToSuccessor)).await {
                            eprintln!("Error: Could not send JoinToSuccessor message because of error '{}'", e);
                        }
                    }
                    MessageContent::JoinToSuccessor => {
                        // We have a new predecessor
                        let old_p = r.node.predecessor();
                        r.node.set_predecessor(msg.src);


                    },
                    MessageContent::SuccessorHeartbeat => {
                        // Check if we have a later predecessor than the node that thinks we're
                        // its successor
                        match r.node.predecessor() {
                            Some((pre_node, pre_addr)) => {
                                if pre_node != msg.src.0 { // Could go up or down if nodes join/leave the network
                                    // Update the sender to the new in-between node
                                    if let Err(e) = r.outgoing.send(ChordMessage::new(r.node.msg_id(), msg.src.1, MessageContent::SuccessorHeartbeatNewSuccessor(pre_node, pre_addr))).await {
                                        eprintln!("Error: Could not send SuccessorHeartbeatNewSuccessor message because of error '{}'", e);
                                    }
                                } else {
                                    // Still the same predecessor
                                    if let Err(e) = r.outgoing.send(ChordMessage::new(r.node.msg_id(), msg.src.1, MessageContent::SuccessorHeartbeatAck)).await {
                                        eprintln!("Error: Could not send SuccessorHeartbeatAck message because of error '{}'", e);
                                    }
                                }
                            }
                            None => {
                                eprintln!("Error: Node should not have no predecessor if it receives a heartbeat message from a current or former predecessor.");
                            }
                        };
                    },
                    MessageContent::SuccessorHeartbeatAck => {}, // This is a No-op
                    MessageContent::SuccessorHeartbeatNewSuccessor(s_node, s_addr) => {
                        r.node.add_successor((s_node, s_addr))
                    }
                }
            }
        }
    }
}