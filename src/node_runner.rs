
use std::collections::hash_map::Entry;
use std::collections::{HashMap};
use std::collections::btree_map::BTreeMap;
use std::mem::size_of;
use std::time::Duration;
use rand::Rng;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::Receiver;
use crate::{Address, CacheType, ContentId, ContentStub, Distribution, MASTER_NODE, NodeId, RequestId, Value};
use crate::node::{FindResult, Node};
use crate::message::{ChordMessage, ClientOperation, FutureValue, MessageContent};
use rand::distributions::Distribution as RngDistribution;

pub const BUFFER_SIZE: usize = 128;

pub struct SingleNodeRunner {
    node: Node,
    keys: u64,
    gets_in_transit: HashMap<ContentId, Vec<(FutureValue, u64)>>,
    puts_in_transit: HashMap<ContentId, (Value, u64)>,
    // Used only for Master
    successor_table: BTreeMap<NodeId, (Address, (NodeId, Address))>,
    incoming: Receiver<ChordMessage>,
    outgoing: Sender<ChordMessage>,
    requests: u64,
    distribution: Distribution,
    zipf_param: f64,
    op_count: u64
}

impl SingleNodeRunner {
    pub fn new(n: u32, address: Address, keys: u64, cache: CacheType, cache_size: usize, requests: RequestId, distribution: Distribution, zipf_param: f64) -> (SingleNodeRunner, Sender<ChordMessage>, Receiver<ChordMessage>) {
        let (in_tx, incoming) = mpsc::channel(BUFFER_SIZE);
        let (outgoing, out_rx) = mpsc::channel(BUFFER_SIZE);
        let mut node = Node::new(n, address);
        node.init_cache(cache, cache_size);
        (SingleNodeRunner {
            node,
            keys,
            gets_in_transit: HashMap::new(),
            puts_in_transit: HashMap::new(),
            successor_table: BTreeMap::new(),
            incoming,
            outgoing,
            requests,
            distribution,
            zipf_param,
            op_count: 0u64
        }, in_tx, out_rx)
    }
}

fn split_u64(n: u64) -> (NodeId, ContentStub) {
    let bytes = u64::to_be_bytes(n);
    let mut node_id = [0u8; 4];
    for i in 0..4 {
        node_id[i] = bytes[i];
    }
    let mut node_offset = [0u8; 4];
    for i in 0..4 {
        node_offset[i] = bytes[i + 4];
    }
    (u32::from_be_bytes(node_id), u32::from_be_bytes(node_offset))
}

// Submits requests for keys to a single nade according to the given distribution
pub async fn run_requests(requests: u64, keys: u64, node_addr: Address, tx: mpsc::Sender<ChordMessage>, dist: Distribution, zipf_param: f64) {
    for request_id in 0..requests {
        let sink = FutureValue::new();
        tx.send(ChordMessage::new((u32::MAX, u32::MAX),node_addr, MessageContent::ClientRequest(
            match dist {
                Distribution::Uniform => {
                    // ThreadRng is not `Send` because it relies on thread-specific mechanics,
                    // so we need a new handle each time so no ThreadRng instance crosses an `await`.
                    let mut rng = rand::thread_rng();
                    split_u64(rng.gen_range(0..keys))
                }
                Distribution::Zipf => {
                    let mut rng = rand::thread_rng();
                    let zipf = zipf::ZipfDistribution::new(keys.try_into().expect("Number of keys too large for CPU registers."), zipf_param).expect("Error setting up Zipf distribution.");
                    split_u64(zipf.sample(&mut rng).try_into().expect("Zipf sample too large for a u64."))
                }
            },
            ClientOperation::Get(sink)
        ))).await // Await because we don't want to send the next request until this one actually gets added to the send queue.
            .expect("Tokio send error when sending the request.")
    }
}

pub async fn send_heartbeat_triggers(inbox: Sender<ChordMessage>, interval: Duration) {
    let mut timer = tokio::time::interval(interval);
    loop {
        timer.tick().await;
        // Don't need to fill src/dest information because this message is internal.
        inbox.send(ChordMessage::new((u32::MAX, u32::MAX), u32::MAX, MessageContent::HeartbeatTimerExpired)).await
            .expect("Error: Could not send HeartbeatTimerExpired message to inbox.");
    }
}

pub async fn run_node(mut r: SingleNodeRunner) {
    tokio::spawn(run_requests(r.requests, r.keys, r.node.address(), r.outgoing.clone(), r.distribution, r.zipf_param));
    if r.node.id() == MASTER_NODE {
        // We're the master node. That means we start first (by assumption), making us our own
        // predecessor and successor.
        r.node.set_predecessor((MASTER_NODE, r.node.address()));
        r.node.add_successor((MASTER_NODE, r.node.address()));
        r.node.activate();
    } else {
        // We're a normal node. We need to populate finger table by asking our successor to find
        // the location of exponentially increasing keys.
    }
    let r = &mut r;
    loop {
        match r.incoming.recv().await {
            None => break,
            Some(msg) => {
                match msg.content {
                    MessageContent::ClientRequest(key, op) => {
                        if r.node.inactive() {
                            let src = r.node.msg_id();
                            if let Err(e) = r.outgoing.send(ChordMessage::new(src, msg.src.1, MessageContent::FindResponse(key, FindResult::Error(String::from("Node is not active yet."))))).await {
                                eprintln!("Error: Could not send inactive error for FindResponse message because of error '{}'", e);
                            }
                        } else {
                            let opt_put_val = match &op {
                                ClientOperation::Put(put_val) => Some(*put_val),
                                _ => None
                            };
                            match op {
                                ClientOperation::Get(out) => {
                                    match r.gets_in_transit.entry(key) {
                                        Entry::Occupied(mut e) => {
                                            // Add another future waiting for this key
                                            e.get_mut().push((out, r.op_count));
                                        }
                                        Entry::Vacant(e) => {
                                            // Nobody waiting for this key yet. Add a new hash entry
                                            e.insert(vec![(out, r.op_count)]);
                                        }
                                    }
                                }
                                ClientOperation::Put(val) => {
                                    r.puts_in_transit.insert(key, (val, r.op_count));
                                }
                            }
                            match r.node.next_finger(key) {
                                FindResult::Value(val, _key_range) => {
                                    // Complete all futures waiting on that key

                                    // The existing reads can't have a higher sequence number because we never awaited
                                    // since the write was received.
                                    if let Some(futures) = r.gets_in_transit.remove(&key) {
                                        futures.into_iter().for_each(|(mut f, _seq)| f.complete(val));
                                    }

                                    // If we get here, we own the key, so we can just set the entry.
                                    if let Some(put_val) = opt_put_val {
                                        r.node.set(key, put_val);
                                    }
                                }
                                FindResult::Redirect(next) => {
                                    let src = r.node.msg_id();
                                    if let Err(e) = r.outgoing.send(ChordMessage::new(src, next, MessageContent::Find(key))).await {
                                        eprintln!("Error: Could not send Find message because of error '{}'", e);
                                    }
                                }
                                FindResult::Error(e) => {
                                    eprintln!("Find error (Node = {}, Requested key = (node: {}, stub: {})): {}", r.node.id(), key.0, key.1, e);
                                }
                            }
                        }
                    },
                    MessageContent::Find(key) => {
                        let src = r.node.msg_id();
                        if r.node.inactive() {
                            if let Err(e) = r.outgoing.send(ChordMessage::new(src, msg.src.1, MessageContent::FindResponse(key, FindResult::Error(String::from("Node is not active yet."))))).await {
                                eprintln!("Error: Could not send inactive error for FindResponse message because of error '{}'", e);
                            }
                        } else {
                            // Find the key (returns either the value or a redirect address), and send that back
                            if let Err(e) = r.outgoing.send(ChordMessage::new(src, msg.src.1, MessageContent::FindResponse(key, r.node.next_finger(key)))).await {
                                eprintln!("Error: Could not send FindResponse message because of error '{}'", e);
                            }
                        }
                    },
                    MessageContent::FindResponse(key, result) => {
                        match result {
                            FindResult::Value(get_val, key_range) => {
                                if r.node.inactive() {
                                    // If we get a response at this point, it's a request we issued internally
                                    // to populate our finger table. In that case, the value is
                                    // worthless to us; we only care about which node serviced the key.
                                    let size_so_far = r.node.populate_finger(key.0, msg.src.0, msg.src.1, key_range);
                                    if size_so_far >= size_of::<NodeId>() / 2 {
                                        // We've fully populated our table. Our node is ready for prime time!
                                        r.node.activate();
                                    }
                                }
                                let put_in_transit = r.puts_in_transit.get(&key);
                                // Found it! Respond to the request by placing the value in the future.
                                if let Some(futures) = r.gets_in_transit.remove(&key) {
                                    futures.into_iter().for_each(|(mut f, get_seq_num)| {
                                        match put_in_transit {
                                            None => {
                                                f.complete(get_val)
                                            }
                                            Some((put_val, put_seq_num)) => {
                                                if *put_seq_num > get_seq_num {
                                                    f.complete(*put_val)
                                                } else {
                                                    f.complete(get_val)
                                                }
                                            }
                                        }
                                    });
                                }
                                if let Some((put_val, _)) = put_in_transit {
                                    // If we have a put in transit, send it to the node we found.
                                    let src = r.node.msg_id();
                                    if let Err(e) = r.outgoing.send(ChordMessage::new(src, msg.src.1, MessageContent::PutValue(key, *put_val))).await {
                                        eprintln!("Error: Could not send PutValue message because of error '{}'", e);
                                    }
                                }
                                // Put the key into our cache
                                r.node.cache_key(key, msg.src.1)
                            }
                            FindResult::Redirect(next) => {
                                let src = r.node.msg_id();
                                if let Err(e) = r.outgoing.send(ChordMessage::new(src, next, MessageContent::Find(key))).await {
                                    eprintln!("Error: Could not send Find message because of error '{}'", e);
                                }
                            }
                            FindResult::Error(e) => {
                                eprintln!("Find error (Node = {}, Requested key = (node: {}, stub: {})): {}", r.node.id(), key.0, key.1, e);
                            }
                        }
                    },
                    MessageContent::PutValue(key, value) => {
                        r.node.set(key, value);
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
                            let src = r.node.msg_id();
                            if let Err(e) = r.outgoing.send(ChordMessage::new(src, msg.src.1, MessageContent::JoinToMasterResponse(s_node, s_addr))).await {
                                eprintln!("Error: Could not send JoinToMasterResponse message because of error '{}'", e);
                            }
                        }
                    },
                    MessageContent::JoinToMasterResponse(s_node, s_addr) => {
                        // Mark the new node as our successor
                        r.node.add_successor((s_node, s_addr));
                        // Tell new successor about us
                        let src = r.node.msg_id();
                        if let Err(e) = r.outgoing.send(ChordMessage::new(src, s_addr, MessageContent::JoinToSuccessor)).await {
                            eprintln!("Error: Could not send JoinToSuccessor message because of error '{}'", e);
                        }
                        // Then bug the successor until it tells us enough keys to populate the finger table.
                        let mut curr: NodeId = 1;
                        let src = r.node.msg_id();
                        loop {
                            // Ask our successor about the location of several keys.
                            // We can just set the content stub to 0.
                            let dest = r.node.nth_successor(0).1;
                            // We want this to overflow, e.g. the successor of node 2^32 - 1 is node 0.
                            let next_node = r.node.id().wrapping_add(curr);
                            if let Err(e) = r.outgoing.send(ChordMessage::new(src, dest, MessageContent::Find((next_node, 0)))).await {
                                eprintln!("Error: Could not send inactive error for FindResponse message because of error '{}'", e);
                            }
                            // Can't use normal while condition here because we'd overflow before we failed the condition.
                            if curr >= NodeId::MAX / 2 {
                                break;
                            }
                            curr *= 2;
                        }
                    }
                    MessageContent::JoinToSuccessor => {
                        // We have a new predecessor
                        let old_p = r.node.predecessor();
                        r.node.set_predecessor(msg.src);
                        // Reply to the new node with the keys it should be responsible for
                        let send_keys = r.node.drain_keys_before(msg.src.0);
                        let src = r.node.msg_id();
                        if let Err(e) = r.outgoing.send(ChordMessage::new(src, msg.src.1, MessageContent::JoinToSuccessorAck(send_keys))).await {
                            eprintln!("Error: Could not send SuccessorHeartbeatNewSuccessor message because of error '{}'", e);
                        }
                    },
                    MessageContent::JoinToSuccessorAck(keys) => {
                        keys.into_iter().for_each(|(k, v) | r.node.set(k, v));

                    },
                    MessageContent::SuccessorHeartbeat => {
                        // Check if we have a later predecessor than the node that thinks we're
                        // its successor
                        let opt_predecessor = r.node.predecessor();
                        match opt_predecessor {
                            Some((pre_node, pre_addr)) => {
                                if pre_node != msg.src.0 { // Could go up or down if nodes join/leave the network
                                    // Update the sender to the new in-between node
                                    let src = r.node.msg_id();
                                    if let Err(e) = r.outgoing.send(ChordMessage::new(src, msg.src.1, MessageContent::SuccessorHeartbeatNewSuccessor(pre_node, pre_addr))).await {
                                        eprintln!("Error: Could not send SuccessorHeartbeatNewSuccessor message because of error '{}'", e);
                                    }
                                } else {
                                    // Still the same predecessor
                                    let src = r.node.msg_id();
                                    if let Err(e) = r.outgoing.send(ChordMessage::new(src, msg.src.1, MessageContent::SuccessorHeartbeatAck)).await {
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
                    MessageContent::HeartbeatTimerExpired => {
                        match r.successor_table.iter().next() {
                            None => {}
                            Some((_my_node, (_my_addr, (s_node, s_addr)))) => {
                                let src = r.node.msg_id();
                                if let Err(e) = r.outgoing.send(ChordMessage::new(src, *s_addr, MessageContent::SuccessorHeartbeat)).await {
                                    eprintln!("Error: Could not send SuccessorHeartbeatAck message because of error '{}'", e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}