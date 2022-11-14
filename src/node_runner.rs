use std::collections::hash_map::Entry;
use std::collections::{HashMap};
use std::collections::btree_map::BTreeMap;
use std::mem::size_of;
use std::ops::Range;
use std::time::Duration;
use rand::Rng;
use tokio::sync::{mpsc, oneshot};
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::Receiver;
use crate::{Address, CacheType, ContentId, ContentStub, Distribution, MASTER_NODE, NodeId, RequestId, Value};
use crate::node::{between_mod_id, FindResult, Node};
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
    op_count: u64,
    master_ip: Address,
    verbose: bool,
    activation: Option<oneshot::Sender<()>>
}

impl SingleNodeRunner {
    pub fn new(
        n: u32,
        address: Address,
        keys: u64,
        cache: CacheType,
        cache_size: usize,
        master_ip: Address,
        requests: RequestId,
        distribution: Distribution,
        zipf_param: f64,
        verbose: bool,
    ) -> (SingleNodeRunner, Sender<ChordMessage>, Receiver<ChordMessage>, oneshot::Receiver<()>) {
        let (in_tx, incoming) = mpsc::channel(BUFFER_SIZE);
        let (outgoing, out_rx) = mpsc::channel(BUFFER_SIZE);
        let mut node = Node::new(n, address);
        node.init_cache(cache, cache_size);
        let (act_tx, act_rx) = oneshot::channel();
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
            op_count: 0u64,
            master_ip,
            verbose,
            activation: Some(act_tx)
        }, in_tx, out_rx, act_rx)
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

// Submits requests for keys to a single node according to the given distribution
pub async fn run_requests(requests: u64, node_addr: Address, tx: Sender<ChordMessage>, dist: Distribution, zipf_param: f64, activation: oneshot::Receiver<()>) {
    activation.await.expect("Receive error on activation oneshot() channel.");
    let keys = u64::MAX;
    for request_id in 0..requests {
        let sink = FutureValue::new();
        tx.send(ChordMessage::new((u32::MAX, Default::default()),node_addr, MessageContent::ClientRequest(
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
            ClientOperation::Put([
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31
            ])
        ))).await // Await because we don't want to send the next request until this one actually gets added to the send queue.
            .expect("Tokio send error when sending the request.")
    }
}

pub async fn send_heartbeat_triggers(inbox: Sender<ChordMessage>, interval: Duration) {
    let mut timer = tokio::time::interval(interval);
    loop {
        timer.tick().await;
        // Don't need to fill src/dest information because this message is internal.
        inbox.send(ChordMessage::new((u32::MAX, Default::default()), Default::default(), MessageContent::HeartbeatTimerExpired)).await
            .expect("Error: Could not send HeartbeatTimerExpired message to inbox.");
    }
}

pub async fn send_fix_fingers_triggers(inbox: Sender<ChordMessage>, interval: Duration) {
    let mut delta: NodeId = 1;
    let mut timer = tokio::time::interval(interval);
    loop {
        timer.tick().await;
        inbox.send(ChordMessage::new((u32::MAX, Default::default()), Default::default(), MessageContent::FixFingerTimerExpired(delta))).await
            .expect("Error: Could not send HeartbeatTimerExpired message to inbox.");
        if delta >= NodeId::MAX / 2 {
            delta = 1;
        } else {
            delta *= 2;
        }
    }
}

pub async fn run_node(mut r: SingleNodeRunner, activation: oneshot::Receiver<()>) {
    tokio::spawn(run_requests(r.requests, r.node.address(), r.outgoing.clone(), r.distribution, r.zipf_param, activation));
    if r.node.id() == MASTER_NODE {
        // We're the master node. That means we start first (by assumption), making us our own
        // predecessor and successor.
        r.node.set_predecessor((MASTER_NODE, r.node.address()));
        r.node.set_successor((MASTER_NODE, r.node.address()));
        // Insert ourselves as our own successor
        r.successor_table.insert(MASTER_NODE, (r.node.address(), (MASTER_NODE, r.node.address())));
        r.node.activate();
    } else {
        // We're a normal node. We need to message the master to bootstrap ourselves into the ring.
        let src = r.node.msg_id();
        if let Err(e) = r.outgoing.send(ChordMessage::new(src, r.master_ip, MessageContent::JoinToMaster)).await {
            eprintln!("Error: Could not send inactive error for FindResponse message because of error '{}'", e);
        }
    }
    let r = &mut r;
    loop {
        match r.incoming.recv().await {
            None => break,
            Some(msg) => {
                if r.verbose {
                    println!("Incoming message from {:?}: {:?}", msg.src, msg.content);
                }
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
                                    println!("adding a Put value!");
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
                                    println!("First redirect for key ({}, {}) to address {}", key.0, key.1, next.to_string());
                                    let src = r.node.msg_id();
                                    if let Err(e) = r.outgoing.send(ChordMessage::new(src, next, MessageContent::Find(key))).await {
                                        eprintln!("Error: Could not send Find message because of error '{}'", e);
                                    }
                                }
                                FindResult::Error(e) => {
                                    eprintln!("Find error (Node = {}, Requested key = (node: {}, stub: {})): {}", r.node.id(), key.0, key.1, e);
                                }
                                FindResult::NoSuchEntry(_) => {
                                    eprintln!("Error: No such entry for key: ({}, {})", key.0, key.1);
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
                        let me = r.node.msg_id();
                        let mut update_finger = |key: ContentId, key_range: Range<NodeId>| {
                            let diff_key = key.0.wrapping_sub(me.0) as f64;
                            if diff_key.log2().floor() == diff_key.log2().ceil() {
                                // We got a key that was a power of 2 away from our node, possibly from fix_finger().
                                // We can use it to update our finger table.
                                let size_so_far = r.node.populate_finger(key.0, msg.src.0, msg.src.1, key_range);
                                if r.node.inactive() && size_so_far >= size_of::<NodeId>() / 2 {
                                    // We've fully populated our table. Our node is ready for prime time!
                                    println!("activate!");
                                    r.node.activate();
                                    r.activation.take().expect("Should not get here more than once.").send(()).expect("Send error on activation oneshot() channel.");
                                }
                            }
                        };
                        let put_in_transit = r.puts_in_transit.get(&key);
                        if let Some(p) = &put_in_transit{
                            println!("Put is in transit!");
                        }
                        match result {
                            FindResult::Value(get_val, key_range) => {
                                update_finger(key, key_range);
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
                                    if let Err(e) = r.outgoing.send(ChordMessage::new(me, msg.src.1, MessageContent::PutValue(key, *put_val))).await {
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
                            FindResult::NoSuchEntry(key_range) => {
                                update_finger(key, key_range);
                                if let Some((put_val, _)) = put_in_transit {
                                    // If we have a put in transit, send it to the node we found.
                                    if let Err(e) = r.outgoing.send(ChordMessage::new(me, msg.src.1, MessageContent::PutValue(key, *put_val))).await {
                                        eprintln!("Error: Could not send PutValue message because of error '{}'", e);
                                    }
                                }
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
                            println!("successor table = {:?}", r.successor_table);
                            // Add new node's successor
                            let (s_node, s_addr) = r.successor_table.iter().min_by(|(me, _), (other, _)| {
                                let my_sub = me.wrapping_sub(new_node_id);
                                let other_sub = other.wrapping_sub(new_node_id);
                                println!("su_sub for {} - {} = {}", **me, new_node_id, my_sub);
                                println!("su_sub for {} - {} = {}", **other, new_node_id, other_sub);
                                let ordering = my_sub.cmp(&other_sub);
                                println!("ordering: {:?}", ordering);
                                ordering

                            }).map(|(n, (a, (sn, sa)))| (*n, *a))
                                .expect("There must be at least one key in the successor table if we get here.");

                            // Add previous node's successor
                            // Add new node's successor
                            let (p_node, p_addr) = r.successor_table.iter().min_by(|(me, _), (other, _)| {
                                // Notice the subtraction is reversed here from above. wrapping_add() calls make the same node as a predecessor rank last, not first.
                                let my_sub = new_node_id.wrapping_sub(**me);
                                let other_sub = new_node_id.wrapping_sub(**other);
                                println!("pre_sub for {} - {} = {}", new_node_id, **me, my_sub);
                                println!("pre_sub for {} - {} = {}", new_node_id, **other, other_sub);
                                let ordering = my_sub.cmp(&other_sub);
                                println!("ordering: {:?}", ordering);
                                ordering
                            }).map(|(n, (a, (sn, sa)))| (*n, *a))
                                .expect("There must be at least one key in the successor table if we get here.");

                            r.successor_table.insert(msg.src.0, (msg.src.1, (s_node, s_addr)));
                            r.successor_table.insert(p_node, (p_addr, msg.src));

                            if r.verbose {
                                println!("Master: new node with ID {}. Its successor is {} and predecessor is {}.", msg.src.0, s_node, p_node);
                            }
                            // Tell the sender who its successor is
                            let src = r.node.msg_id();
                            if let Err(e) = r.outgoing.send(ChordMessage::new(src, msg.src.1, MessageContent::JoinToMasterResponse(s_node, s_addr))).await {
                                eprintln!("Error: Could not send JoinToMasterResponse message because of error '{}'", e);
                            }
                        }
                    },
                    MessageContent::JoinToMasterResponse(s_node, s_addr) => {
                        // Mark the new node as our successor
                        println!("Setting successor as {}", s_node);
                        r.node.set_successor((s_node, s_addr));
                        // Tell new successor about us
                        let src = r.node.msg_id();
                        if let Err(e) = r.outgoing.send(ChordMessage::new(src, s_addr, MessageContent::JoinToSuccessor)).await {
                            eprintln!("Error: Could not send JoinToSuccessor message because of error '{}'", e);
                        }
                        // Then bug the successor until it tells us enough keys to populate the finger table.
                        let mut curr: NodeId = 1;
                        let src = r.node.msg_id();
                        let dest = r.node.successor().expect("Error: node has no successors even though we just added one. This is a bug.").1;
                        loop {
                            // Ask our successor about the location of several keys.
                            // We can just set the content stub to 0.
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
                        // Reply to the new node with the keys it should be responsible for
                        let send_keys = r.node.offload_keys_before(msg.src.0);
                        let src = r.node.msg_id();
                        let old_predecessor = r.node.predecessor();
                        // We have a new predecessor
                        r.node.set_predecessor(msg.src);
                        if let Some(pred) = old_predecessor {
                            if let Err(e) = r.outgoing.send(ChordMessage::new(src, msg.src.1, MessageContent::JoinToSuccessorAck(pred, send_keys))).await {
                                eprintln!("Error: Could not send SuccessorHeartbeatAck message because of error '{}'", e);
                            }
                        }
                    },
                    MessageContent::JoinToSuccessorAck(predecessor, keys) => {
                        r.node.set_predecessor(predecessor);
                        keys.into_iter().for_each(|(k, v) | r.node.set(k, v));
                    },
                    MessageContent::PutValues( keys) => {
                        // Same as JoinToSuccessorAck without initializing the predecessor.
                        keys.into_iter().for_each(|(k, v) | r.node.set(k, v));
                    },
                    MessageContent::SuccessorHeartbeat => {
                        // Check if we have a later predecessor than the node that thinks we're
                        // its successor
                        let opt_predecessor = r.node.predecessor();
                        match opt_predecessor {
                            Some((pre_node, pre_addr)) => {
                                let src = r.node.msg_id();
                                if between_mod_id(msg.src.0, pre_node, r.node.id()) {
                                    if pre_node == r.node.id() {
                                        // Special case. The same node as a predecessor is actually the WORST possible one.
                                        // The sender's is better, no matter what it is.
                                        if let Err(e) = r.outgoing.send(ChordMessage::new(src, msg.src.1, MessageContent::PutValues(r.node.offload_keys_before(msg.src.0)))).await {
                                            eprintln!("Error: Could not send SuccessorHeartbeatNewSuccessor message because of error '{}'", e);
                                        }
                                        r.node.set_predecessor(msg.src);
                                    } else {
                                        // We have a closer predecessor than the sender; tell the sender to make it its successor.
                                        // Update the sender to the new in-between node
                                        if let Err(e) = r.outgoing.send(ChordMessage::new(src, msg.src.1, MessageContent::SuccessorHeartbeatNewSuccessor(pre_node, pre_addr))).await {
                                            eprintln!("Error: Could not send SuccessorHeartbeatNewSuccessor message because of error '{}'", e);
                                        }
                                    }
                                } else {
                                    if msg.src.0 != pre_node {
                                        // The sender is a closer predecessor than what we have marked.
                                        if let Err(e) = r.outgoing.send(ChordMessage::new(src, msg.src.1, MessageContent::PutValues(r.node.offload_keys_before(msg.src.0)))).await {
                                            eprintln!("Error: Could not send SuccessorHeartbeatNewSuccessor message because of error '{}'", e);
                                        }
                                        r.node.set_predecessor(msg.src);
                                    } // Otherwise we still have the same predecessor.
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
                        println!("Changing successor from {} to {}.", r.node.successor().map_or_else(|| String::from("nothing"), |(a, b)| a.to_string()), s_node);
                        r.node.set_successor((s_node, s_addr))
                    }
                    MessageContent::HeartbeatTimerExpired => {
                        if r.verbose {
                            r.node.print_status();
                        }
                        let opt_successor = r.node.successor();
                        if let Some((_s_node, s_addr)) = opt_successor {
                            let src = r.node.msg_id();
                            if let Err(e) = r.outgoing.send(ChordMessage::new(src, s_addr, MessageContent::SuccessorHeartbeat)).await {
                                eprintln!("Error: Could not send SuccessorHeartbeatAck message because of error '{}'", e);
                            }
                        }
                    }
                    MessageContent::FixFingerTimerExpired(delta) => {
                        let src = r.node.msg_id();
                        let dummy_key = (src.0.wrapping_add(delta), 0);
                        match r.node.next_finger(dummy_key) {
                            FindResult::Value(..) | FindResult::NoSuchEntry(..) => {
                                // We redirected to ourself. There's nothing to put in the finger table.
                            }
                            FindResult::Redirect(next) => {
                                if let Err(e) = r.outgoing.send(ChordMessage::new(src, next, MessageContent::Find(dummy_key))).await {
                                    eprintln!("Error: Could not send inactive error for FindResponse message because of error '{}'", e);
                                }
                            }
                            FindResult::Error(e) => {
                                //eprintln!("Error: Find for fix_finger was unsuccessful: {}", e)
                            }
                        }
                    }
                }
            }
        }
    }
}