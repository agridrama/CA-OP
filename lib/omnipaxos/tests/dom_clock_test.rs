pub mod utils;

use omnipaxos::{
    messages::{
        sequence_paxos::{EntryId, PaxosMsg},
        Message,
    },
    util::{LogEntry, NodeId, PhysicalClock},
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_storage::memory_storage::MemoryStorage;
use serial_test::serial;
use std::cmp::Reverse;
use std::collections::VecDeque;
use std::sync::Mutex;
use utils::{
    verification::{verify_log_unordered, verify_matching_logs},
    StorageType, Value,
};

type TestOmniPaxos = OmniPaxos<'static, Value, StorageType<Value>, FakeClock>;

struct FakeClock {
    time: Mutex<i64>,
    uncertainty: Mutex<i64>,
}

impl FakeClock {
    fn new(time: i64, uncertainty: i64) -> Self {
        Self {
            time: Mutex::new(time),
            uncertainty: Mutex::new(uncertainty),
        }
    }

    fn advance(&self, delta: i64) {
        *self.time.lock().expect("clock poisoned") += delta;
    }

    fn set_time(&self, time: i64) {
        *self.time.lock().expect("clock poisoned") = time;
    }
}

impl PhysicalClock for FakeClock {
    fn get_time(&self) -> i64 {
        *self.time.lock().expect("clock poisoned")
    }

    fn get_uncertainty(&self) -> i64 {
        *self.uncertainty.lock().expect("clock poisoned")
    }

    fn get_time_with_uncertainty(&self) -> (i64, i64) {
        (self.get_time(), self.get_uncertainty())
    }
}

struct SimNode {
    pid: NodeId,
    clock: &'static FakeClock,
    op: TestOmniPaxos,
}

fn build_node(pid: NodeId, nodes: &[NodeId], clock: &'static FakeClock) -> TestOmniPaxos {
    let mut op_config = OmniPaxosConfig::default();
    op_config.server_config.pid = pid;
    op_config.server_config.election_tick_timeout = 1_000;
    op_config.server_config.resend_message_tick_timeout = 1_000;
    op_config.server_config.flush_batch_tick_timeout = 1_000;
    op_config.cluster_config.configuration_id = 1;
    op_config.cluster_config.nodes = nodes.to_vec();
    op_config
        .build(StorageType::with_memory(MemoryStorage::default()), clock)
        .expect("failed to build OmniPaxos")
}

fn make_cluster(clock_specs: &[(NodeId, i64, i64)]) -> Vec<SimNode> {
    let nodes: Vec<NodeId> = clock_specs.iter().map(|(pid, _, _)| *pid).collect();
    clock_specs
        .iter()
        .map(|(pid, time, uncertainty)| {
            let clock = Box::leak(Box::new(FakeClock::new(*time, *uncertainty)));
            SimNode {
                pid: *pid,
                clock,
                op: build_node(*pid, &nodes, clock),
            }
        })
        .collect()
}

fn node_idx(nodes: &[SimNode], pid: NodeId) -> usize {
    nodes.iter()
        .position(|node| node.pid == pid)
        .unwrap_or_else(|| panic!("node {} not found", pid))
}

fn drain_outgoing_from(nodes: &mut [SimNode], pid: NodeId) -> Vec<Message<Value>> {
    let idx = node_idx(nodes, pid);
    let mut outgoing = vec![];
    nodes[idx].op.take_outgoing_messages(&mut outgoing);
    outgoing
}

fn drain_outgoing_from_all(nodes: &mut [SimNode]) -> Vec<Message<Value>> {
    let mut out = vec![];
    for idx in 0..nodes.len() {
        nodes[idx].op.take_outgoing_messages(&mut out);
    }
    out
}

fn deliver_one(nodes: &mut [SimNode], mailbox: &mut VecDeque<Message<Value>>, msg: Message<Value>) {
    let receiver = msg.get_receiver();
    let idx = node_idx(nodes, receiver);
    nodes[idx].op.handle_incoming(msg);
    mailbox.extend(drain_outgoing_from(nodes, receiver));
}

fn deliver_all_fifo(nodes: &mut [SimNode], mailbox: &mut VecDeque<Message<Value>>) {
    let mut handled = 0usize;
    while let Some(msg) = mailbox.pop_front() {
        handled += 1;
        assert!(handled < 20_000, "message pump did not quiesce");
        deliver_one(nodes, mailbox, msg);
    }
}

fn retain_without_follower_accepted(
    mailbox: &mut VecDeque<Message<Value>>,
    leader_pid: NodeId,
) -> usize {
    let before = mailbox.len();
    mailbox.retain(|msg| {
        !matches!(
            msg,
            Message::SequencePaxos(px)
                if px.to == leader_pid && matches!(px.msg, PaxosMsg::Accepted(_))
        )
    });
    before - mailbox.len()
}

fn tick_nodes(nodes: &mut [SimNode], pids: &[NodeId], mailbox: &mut VecDeque<Message<Value>>) {
    for pid in pids {
        let idx = node_idx(nodes, *pid);
        nodes[idx].op.poll();
        mailbox.extend(drain_outgoing_from(nodes, *pid));
    }
}

fn append_at_leader(nodes: &mut [SimNode], leader_pid: NodeId, value_id: u64) {
    let leader_idx = node_idx(nodes, leader_pid);
    nodes[leader_idx]
        .op
        .append_with_id(
            Value::with_id(value_id),
            EntryId {
                client_id: leader_pid,
                command_id: value_id as usize,
            },
        )
        .expect("failed to append");
}

fn establish_leader(nodes: &mut [SimNode], leader_pid: NodeId) {
    let leader_idx = node_idx(nodes, leader_pid);
    nodes[leader_idx].op.try_become_leader();
    let mut mailbox = VecDeque::from(drain_outgoing_from_all(nodes));
    deliver_all_fifo(nodes, &mut mailbox);
}

fn decided_logs(nodes: &[SimNode]) -> Vec<Vec<LogEntry<Value>>> {
    nodes.iter()
        .map(|node| node.op.read_decided_suffix(0).unwrap_or_default())
        .collect()
}

fn assert_logs_match_exact(nodes: &[SimNode], expected: &[Value]) {
    let logs = decided_logs(nodes);
    let reference = logs.first().expect("missing logs");
    verify_log_unordered(reference.clone(), expected.to_vec());
    for log in logs.iter().skip(1) {
        verify_matching_logs(reference, log);
    }
}

fn release_only_leader(nodes: &mut [SimNode], leader_pid: NodeId) {
    let leader_idx = node_idx(nodes, leader_pid);
    let now = nodes[leader_idx].clock.get_time();
    for node in nodes.iter() {
        if node.pid == leader_pid {
            node.clock.set_time(now + 20_000);
        }
    }
    let mut mailbox = VecDeque::new();
    tick_nodes(nodes, &[leader_pid], &mut mailbox);
    deliver_all_fifo(nodes, &mut mailbox);
}

fn release_only_followers(nodes: &mut [SimNode], follower_pids: &[NodeId]) {
    for pid in follower_pids {
        let idx = node_idx(nodes, *pid);
        let now = nodes[idx].clock.get_time();
        nodes[idx].clock.set_time(now + 20_000);
    }
    let mut mailbox = VecDeque::new();
    tick_nodes(nodes, follower_pids, &mut mailbox);
    deliver_all_fifo(nodes, &mut mailbox);
}

fn release_only_followers_collect(
    nodes: &mut [SimNode],
    follower_pids: &[NodeId],
) -> VecDeque<Message<Value>> {
    for pid in follower_pids {
        let idx = node_idx(nodes, *pid);
        let now = nodes[idx].clock.get_time();
        nodes[idx].clock.set_time(now + 20_000);
    }
    let mut mailbox = VecDeque::new();
    tick_nodes(nodes, follower_pids, &mut mailbox);
    mailbox
}

fn split_dom_proposals(
    mut mailbox: VecDeque<Message<Value>>,
) -> (Vec<Message<Value>>, VecDeque<Message<Value>>) {
    let mut dom_props = vec![];
    let mut rest = VecDeque::new();
    while let Some(msg) = mailbox.pop_front() {
        match &msg {
            Message::SequencePaxos(px) if matches!(px.msg, PaxosMsg::DomPropose(_)) => {
                dom_props.push(msg)
            }
            _ => rest.push_back(msg),
        }
    }
    (dom_props, rest)
}

fn dom_command_id(msg: &Message<Value>) -> usize {
    match msg {
        Message::SequencePaxos(px) => match &px.msg {
            PaxosMsg::DomPropose(prop) => prop.entry_id.command_id,
            _ => panic!("expected DomPropose"),
        },
        Message::BLE(_) => panic!("expected SequencePaxos DomPropose"),
    }
}

fn deliver_to_follower_in_order(
    nodes: &mut [SimNode],
    follower_pid: NodeId,
    dom_props: &[Message<Value>],
    order: &[usize],
) {
    let mut mailbox = VecDeque::new();
    for command_id in order {
        let msg = dom_props
            .iter()
            .find(|msg| {
                matches!(
                    msg,
                    Message::SequencePaxos(px)
                        if px.to == follower_pid && dom_command_id(msg) == *command_id
                )
            })
            .unwrap_or_else(|| panic!("missing DomPropose {} for follower {}", command_id, follower_pid))
            .clone();
        deliver_one(nodes, &mut mailbox, msg);
        deliver_all_fifo(nodes, &mut mailbox);
    }
}

#[test]
#[serial]
fn slow_path_when_followers_never_release_test() {
    let mut nodes = make_cluster(&[(1, 1_000_000, 0), (2, 0, 0), (3, 0, 0)]);
    establish_leader(&mut nodes, 1);

    for value_id in 1..=3 {
        append_at_leader(&mut nodes, 1, value_id);
        let mut mailbox = VecDeque::from(drain_outgoing_from_all(&mut nodes));
        deliver_all_fifo(&mut nodes, &mut mailbox);
        release_only_leader(&mut nodes, 1);
    }

    assert_logs_match_exact(
        &nodes,
        &[Value::with_id(1), Value::with_id(2), Value::with_id(3)],
    );
}

#[test]
#[serial]
fn fast_path_after_slow_leader_release_test() {
    let mut nodes = make_cluster(&[(1, 1_000_000, 0), (2, 2_000_000, 0), (3, 2_500_000, 0)]);
    establish_leader(&mut nodes, 1);

    for value_id in 10..=12 {
        append_at_leader(&mut nodes, 1, value_id);
        let mut mailbox = VecDeque::from(drain_outgoing_from_all(&mut nodes));
        deliver_all_fifo(&mut nodes, &mut mailbox);
        let mut mailbox = release_only_followers_collect(&mut nodes, &[2, 3]);
        let dropped_accepted = retain_without_follower_accepted(&mut mailbox, 1);
        assert_eq!(
            dropped_accepted, 0,
            "followers should not have emitted slow-path Accepted before the leader release for proposal {}",
            value_id
        );
        assert!(
            mailbox.iter().any(|msg| {
                matches!(
                    msg,
                    Message::SequencePaxos(px)
                        if px.to == 1 && matches!(px.msg, PaxosMsg::FastAccepted(_))
                )
            }),
            "expected FastAccepted messages to reach the leader for proposal {}",
            value_id
        );
        deliver_all_fifo(&mut nodes, &mut mailbox);

        let leader_log_before_release = nodes[node_idx(&nodes, 1)]
            .op
            .read_decided_suffix(0)
            .unwrap_or_default();
        assert!(
            leader_log_before_release.len() == (value_id - 10) as usize,
            "leader should not decide proposal {} before its own release",
            value_id
        );

        release_only_leader(&mut nodes, 1);
    }

    assert_logs_match_exact(
        &nodes,
        &[Value::with_id(10), Value::with_id(11), Value::with_id(12)],
    );
}

#[test]
#[serial]
fn leader_late_reordering_is_followed_by_followers_test() {
    let mut nodes = make_cluster(&[(1, 3_000_000, 0), (2, 3_000_000, 0), (3, 3_000_000, 0)]);
    establish_leader(&mut nodes, 1);

    append_at_leader(&mut nodes, 1, 3);
    let mut mailbox = VecDeque::from(drain_outgoing_from_all(&mut nodes));

    release_only_leader(&mut nodes, 1);

    append_at_leader(&mut nodes, 1, 1);
    append_at_leader(&mut nodes, 1, 2);
    mailbox.extend(drain_outgoing_from_all(&mut nodes));

    let mut dom_props = vec![];
    let mut rest = VecDeque::new();
    while let Some(msg) = mailbox.pop_front() {
        match &msg {
            Message::SequencePaxos(px) if matches!(px.msg, PaxosMsg::DomPropose(_)) => {
                dom_props.push(msg)
            }
            _ => rest.push_back(msg),
        }
    }

    dom_props.sort_by_key(|msg| match msg {
        Message::SequencePaxos(px) => match &px.msg {
            PaxosMsg::DomPropose(prop) => prop.entry_id.command_id,
            _ => unreachable!(),
        },
        _ => unreachable!(),
    });

    let mut follower_mailbox = VecDeque::from(dom_props);
    deliver_all_fifo(&mut nodes, &mut follower_mailbox);

    release_only_followers(&mut nodes, &[2, 3]);

    deliver_all_fifo(&mut nodes, &mut rest);
    release_only_leader(&mut nodes, 1);

    assert_logs_match_exact(
        &nodes,
        &[Value::with_id(3), Value::with_id(1), Value::with_id(2)],
    );
}

#[test]
#[serial]
fn random_clock_changes_still_converge_test() {
    let mut nodes = make_cluster(&[(1, 5_000_000, 0), (2, 4_900_000, 0), (3, 5_100_000, 0)]);
    establish_leader(&mut nodes, 1);

    let clock_deltas = [
        (1, 7_000, 2, -3_000, 3, 9_000),
        (1, -4_000, 2, 11_000, 3, -6_000),
        (1, 13_000, 2, -5_000, 3, 8_000),
        (1, -2_000, 2, 7_000, 3, -1_000),
    ];

    for (proposal, deltas) in (21_u64..=24).zip(clock_deltas) {
        nodes[node_idx(&nodes, deltas.0)].clock.advance(deltas.1);
        nodes[node_idx(&nodes, deltas.2)].clock.advance(deltas.3);
        nodes[node_idx(&nodes, deltas.4)].clock.advance(deltas.5);

        append_at_leader(&mut nodes, 1, proposal);
        let mut mailbox = VecDeque::from(drain_outgoing_from_all(&mut nodes));
        deliver_all_fifo(&mut nodes, &mut mailbox);

        for node in &nodes {
            node.clock.advance(30_000);
        }
        let mut tick_mailbox = VecDeque::new();
        tick_nodes(&mut nodes, &[1, 2, 3], &mut tick_mailbox);
        deliver_all_fifo(&mut nodes, &mut tick_mailbox);
    }

    assert_logs_match_exact(
        &nodes,
        &[
            Value::with_id(21),
            Value::with_id(22),
            Value::with_id(23),
            Value::with_id(24),
        ],
    );
}

#[test]
#[serial]
fn followers_late_but_leader_still_decides_test() {
    let mut nodes = make_cluster(&[(1, 1_000_000, 0), (2, 1_000_000, 0), (3, 1_000_000, 0)]);
    establish_leader(&mut nodes, 1);

    append_at_leader(&mut nodes, 1, 31);
    nodes[node_idx(&nodes, 1)].clock.advance(1_000);
    append_at_leader(&mut nodes, 1, 32);
    nodes[node_idx(&nodes, 1)].clock.advance(1_000);
    append_at_leader(&mut nodes, 1, 33);
    nodes[node_idx(&nodes, 1)].clock.advance(1_000);
    append_at_leader(&mut nodes, 1, 34);

    let mailbox = VecDeque::from(drain_outgoing_from_all(&mut nodes));
    let (dom_props, mut rest) = split_dom_proposals(mailbox);

    deliver_to_follower_in_order(&mut nodes, 2, &dom_props, &[34]);
    deliver_to_follower_in_order(&mut nodes, 3, &dom_props, &[34]);
    release_only_followers(&mut nodes, &[2, 3]);

    let mut late_mailbox = VecDeque::new();
    deliver_to_follower_in_order(&mut nodes, 2, &dom_props, &[31, 32, 33]);
    deliver_to_follower_in_order(&mut nodes, 3, &dom_props, &[31, 32, 33]);
    late_mailbox.extend(drain_outgoing_from_all(&mut nodes));
    assert!(
        !late_mailbox.iter().any(|msg| {
            matches!(
                msg,
                Message::SequencePaxos(px)
                    if px.to == 1 && matches!(px.msg, PaxosMsg::FastAccepted(_))
            )
        }),
        "late follower deliveries should not emit FastAccepted"
    );
    deliver_all_fifo(&mut nodes, &mut late_mailbox);

    deliver_all_fifo(&mut nodes, &mut rest);
    release_only_leader(&mut nodes, 1);

    assert_logs_match_exact(
        &nodes,
        &[
            Value::with_id(31),
            Value::with_id(32),
            Value::with_id(33),
            Value::with_id(34),
        ],
    );
}

#[test]
#[serial]
fn conflicting_follower_release_orders_still_converge_test() {
    let mut nodes = make_cluster(&[(1, 4_000_000, 0), (2, 4_000_000, 0), (3, 4_000_000, 0)]);
    establish_leader(&mut nodes, 1);

    for value_id in [41_u64, 42, 43] {
        append_at_leader(&mut nodes, 1, value_id);
    }

    let mailbox = VecDeque::from(drain_outgoing_from_all(&mut nodes));
    let (mut dom_props, mut rest) = split_dom_proposals(mailbox);
    dom_props.sort_by_key(dom_command_id);

    deliver_to_follower_in_order(&mut nodes, 2, &dom_props, &[41, 42, 43]);
    release_only_followers(&mut nodes, &[2]);

    deliver_to_follower_in_order(&mut nodes, 3, &dom_props, &[42, 41, 43]);
    release_only_followers(&mut nodes, &[3]);

    deliver_all_fifo(&mut nodes, &mut rest);
    release_only_leader(&mut nodes, 1);

    assert_logs_match_exact(
        &nodes,
        &[
            Value::with_id(41),
            Value::with_id(42),
            Value::with_id(43),
        ],
    );
}

#[test]
#[serial]
fn leader_backward_jump_still_converges_test() {
    let mut nodes = make_cluster(&[(1, 6_000_000, 0), (2, 6_100_000, 0), (3, 6_200_000, 0)]);
    establish_leader(&mut nodes, 1);

    for value_id in 51..=54 {
        append_at_leader(&mut nodes, 1, value_id);
        let mut mailbox = VecDeque::from(drain_outgoing_from_all(&mut nodes));
        deliver_all_fifo(&mut nodes, &mut mailbox);

        nodes[node_idx(&nodes, 1)].clock.advance(-15_000);
        nodes[node_idx(&nodes, 2)].clock.advance(8_000);
        nodes[node_idx(&nodes, 3)].clock.advance(12_000);

        let mut tick_mailbox = VecDeque::new();
        tick_nodes(&mut nodes, &[1, 2, 3], &mut tick_mailbox);
        deliver_all_fifo(&mut nodes, &mut tick_mailbox);

        nodes[node_idx(&nodes, 1)].clock.advance(40_000);
        let mut settle_mailbox = VecDeque::new();
        tick_nodes(&mut nodes, &[1], &mut settle_mailbox);
        deliver_all_fifo(&mut nodes, &mut settle_mailbox);
    }

    assert_logs_match_exact(
        &nodes,
        &[
            Value::with_id(51),
            Value::with_id(52),
            Value::with_id(53),
            Value::with_id(54),
        ],
    );
}

#[test]
#[serial]
fn mixed_fast_slow_and_late_paths_per_proposal_test() {
    let mut nodes = make_cluster(&[(1, 8_000_000, 0), (2, 8_000_000, 0), (3, 8_000_000, 0)]);
    establish_leader(&mut nodes, 1);

    append_at_leader(&mut nodes, 1, 61);
    let mut mailbox = VecDeque::from(drain_outgoing_from_all(&mut nodes));
    deliver_all_fifo(&mut nodes, &mut mailbox);
    let mut fast_mailbox = release_only_followers_collect(&mut nodes, &[2, 3]);
    assert!(
        fast_mailbox.iter().any(|msg| {
            matches!(
                msg,
                Message::SequencePaxos(px)
                    if px.to == 1 && matches!(px.msg, PaxosMsg::FastAccepted(_))
            )
        }),
        "expected proposal 61 to produce FastAccepted messages"
    );
    deliver_all_fifo(&mut nodes, &mut fast_mailbox);
    release_only_leader(&mut nodes, 1);

    append_at_leader(&mut nodes, 1, 62);
    let mut mailbox = VecDeque::from(drain_outgoing_from_all(&mut nodes));
    deliver_all_fifo(&mut nodes, &mut mailbox);
    release_only_leader(&mut nodes, 1);

    append_at_leader(&mut nodes, 1, 63);
    nodes[node_idx(&nodes, 1)].clock.advance(1_000);
    append_at_leader(&mut nodes, 1, 64);
    let mailbox = VecDeque::from(drain_outgoing_from_all(&mut nodes));
    let (dom_props, mut rest) = split_dom_proposals(mailbox);

    deliver_to_follower_in_order(&mut nodes, 2, &dom_props, &[64]);
    deliver_to_follower_in_order(&mut nodes, 3, &dom_props, &[64]);
    nodes[node_idx(&nodes, 2)].clock.advance(100_000);
    nodes[node_idx(&nodes, 3)].clock.advance(100_000);
    let mut fast_late_seed = release_only_followers_collect(&mut nodes, &[2, 3]);
    assert!(
        fast_late_seed.iter().any(|msg| {
            matches!(
                msg,
                Message::SequencePaxos(px)
                    if px.to == 1 && matches!(px.msg, PaxosMsg::FastAccepted(_))
            )
        }),
        "expected proposal 64 to produce FastAccepted messages"
    );
    deliver_all_fifo(&mut nodes, &mut fast_late_seed);

    deliver_to_follower_in_order(&mut nodes, 2, &dom_props, &[63]);
    deliver_to_follower_in_order(&mut nodes, 3, &dom_props, &[63]);
    let mut late_mailbox = VecDeque::from(drain_outgoing_from_all(&mut nodes));
    assert!(
        !late_mailbox.iter().any(|msg| {
            matches!(
                msg,
                Message::SequencePaxos(px)
                    if px.to == 1 && matches!(px.msg, PaxosMsg::FastAccepted(_))
            )
        }),
        "late followers should not emit FastAccepted for proposal 63"
    );
    deliver_all_fifo(&mut nodes, &mut late_mailbox);
    deliver_all_fifo(&mut nodes, &mut rest);
    release_only_leader(&mut nodes, 1);

    assert_logs_match_exact(
        &nodes,
        &[
            Value::with_id(61),
            Value::with_id(62),
            Value::with_id(63),
            Value::with_id(64),
        ],
    );
}

#[test]
#[serial]
fn random_clock_and_message_reorder_still_converge_test() {
    let mut nodes = make_cluster(&[(1, 10_000_000, 0), (2, 9_700_000, 0), (3, 10_300_000, 0)]);
    establish_leader(&mut nodes, 1);

    for value_id in 71..=74 {
        append_at_leader(&mut nodes, 1, value_id);
        let mailbox = VecDeque::from(drain_outgoing_from_all(&mut nodes));
        let (mut dom_props, mut rest) = split_dom_proposals(mailbox);
        dom_props.sort_by_key(|msg| Reverse(dom_command_id(msg)));

        let mut reordered = VecDeque::from(dom_props);
        reordered.append(&mut rest);
        deliver_all_fifo(&mut nodes, &mut reordered);

        nodes[node_idx(&nodes, 1)]
            .clock
            .advance(if value_id % 2 == 0 { -7_000 } else { 18_000 });
        nodes[node_idx(&nodes, 2)]
            .clock
            .advance(if value_id % 2 == 0 { 14_000 } else { -5_000 });
        nodes[node_idx(&nodes, 3)]
            .clock
            .advance(if value_id % 2 == 0 { -3_000 } else { 11_000 });

        let mut tick_mailbox = VecDeque::new();
        tick_nodes(&mut nodes, &[1, 2, 3], &mut tick_mailbox);
        let (mut dom_props, mut rest) = split_dom_proposals(tick_mailbox);
        dom_props.sort_by_key(|msg| Reverse(dom_command_id(msg)));
        let mut reordered_ticks = VecDeque::from(dom_props);
        reordered_ticks.append(&mut rest);
        deliver_all_fifo(&mut nodes, &mut reordered_ticks);

        nodes[node_idx(&nodes, 1)].clock.advance(35_000);
        nodes[node_idx(&nodes, 2)].clock.advance(35_000);
        nodes[node_idx(&nodes, 3)].clock.advance(35_000);
        let mut settle = VecDeque::new();
        tick_nodes(&mut nodes, &[1, 2, 3], &mut settle);
        deliver_all_fifo(&mut nodes, &mut settle);
    }

    assert_logs_match_exact(
        &nodes,
        &[
            Value::with_id(71),
            Value::with_id(72),
            Value::with_id(73),
            Value::with_id(74),
        ],
    );
}
