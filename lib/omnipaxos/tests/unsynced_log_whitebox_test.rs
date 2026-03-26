pub mod utils;

use omnipaxos::{
    ballot_leader_election::Ballot,
    messages::{
        sequence_paxos::{
            AcceptSync, DomPropose, EntryId, PaxosMessage, PaxosMsg, Prepare, Promise,
        },
        Message,
    },
    util::{LogEntry, LogSync, NodeId, SequenceNumber, SystemClock},
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_storage::memory_storage::MemoryStorage;
use serial_test::serial;
use std::collections::BTreeSet;
use utils::{test_clock, StorageType, Value};

type TestOmniPaxos = OmniPaxos<'static, Value, StorageType<Value>, SystemClock>;

fn build_node(pid: NodeId, nodes: &[NodeId]) -> TestOmniPaxos {
    let mut op_config = OmniPaxosConfig::default();
    op_config.server_config.pid = pid;
    op_config.server_config.election_tick_timeout = 1;
    op_config.cluster_config.configuration_id = 1;
    op_config.cluster_config.nodes = nodes.to_vec();
    op_config
        .build(StorageType::with_memory(MemoryStorage::default()), test_clock())
        .expect("failed to build OmniPaxos")
}

fn establish_follower_accept(
    pid: NodeId,
    nodes: &[NodeId],
    leader_ballot: Ballot,
) -> TestOmniPaxos {
    let mut op = build_node(pid, nodes);
    op.handle_incoming(Message::SequencePaxos(PaxosMessage {
        from: leader_ballot.pid,
        to: pid,
        msg: PaxosMsg::Prepare(Prepare {
            n: leader_ballot,
            decided_idx: 0,
            accepted_idx: 0,
            n_accepted: Ballot::default(),
        }),
    }));
    op.handle_incoming(Message::SequencePaxos(PaxosMessage {
        from: leader_ballot.pid,
        to: pid,
        msg: PaxosMsg::AcceptSync(AcceptSync {
            n: leader_ballot,
            seq_num: SequenceNumber {
                session: 1,
                counter: 1,
            },
            decided_idx: 0,
            log_sync: LogSync {
                decided_snapshot: None,
                suffix: vec![],
                sync_idx: 0,
                stopsign: None,
            },
            log_prefix_hash: Default::default(),
            #[cfg(feature = "unicache")]
            unicache: <Value as omnipaxos::storage::Entry>::UniCache::new(),
        }),
    }));
    let mut ignored = vec![];
    op.take_outgoing_messages(&mut ignored);
    op
}

fn inject_unsynced(
    op: &mut TestOmniPaxos,
    receiver: NodeId,
    sender: NodeId,
    value_id: u64,
    deadline: i64,
) {
    op.handle_incoming(Message::SequencePaxos(PaxosMessage {
        from: sender,
        to: receiver,
        msg: PaxosMsg::DomPropose(DomPropose {
            entry_id: EntryId {
                client_id: sender,
                command_id: value_id as usize,
            },
            sender,
            sent_time: (0, 0),
            deadline,
            entry: Value::with_id(value_id),
        }),
    }));
    op.poll();
    let mut ignored = vec![];
    op.take_outgoing_messages(&mut ignored);
}

fn inject_unsynced_with_entry_id(
    op: &mut TestOmniPaxos,
    receiver: NodeId,
    sender: NodeId,
    entry_id: EntryId,
    value_id: u64,
    deadline: i64,
) {
    op.handle_incoming(Message::SequencePaxos(PaxosMessage {
        from: sender,
        to: receiver,
        msg: PaxosMsg::DomPropose(DomPropose {
            entry_id,
            sender,
            sent_time: (0, 0),
            deadline,
            entry: Value::with_id(value_id),
        }),
    }));
    op.poll();
    let mut ignored = vec![];
    op.take_outgoing_messages(&mut ignored);
}

fn trigger_prepare(leader: &mut TestOmniPaxos) -> Vec<PaxosMessage<Value>> {
    leader.try_become_leader();
    let mut outgoing = vec![];
    leader.take_outgoing_messages(&mut outgoing);
    outgoing
        .into_iter()
        .filter_map(|msg| match msg {
            Message::SequencePaxos(px) if matches!(px.msg, PaxosMsg::Prepare(_)) => Some(px),
            _ => None,
        })
        .collect()
}

fn collect_promise(
    follower: &mut TestOmniPaxos,
    prepare: PaxosMessage<Value>,
) -> Promise<Value> {
    follower.handle_incoming(Message::SequencePaxos(prepare));
    let mut outgoing = vec![];
    follower.take_outgoing_messages(&mut outgoing);
    outgoing
        .into_iter()
        .find_map(|msg| match msg {
            Message::SequencePaxos(px) => match px.msg {
                PaxosMsg::Promise(promise) => Some(promise),
                _ => None,
            },
            _ => None,
        })
        .expect("follower should reply with Promise")
}

fn deliver_promises(leader: &mut TestOmniPaxos, from: &[(NodeId, Promise<Value>)]) {
    for (pid, promise) in from {
        leader.handle_incoming(Message::SequencePaxos(PaxosMessage {
            from: *pid,
            to: 1,
            msg: PaxosMsg::Promise(promise.clone()),
        }));
    }
}

fn recovered_first_entry(leader: &TestOmniPaxos) -> Option<Value> {
    match leader.read(0) {
        Some(LogEntry::Undecided(v)) | Some(LogEntry::Decided(v)) => Some(v),
        _ => None,
    }
}

fn follower_triplets<T: Clone>(items: &[(NodeId, T)]) -> Vec<Vec<(NodeId, T)>> {
    let mut out = Vec::new();
    for i in 0..items.len() {
        for j in (i + 1)..items.len() {
            for k in (j + 1)..items.len() {
                out.push(vec![
                    (items[i].0, items[i].1.clone()),
                    (items[j].0, items[j].1.clone()),
                    (items[k].0, items[k].1.clone()),
                ]);
            }
        }
    }
    out
}

fn acceptsync_suffixes(leader: &mut TestOmniPaxos) -> Vec<Vec<Value>> {
    let mut outgoing = vec![];
    leader.take_outgoing_messages(&mut outgoing);
    outgoing
        .into_iter()
        .filter_map(|msg| match msg {
            Message::SequencePaxos(px) => match px.msg {
                PaxosMsg::AcceptSync(accsync) => Some(accsync.log_sync.suffix),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

fn read_all_values(leader: &TestOmniPaxos) -> Vec<Value> {
    let mut values = Vec::new();
    let mut idx = 0;
    while let Some(entry) = leader.read(idx) {
        match entry {
            LogEntry::Decided(v) | LogEntry::Undecided(v) => values.push(v),
            _ => break,
        }
        idx += 1;
    }
    values
}

#[test]
#[serial]
fn prepare_recovery_unsynced_majority_wins_test() {
    let all_triplets = follower_triplets(&(2..=7).map(|pid| (pid, ())).collect::<Vec<_>>());
    for triplet in all_triplets {
        let chosen: BTreeSet<_> = triplet.iter().map(|(pid, _)| *pid).collect();

        let nodes: Vec<NodeId> = (1..=7).collect();
        let old_leader = Ballot::with(1, 1, 0, 7);
        let mut leader = establish_follower_accept(1, &nodes, old_leader);
        let mut followers: Vec<_> = (2..=7)
            .map(|pid| (pid, establish_follower_accept(pid, &nodes, old_leader)))
            .collect();

        for (pid, follower) in followers.iter_mut() {
            if *pid <= 5 {
                inject_unsynced(follower, *pid, 90, 10_001, 1000);
            } else {
                inject_unsynced(follower, *pid, 91, 20_001, 2000);
            }
        }

        let prepares = trigger_prepare(&mut leader);
        let mut promises = vec![];
        for (pid, follower) in followers.iter_mut() {
            let prepare = prepares
                .iter()
                .find(|msg| msg.to == *pid)
                .cloned()
                .expect("missing Prepare");
            promises.push((*pid, collect_promise(follower, prepare)));
        }

        let delivered: Vec<_> = promises
            .into_iter()
            .filter(|(pid, _)| chosen.contains(pid))
            .collect();
        deliver_promises(&mut leader, &delivered);

        let supporters_for_a = chosen.iter().filter(|pid| **pid <= 5).count();
        if supporters_for_a >= 3 {
            let recovered = recovered_first_entry(&leader)
                .expect("leader should recover the majority candidate with three matching promises");
            assert_eq!(recovered, Value::with_id(10_001), "chosen followers: {:?}", chosen);

            let suffixes = acceptsync_suffixes(&mut leader);
            assert!(
                suffixes
                    .iter()
                    .all(|suffix| suffix.first() == Some(&Value::with_id(10_001))),
                "all AcceptSync suffixes should start with the recovered majority entry; chosen followers: {:?}",
                chosen
            );
        } else {
            assert!(
                recovered_first_entry(&leader).is_none(),
                "leader should not recover without three matching follower promises; chosen followers: {:?}",
                chosen
            );
        }
    }
}

#[test]
#[serial]
fn prepare_recovery_unsynced_tie_depends_on_new_leader_test() {
    let all_triplets = follower_triplets(&(2..=7).map(|pid| (pid, ())).collect::<Vec<_>>());
    for triplet in all_triplets {
        let chosen: BTreeSet<_> = triplet.iter().map(|(pid, _)| *pid).collect();

        let nodes: Vec<NodeId> = (1..=7).collect();
        let old_leader = Ballot::with(1, 1, 0, 7);
        let mut leader = establish_follower_accept(1, &nodes, old_leader);
        inject_unsynced(&mut leader, 1, 90, 30_001, 3000);

        let mut followers: Vec<_> = (2..=7)
            .map(|pid| (pid, establish_follower_accept(pid, &nodes, old_leader)))
            .collect();
        for (pid, follower) in followers.iter_mut() {
            match *pid {
                2 | 3 => inject_unsynced(follower, *pid, 90, 30_001, 3000),
                4 | 5 => inject_unsynced(follower, *pid, 91, 31_001, 4000),
                6 | 7 => inject_unsynced(follower, *pid, 92, 32_001, 5000),
                _ => unreachable!(),
            }
        }

        let prepares = trigger_prepare(&mut leader);
        let mut promises = vec![];
        for (pid, follower) in followers.iter_mut() {
            let prepare = prepares
                .iter()
                .find(|msg| msg.to == *pid)
                .cloned()
                .expect("missing Prepare");
            promises.push((*pid, collect_promise(follower, prepare)));
        }

        let delivered: Vec<_> = promises
            .into_iter()
            .filter(|(pid, _)| chosen.contains(pid))
            .collect();
        deliver_promises(&mut leader, &delivered);

        let followers_supporting_leader = chosen.iter().filter(|pid| matches!(**pid, 2 | 3)).count();
        if followers_supporting_leader >= 2 {
            let recovered = recovered_first_entry(&leader)
                .expect("leader should recover its own candidate when two matching follower promises arrive");
            assert_eq!(
                recovered,
                Value::with_id(30_001),
                "the leader's own unsynced entry should break the tie; chosen followers: {:?}",
                chosen
            );
        } else {
            assert!(
                recovered_first_entry(&leader).is_none(),
                "leader should not recover without two matching followers backing its own unsynced entry; chosen followers: {:?}",
                chosen
            );
        }
    }
}

#[test]
#[serial]
fn prepare_recovery_unsynced_no_threshold_stops_recovery_test() {
    let all_triplets = follower_triplets(&(2..=7).map(|pid| (pid, ())).collect::<Vec<_>>());
    for triplet in all_triplets {
        let chosen: BTreeSet<_> = triplet.iter().map(|(pid, _)| *pid).collect();

        let nodes: Vec<NodeId> = (1..=7).collect();
        let old_leader = Ballot::with(1, 1, 0, 7);
        let mut leader = establish_follower_accept(1, &nodes, old_leader);
        let mut followers: Vec<_> = (2..=7)
            .map(|pid| (pid, establish_follower_accept(pid, &nodes, old_leader)))
            .collect();

        for (pid, follower) in followers.iter_mut() {
            match *pid {
                2 | 3 => inject_unsynced(follower, *pid, 90, 40_001, 6000),
                4 | 5 => inject_unsynced(follower, *pid, 91, 41_001, 7000),
                6 => inject_unsynced(follower, *pid, 92, 42_001, 8000),
                7 => {}
                _ => unreachable!(),
            }
        }

        let prepares = trigger_prepare(&mut leader);
        let mut promises = vec![];
        for (pid, follower) in followers.iter_mut() {
            let prepare = prepares
                .iter()
                .find(|msg| msg.to == *pid)
                .cloned()
                .expect("missing Prepare");
            promises.push((*pid, collect_promise(follower, prepare)));
        }
        let delivered: Vec<_> = promises
            .into_iter()
            .filter(|(pid, _)| chosen.contains(pid))
            .collect();
        deliver_promises(&mut leader, &delivered);

        assert!(
            recovered_first_entry(&leader).is_none(),
            "no unsynced candidate should be recovered without reaching the threshold; chosen followers: {:?}",
            chosen
        );

        leader.handle_incoming(Message::SequencePaxos(PaxosMessage {
            from: 99,
            to: 1,
            msg: PaxosMsg::DomPropose(DomPropose {
                entry_id: EntryId {
                    client_id: 99,
                    command_id: 50_001,
                },
                sender: 99,
                sent_time: (0, 0),
                deadline: 0,
                entry: Value::with_id(50_001),
            }),
        }));
        leader.poll();

        let first = recovered_first_entry(&leader).expect("leader should accept a new proposal");
        assert_eq!(first, Value::with_id(50_001), "chosen followers: {:?}", chosen);
    }
}

#[test]
#[serial]
fn prepare_recovery_stops_on_duplicate_entry_id_test() {
    let nodes: Vec<NodeId> = (1..=7).collect();
    let old_leader = Ballot::with(1, 1, 0, 7);
    let mut leader = establish_follower_accept(1, &nodes, old_leader);
    let mut followers: Vec<_> = (2..=7)
        .map(|pid| (pid, establish_follower_accept(pid, &nodes, old_leader)))
        .collect();

    let duplicated_entry_id = EntryId {
        client_id: 90,
        command_id: 60_001,
    };
    for (pid, follower) in followers.iter_mut() {
        if matches!(*pid, 2 | 3 | 4) {
            inject_unsynced_with_entry_id(
                follower,
                *pid,
                90,
                duplicated_entry_id,
                60_001,
                9000,
            );
            inject_unsynced_with_entry_id(
                follower,
                *pid,
                90,
                duplicated_entry_id,
                60_001,
                10_000,
            );
        }
    }

    let prepares = trigger_prepare(&mut leader);
    let mut promises = vec![];
    for (pid, follower) in followers.iter_mut() {
        let prepare = prepares
            .iter()
            .find(|msg| msg.to == *pid)
            .cloned()
            .expect("missing Prepare");
        promises.push((*pid, collect_promise(follower, prepare)));
    }

    let delivered: Vec<_> = promises
        .into_iter()
        .filter(|(pid, _)| matches!(*pid, 2 | 3 | 4))
        .collect();
    deliver_promises(&mut leader, &delivered);

    let recovered = read_all_values(&leader);
    assert_eq!(
        recovered,
        vec![Value::with_id(60_001)],
        "duplicate entry ids in the recovered tail must not be appended twice"
    );
}

#[test]
#[serial]
fn recovered_remote_entry_id_is_dropped_on_release_test() {
    let nodes: Vec<NodeId> = (1..=7).collect();
    let old_leader = Ballot::with(1, 1, 0, 7);
    let mut leader = establish_follower_accept(1, &nodes, old_leader);
    let mut followers: Vec<_> = (2..=7)
        .map(|pid| (pid, establish_follower_accept(pid, &nodes, old_leader)))
        .collect();

    let recovered_entry_id = EntryId {
        client_id: 91,
        command_id: 70_001,
    };
    for (pid, follower) in followers.iter_mut() {
        if matches!(*pid, 2 | 3 | 4) {
            inject_unsynced_with_entry_id(
                follower,
                *pid,
                91,
                recovered_entry_id,
                70_001,
                11_000,
            );
        }
    }

    let prepares = trigger_prepare(&mut leader);
    let mut promises = vec![];
    for (pid, follower) in followers.iter_mut() {
        let prepare = prepares
            .iter()
            .find(|msg| msg.to == *pid)
            .cloned()
            .expect("missing Prepare");
        promises.push((*pid, collect_promise(follower, prepare)));
    }

    let delivered: Vec<_> = promises
        .into_iter()
        .filter(|(pid, _)| matches!(*pid, 2 | 3 | 4))
        .collect();
    deliver_promises(&mut leader, &delivered);

    leader.handle_incoming(Message::SequencePaxos(PaxosMessage {
        from: 91,
        to: 1,
        msg: PaxosMsg::DomPropose(DomPropose {
            entry_id: recovered_entry_id,
            sender: 91,
            sent_time: (0, 0),
            deadline: 12_000,
            entry: Value::with_id(70_001),
        }),
    }));
    leader.poll();

    let recovered = read_all_values(&leader);
    assert_eq!(
        recovered,
        vec![Value::with_id(70_001)],
        "a DOM proposal with a recovered remote entry id must be dropped at release time"
    );
}
