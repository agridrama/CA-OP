pub mod utils;

use kompact::prelude::{promise, Ask, FutureCollection};
use omnipaxos::util::{LogEntry, NodeId};
use serial_test::serial;
use std::mem;
use utils::{
    verification::verify_log_unordered,
    StorageType, TestConfig, TestSystem, Value,
};

fn restart_node(sys: &mut TestSystem, cfg: &TestConfig, pid: NodeId) {
    let storage_path = sys.temp_dir_path.clone();
    let storage: StorageType<Value> =
        StorageType::with(cfg.storage_type, &format!("{storage_path}{pid}"));
    sys.create_node(pid, cfg, storage);
    sys.start_node(pid);
}

fn collect_logs(sys: &TestSystem) -> Vec<(NodeId, Vec<LogEntry<Value>>)> {
    sys.nodes
        .iter()
        .map(|(pid, node)| {
            (
                *pid,
                node.on_definition(|x| x.paxos.read_decided_suffix(0).unwrap_or_default()),
            )
        })
        .collect()
}

fn assert_all_logs_match_expected(logs: &[(NodeId, Vec<LogEntry<Value>>)], expected: &[Value]) {
    for (_, log) in logs {
        verify_log_unordered(log.clone(), expected.to_vec());
    }
}

#[test]
#[serial]
fn followers_crash_rejoin_slow_path_progress_test() {
    let cfg = TestConfig::load("reconnect_test").expect("Test config loaded");
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let initial: Vec<Value> = (1..=5).map(Value::with_id).collect();
    sys.make_proposals(1, initial.clone(), cfg.wait_timeout);
    let leader_id = sys.get_elected_leader(1, cfg.wait_timeout);

    let crashed_followers: Vec<_> = (1..=cfg.num_nodes as NodeId)
        .filter(|pid| *pid != leader_id)
        .take(2)
        .collect();
    for pid in &crashed_followers {
        sys.kill_node(*pid);
    }

    let during_crash: Vec<Value> = (6..=12).map(Value::with_id).collect();
    sys.make_proposals(leader_id, during_crash.clone(), cfg.wait_timeout);

    let surviving_logs = collect_logs(&sys);
    assert_eq!(surviving_logs.len(), 3, "only three nodes should remain alive");
    let expected_during_crash: Vec<Value> = initial
        .clone()
        .into_iter()
        .chain(during_crash.clone().into_iter())
        .collect();
    assert_all_logs_match_expected(&surviving_logs, &expected_during_crash);

    let mut recovered_futures = vec![];
    for pid in crashed_followers {
        restart_node(&mut sys, &cfg, pid);
        let node = sys.nodes.get(&pid).expect("restarted node missing");
        let (kprom, kfuture) = promise::<()>();
        let last = Value::with_id(18);
        node.on_definition(|x| {
            x.insert_decided_future(Ask::new(kprom, last));
        });
        recovered_futures.push(kfuture);
    }

    let after_rejoin: Vec<Value> = (13..=18).map(Value::with_id).collect();
    sys.make_proposals(leader_id, after_rejoin.clone(), cfg.wait_timeout);
    FutureCollection::collect_with_timeout::<Vec<_>>(recovered_futures, 3 * cfg.wait_timeout)
        .expect("recovered followers did not catch up after rejoin");

    let expected_log: Vec<Value> = initial
        .into_iter()
        .chain(during_crash.into_iter())
        .chain(after_rejoin.into_iter())
        .collect();
    let logs = collect_logs(&sys);
    assert_all_logs_match_expected(&logs, &expected_log);

    let kompact_system =
        mem::take(&mut sys.kompact_system).expect("No KompactSystem found in memory");
    kompact_system.shutdown().expect("Error on kompact shutdown");
}

#[test]
#[serial]
fn followers_crash_rejoin_resyncs_missed_entries_test() {
    let cfg = TestConfig::load("reconnect_test").expect("Test config loaded");
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    let initial: Vec<Value> = (100..=104).map(Value::with_id).collect();
    sys.make_proposals(1, initial.clone(), cfg.wait_timeout);
    let leader_id = sys.get_elected_leader(1, cfg.wait_timeout);

    let crashed_followers: Vec<_> = (1..=cfg.num_nodes as NodeId)
        .filter(|pid| *pid != leader_id)
        .take(2)
        .collect();
    for pid in &crashed_followers {
        sys.kill_node(*pid);
    }

    let missed: Vec<Value> = (105..=112).map(Value::with_id).collect();
    sys.make_proposals(leader_id, missed.clone(), cfg.wait_timeout);

    let mut recovered_futures = vec![];
    for pid in crashed_followers {
        restart_node(&mut sys, &cfg, pid);
        let node = sys.nodes.get(&pid).expect("restarted node missing");
        let (kprom, kfuture) = promise::<()>();
        let last = Value::with_id(113);
        node.on_definition(|x| {
            x.insert_decided_future(Ask::new(kprom, last));
        });
        recovered_futures.push(kfuture);
    }

    let tail = vec![Value::with_id(113)];
    sys.make_proposals(leader_id, tail.clone(), cfg.wait_timeout);
    FutureCollection::collect_with_timeout::<Vec<_>>(recovered_futures, 3 * cfg.wait_timeout)
        .expect("recovered followers did not catch up to the tail proposal");

    let expected_log: Vec<Value> = initial
        .into_iter()
        .chain(missed.into_iter())
        .chain(tail.into_iter())
        .collect();
    let logs = collect_logs(&sys);
    assert_all_logs_match_expected(&logs, &expected_log);

    let kompact_system =
        mem::take(&mut sys.kompact_system).expect("No KompactSystem found in memory");
    kompact_system.shutdown().expect("Error on kompact shutdown");
}
