use super::{
    ballot_leader_election::Ballot,
    messages::sequence_paxos::{AcceptedEntryMeta, EntryId, Promise},
    storage::{Entry, SnapshotType, StopSign},
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    fmt::Debug,
    hash::{DefaultHasher, Hash, Hasher},
    marker::PhantomData,
    time::{SystemTime, UNIX_EPOCH},
};

/// Struct for implementing hashes.
///
/// It is possible to create a default hash with value 0, or using a (entry_id, deadline) pair, and
/// to then extend an existing hash using another existing hash. They should also be comparable.
///
/// The extension uses xor (as in Nezha), it might be important to remember that this makes it
/// commutative. For the applications is should not matter however, the only case where an entry might
/// be out of order is if the leader adds it late, in which case the deadline should have been changed.
///
/// I'm sure that there are a thousand and one issues with the implementation, but hopefully it
/// works for the required circumstances.
#[derive(Clone, Copy, Debug, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct DOMHash {
    hash: u64,
}

impl DOMHash {
    pub(crate) fn with(entry_id: EntryId, deadline: i64) -> Self {
        let tuple = (entry_id, deadline);
        let mut hasher = DefaultHasher::new();
        tuple.hash(&mut hasher);
        Self {
            hash: hasher.finish(),
        }
    }

    pub(crate) fn extend_hash(&mut self, other: &Self) {
        self.hash ^= other.hash;
    }

    pub(crate) fn remove_hash(&mut self, other: &Self) {
        self.extend_hash(other);
    }
}

impl PartialEq for DOMHash {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for DOMHash {}

impl Default for DOMHash {
    fn default() -> Self {
        Self { hash: 0 }
    }
}

/// Struct used to help another server synchronize their log with the current state of our own log.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct LogSync<T>
where
    T: Entry,
{
    /// The decided snapshot.
    pub decided_snapshot: Option<SnapshotType<T>>,
    /// The log suffix.
    pub suffix: Vec<T>,
    /// The index of the log where the entries from `suffix` should be applied at (also the compacted idx of `decided_snapshot` if it exists).
    pub sync_idx: usize,
    /// The accepted StopSign.
    pub stopsign: Option<StopSign>,
}

/// Struct used to unsyced-log entries
#[derive(Clone, Debug, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
/// Struct used to keep track of unsynced log entries for the Project.
pub struct UnsyncedLogEntry<T: Entry> {
    /// The entry which was sent on the fast path and is not yet synced.
    pub entry: T,
    /// The entry id of the entry, used for checking if the entry is the same as other entries with the same index.
    pub entry_id: EntryId,
    /// The hash of the entry
    pub deadline: i64,
    /// The hash of the entry
    pub entry_hash: DOMHash,
    /// The hash of the prefix of the log before this entry, used for checking if the entry is on the same log as other entries with the same index.
    /// So, this is the hash of the log up to the previous entry, not including this entry.
    pub prefix_hash: DOMHash,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
/// Struct used to keep track of each entry's fast and slow path acceptors for the Project.
pub struct AcceptedMapEntry {
    /// The hash of the leader's candidate entry for this index.
    pub entry_hash: DOMHash,
    /// The hash of the prefix of the leader's log before this entry, used for checking if the fast path accepted entry is on the same log as other entries with the same index.
    pub prefix_hash: DOMHash,
    /// The set of followers which accepted this entry on the fast path
    /// The key of the map is a tuple of (prefix_hash, entry_hash) where prefix_hash is the hash of the prefix of the follower's log before this entry and entry_hash is the hash of the entry itself.
    pub fast: HashMap<(DOMHash, DOMHash), HashSet<NodeId>>,
    /// The set of followers which accepted this entry on the slow path
    pub slow: HashSet<NodeId>,
}

#[derive(Debug, Clone, Default)]
/// Promise without the log update
pub(crate) struct PromiseMetaData {
    pub n_accepted: Ballot,
    pub accepted_idx: usize,
    pub decided_idx: usize,
    pub pid: NodeId,
}

impl PartialOrd for PromiseMetaData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let ordering = if self.n_accepted == other.n_accepted
            && self.accepted_idx == other.accepted_idx
            && self.pid == other.pid
        {
            Ordering::Equal
        } else if self.n_accepted > other.n_accepted
            || (self.n_accepted == other.n_accepted && self.accepted_idx > other.accepted_idx)
        {
            Ordering::Greater
        } else {
            Ordering::Less
        };
        Some(ordering)
    }
}

impl PartialEq for PromiseMetaData {
    fn eq(&self, other: &Self) -> bool {
        self.n_accepted == other.n_accepted
            && self.accepted_idx == other.accepted_idx
            && self.pid == other.pid
    }
}

#[derive(Debug, Clone)]
/// The promise state of a node.
enum PromiseState {
    /// Not promised to any leader
    NotPromised,
    /// Promised to my ballot
    Promised(PromiseMetaData),
    /// Promised to a leader who's ballot is greater than mine
    PromisedHigher,
}

#[derive(Debug, Clone)]
pub(crate) struct LeaderState<T>
where
    T: Entry,
{
    pub n_leader: Ballot,
    promises_meta: Vec<PromiseState>,
    // the sequence number of accepts for each follower where AcceptSync has sequence number = 1
    follower_seq_nums: Vec<SequenceNumber>,
    pub accepted_indexes: Vec<usize>,
    max_promise_meta: PromiseMetaData,
    max_promise_sync: Option<LogSync<T>>,
    max_promise_accepted_hash: DOMHash,
    latest_accept_meta: Vec<Option<(Ballot, usize)>>, //  index in outgoing
    pub max_pid: usize,
    // The number of promises needed in the prepare phase to become synced and
    // the number of accepteds needed in the accept phase to decide an entry.
    pub quorum: Quorum,

    // Modifications for the Project
    accepted_map: HashMap<usize, AcceptedMapEntry>,
    /// unsynced_log_store\[pid\]\[idx\] = the unsynced log entry with index idx from the follower with id pid
    unsynced_log_store: Vec<HashMap<usize, UnsyncedLogEntry<T>>>,
    recovered_remote_entry_ids: HashSet<EntryId>,
    pending_accept_meta: VecDeque<AcceptedEntryMeta>,
}

impl<T> LeaderState<T>
where
    T: Entry,
{
    pub fn with(n_leader: Ballot, max_pid: usize, quorum: Quorum) -> Self {
        Self {
            n_leader,
            promises_meta: vec![PromiseState::NotPromised; max_pid],
            follower_seq_nums: vec![SequenceNumber::default(); max_pid],
            accepted_indexes: vec![0; max_pid],
            max_promise_meta: PromiseMetaData::default(),
            max_promise_sync: None,
            max_promise_accepted_hash: DOMHash::default(), // 0 hash by default
            latest_accept_meta: vec![None; max_pid],
            max_pid,
            quorum,
            accepted_map: HashMap::new(),
            unsynced_log_store: vec![HashMap::new(); max_pid],
            recovered_remote_entry_ids: HashSet::new(),
            pending_accept_meta: VecDeque::new(),
        }
    }

    fn pid_to_idx(pid: NodeId) -> usize {
        (pid - 1) as usize
    }

    // Resets `pid`'s accept sequence to indicate they are in the next session of accepts
    pub fn increment_seq_num_session(&mut self, pid: NodeId) {
        let idx = Self::pid_to_idx(pid);
        self.follower_seq_nums[idx].session += 1;
        self.follower_seq_nums[idx].counter = 0;
    }

    pub fn next_seq_num(&mut self, pid: NodeId) -> SequenceNumber {
        let idx = Self::pid_to_idx(pid);
        self.follower_seq_nums[idx].counter += 1;
        self.follower_seq_nums[idx]
    }

    pub fn get_seq_num(&mut self, pid: NodeId) -> SequenceNumber {
        self.follower_seq_nums[Self::pid_to_idx(pid)]
    }

    pub fn set_promise(&mut self, prom: Promise<T>, from: NodeId, check_max_prom: bool) -> bool {
        let promise_meta = PromiseMetaData {
            n_accepted: prom.n_accepted,
            accepted_idx: prom.accepted_idx,
            decided_idx: prom.decided_idx,
            pid: from,
        };
        if check_max_prom && promise_meta > self.max_promise_meta {
            self.max_promise_meta = promise_meta.clone();
            self.max_promise_sync = prom.log_sync;
            self.max_promise_accepted_hash = prom.log_prefix_hash;
        }
        self.promises_meta[Self::pid_to_idx(from)] = PromiseState::Promised(promise_meta);
        if let Some(unsynced_log) = prom.log_unsync {
            self.set_unsynced_log(from, unsynced_log);
        }
        let num_promised = self
            .promises_meta
            .iter()
            .filter(|p| matches!(p, PromiseState::Promised(_)))
            .count();
        self.quorum.is_prepare_quorum(num_promised)
    }

    pub fn reset_promise(&mut self, pid: NodeId) {
        self.promises_meta[Self::pid_to_idx(pid)] = PromiseState::NotPromised;
    }

    /// Node `pid` seen with ballot greater than my ballot
    pub fn lost_promise(&mut self, pid: NodeId) {
        self.promises_meta[Self::pid_to_idx(pid)] = PromiseState::PromisedHigher;
    }

    pub fn take_max_promise_sync(&mut self) -> Option<LogSync<T>> {
        std::mem::take(&mut self.max_promise_sync)
    }

    pub fn get_max_promise_meta(&self) -> &PromiseMetaData {
        &self.max_promise_meta
    }

    pub fn get_max_promise_accepted_hash(&self) -> &DOMHash {
        &self.max_promise_accepted_hash
    }

    pub fn get_max_decided_idx(&self) -> usize {
        self.promises_meta
            .iter()
            .filter_map(|p| match p {
                PromiseState::Promised(m) => Some(m.decided_idx),
                _ => None,
            })
            .max()
            .unwrap_or_default()
    }

    pub fn get_promise_meta(&self, pid: NodeId) -> &PromiseMetaData {
        match &self.promises_meta[Self::pid_to_idx(pid)] {
            PromiseState::Promised(metadata) => metadata,
            _ => panic!("No Metadata found for promised follower"),
        }
    }

    pub fn get_min_all_accepted_idx(&self) -> &usize {
        self.accepted_indexes
            .iter()
            .min()
            .expect("Should be all initialised to 0!")
    }

    pub fn reset_latest_accept_meta(&mut self) {
        self.latest_accept_meta = vec![None; self.max_pid];
    }

    pub fn get_promised_followers(&self) -> Vec<NodeId> {
        self.promises_meta
            .iter()
            .enumerate()
            .filter_map(|(idx, x)| match x {
                PromiseState::Promised(_) if idx != Self::pid_to_idx(self.n_leader.pid) => {
                    Some((idx + 1) as NodeId)
                }
                _ => None,
            })
            .collect()
    }

    /// The pids of peers which have not promised a higher ballot than mine.
    pub(crate) fn get_preparable_peers(&self, peers: &[NodeId]) -> Vec<NodeId> {
        peers
            .iter()
            .filter_map(|pid| {
                let idx = Self::pid_to_idx(*pid);
                match self.promises_meta.get(idx).unwrap() {
                    PromiseState::NotPromised => Some(*pid),
                    _ => None,
                }
            })
            .collect()
    }

    pub fn set_latest_accept_meta(&mut self, pid: NodeId, idx: Option<usize>) {
        let meta = idx.map(|x| (self.n_leader, x));
        self.latest_accept_meta[Self::pid_to_idx(pid)] = meta;
    }

    pub fn set_accepted_idx(&mut self, pid: NodeId, idx: usize) {
        self.accepted_indexes[Self::pid_to_idx(pid)] = idx;
    }

    pub fn get_latest_accept_meta(&self, pid: NodeId) -> Option<(Ballot, usize)> {
        self.latest_accept_meta
            .get(Self::pid_to_idx(pid))
            .unwrap()
            .as_ref()
            .copied()
    }

    pub fn get_decided_idx(&self, pid: NodeId) -> Option<usize> {
        match self.promises_meta.get(Self::pid_to_idx(pid)).unwrap() {
            PromiseState::Promised(metadata) => Some(metadata.decided_idx),
            _ => None,
        }
    }

    pub fn get_accepted_idx(&self, pid: NodeId) -> usize {
        *self.accepted_indexes.get(Self::pid_to_idx(pid)).unwrap()
    }

    pub fn is_chosen(&self, idx: usize) -> bool {
        let num_accepted = self
            .accepted_indexes
            .iter()
            .filter(|la| **la >= idx)
            .count();
        self.quorum.is_accept_quorum(num_accepted)
    }

    pub fn set_unsynced_log(
        &mut self,
        pid: NodeId,
        unsynced_log: HashMap<usize, UnsyncedLogEntry<T>>,
    ) {
        let id = Self::pid_to_idx(pid);
        if id < self.unsynced_log_store.len() {
            self.unsynced_log_store[id] = unsynced_log;
        } else {
            // If idx is out of bounds, we can choose to either ignore it or handle it as needed.
            // For now, we'll just ignore it.
        }
    }

    pub fn get_unsynced_log_store(&self) -> &Vec<HashMap<usize, UnsyncedLogEntry<T>>> {
        &self.unsynced_log_store
    }

    pub fn insert_recovered_remote_entry_id(&mut self, entry_id: EntryId) {
        self.recovered_remote_entry_ids.insert(entry_id);
    }

    pub fn has_recovered_remote_entry_id(&self, entry_id: &EntryId) -> bool {
        self.recovered_remote_entry_ids.contains(entry_id)
    }

    pub fn push_pending_accept_meta(&mut self, meta: AcceptedEntryMeta) {
        self.pending_accept_meta.push_back(meta);
    }

    pub fn take_pending_accept_meta(&mut self, count: usize) -> Vec<AcceptedEntryMeta> {
        assert!(
            self.pending_accept_meta.len() >= count,
            "missing pending accept metadata"
        );
        self.pending_accept_meta.drain(..count).collect()
    }

    /// Returns [(entry_id, entry, count), ...] for all entries in the unsynced log store with the idx = `entry_idx` and prev_hash = `prev_hash`.
    pub fn get_matched_unsynced_entries(
        &self,
        entry_idx: usize,
        prefix_hash: DOMHash,
    ) -> Vec<(DOMHash, EntryId, T, usize)> {
        let mut counts: HashMap<DOMHash, usize> = HashMap::new();
        let mut entry_map: HashMap<DOMHash, (EntryId, T)> = HashMap::new();

        for unsynced_log in &self.unsynced_log_store {
            if let Some(unsynced_entry) = unsynced_log.get(&entry_idx) {
                if unsynced_entry.prefix_hash == prefix_hash {
                    *counts.entry(unsynced_entry.entry_hash).or_insert(0) += 1;
                    entry_map.insert(
                        unsynced_entry.entry_hash,
                        (unsynced_entry.entry_id, unsynced_entry.entry.clone()),
                    );
                }
            }
        }

        counts
            .into_iter()
            .map(|(entry_hash, count)| {
                (
                    entry_hash,
                    entry_map[&entry_hash].0,
                    entry_map[&entry_hash].1.clone(),
                    count,
                )
            })
            .collect()
    }

    pub fn set_accepted_map(
        &mut self,
        idx: usize,
        entry_hash: DOMHash,
        prefix_hash: DOMHash,
        pid: NodeId,
        is_fast_path: bool,
    ) -> &mut AcceptedMapEntry {
        let accepted_entry = self
            .accepted_map
            .entry(idx)
            .or_insert_with(|| AcceptedMapEntry {
                entry_hash,
                prefix_hash: prefix_hash.clone(),
                fast: HashMap::new(),
                slow: HashSet::new(),
            });

        if is_fast_path {
            accepted_entry
                .fast
                .entry((prefix_hash, entry_hash))
                .or_insert_with(HashSet::new)
                .insert(pid);
        } else {
            accepted_entry.slow.insert(pid);
        }
        accepted_entry
    }

    pub fn get_accepted_map(&self, idx: usize) -> Option<&AcceptedMapEntry> {
        self.accepted_map.get(&idx)
    }

    /// Prunes the accepted map up to the decided index.
    pub fn prune_accepted_map(&mut self, decided_idx: usize) {
        self.accepted_map.retain(|&idx, _| idx > decided_idx);
    }
}

/// The entry read in the log.
#[derive(Debug, Clone)]
pub enum LogEntry<T>
where
    T: Entry,
{
    /// The entry is decided.
    Decided(T),
    /// The entry is NOT decided. Might be removed from the log at a later time.
    Undecided(T),
    /// The entry has been trimmed.
    Trimmed(TrimmedIndex),
    /// The entry has been snapshotted.
    Snapshotted(SnapshottedEntry<T>),
    /// This Sequence Paxos instance has been stopped for reconfiguration. The accompanying bool
    /// indicates whether the reconfiguration has been decided or not. If it is `true`, then the OmniPaxos instance for the new configuration can be started.
    StopSign(StopSign, bool),
}

impl<T: PartialEq + Entry> PartialEq for LogEntry<T>
where
    <T as Entry>::Snapshot: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (LogEntry::Decided(v1), LogEntry::Decided(v2)) => v1 == v2,
            (LogEntry::Undecided(v1), LogEntry::Undecided(v2)) => v1 == v2,
            (LogEntry::Trimmed(idx1), LogEntry::Trimmed(idx2)) => idx1 == idx2,
            (LogEntry::Snapshotted(s1), LogEntry::Snapshotted(s2)) => s1 == s2,
            (LogEntry::StopSign(ss1, b1), LogEntry::StopSign(ss2, b2)) => ss1 == ss2 && b1 == b2,
            _ => false,
        }
    }
}

/// Convenience struct for checking if a certain index exists, is compacted or is a StopSign.
#[derive(Debug, Clone)]
pub(crate) enum IndexEntry {
    Entry,
    Compacted,
    StopSign(StopSign),
}

#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct SnapshottedEntry<T>
where
    T: Entry,
{
    pub trimmed_idx: TrimmedIndex,
    pub snapshot: T::Snapshot,
    _p: PhantomData<T>,
}

impl<T> SnapshottedEntry<T>
where
    T: Entry,
{
    pub(crate) fn with(trimmed_idx: usize, snapshot: T::Snapshot) -> Self {
        Self {
            trimmed_idx,
            snapshot,
            _p: PhantomData,
        }
    }
}

impl<T: Entry> PartialEq for SnapshottedEntry<T>
where
    <T as Entry>::Snapshot: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.trimmed_idx == other.trimmed_idx && self.snapshot == other.snapshot
    }
}

pub(crate) mod defaults {
    pub(crate) const BUFFER_SIZE: usize = 100000;
    pub(crate) const BLE_BUFFER_SIZE: usize = 100;
    pub(crate) const ELECTION_TIMEOUT: u64 = 1;
    pub(crate) const RESEND_MESSAGE_TIMEOUT: u64 = 100;
    pub(crate) const FLUSH_BATCH_TIMEOUT: u64 = 200;
    pub(crate) const RELEASE_REQUESTS_TIMEOUT: u64 = 1;
}

#[allow(missing_docs)]
pub type TrimmedIndex = usize;

/// ID for an OmniPaxos node
pub type NodeId = u64;
/// ID for an OmniPaxos configuration (i.e., the set of servers in an OmniPaxos cluster)
pub type ConfigurationId = u32;

/// Error message to display when there was an error reading to the storage implementation.
pub const READ_ERROR_MSG: &str = "Error reading from storage.";
/// Error message to display when there was an error writing to the storage implementation.
pub const WRITE_ERROR_MSG: &str = "Error writing to storage.";

/// Used for checking the ordering of message sequences in the accept phase
#[derive(PartialEq, Eq)]
pub(crate) enum MessageStatus {
    /// Expected message sequence progression
    Expected,
    /// Identified a message sequence break
    DroppedPreceding,
    /// An already identified message sequence break
    Outdated,
}

/// Keeps track of the ordering of messages in the accept phase
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SequenceNumber {
    /// Meant to refer to a TCP session
    pub session: u64,
    /// The sequence number with respect to a session
    pub counter: u64,
}

impl SequenceNumber {
    /// Compares this sequence number with the sequence number of an incoming message.
    pub(crate) fn check_msg_status(&self, msg_seq_num: SequenceNumber) -> MessageStatus {
        if msg_seq_num.session == self.session && msg_seq_num.counter == self.counter + 1 {
            MessageStatus::Expected
        } else if msg_seq_num <= *self {
            MessageStatus::Outdated
        } else {
            MessageStatus::DroppedPreceding
        }
    }
}

pub(crate) struct LogicalClock {
    time: u64,
    timeout: u64,
}

impl LogicalClock {
    pub fn with(timeout: u64) -> Self {
        Self { time: 0, timeout }
    }

    pub fn tick_and_check_timeout(&mut self) -> bool {
        self.time += 1;
        if self.time == self.timeout {
            self.time = 0;
            true
        } else {
            false
        }
    }
}

/// Trait for obtaining physical time with uncertainty.
pub trait PhysicalClock {
    /// Returns the current physical time.
    fn get_time(&self) -> i64;
    /// Returns the uncertainty of the physical time.
    fn get_uncertainty(&self) -> i64;
    /// Returns the pair of the current physical time and its uncertainty.
    fn get_time_with_uncertainty(&self) -> (i64, i64);
}

/// System clock that returns UTC nanoseconds since Unix epoch.
pub struct SystemClock;

impl PhysicalClock for SystemClock {
    fn get_time(&self) -> i64 {
        let nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_nanos(),
            Err(_) => 0,
        };
        if nanos > i64::MAX as u128 {
            i64::MAX
        } else {
            nanos as i64
        }
    }

    fn get_uncertainty(&self) -> i64 {
        0
    }

    fn get_time_with_uncertainty(&self) -> (i64, i64) {
        (self.get_time(), self.get_uncertainty())
    }
}

/// Global system clock instance.
pub static SYSTEM_CLOCK: SystemClock = SystemClock;

/// Flexible quorums can be used to increase/decrease the read and write quorum sizes,
/// for different latency vs fault tolerance tradeoffs.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(any(feature = "serde", feature = "toml_config"), derive(Deserialize))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct FlexibleQuorum {
    /// The number of nodes a leader needs to consult to get an up-to-date view of the log.
    pub read_quorum_size: usize,
    /// The number of acknowledgments a leader needs to commit an entry to the log
    pub write_quorum_size: usize,
}

/// The type of quorum used by the OmniPaxos cluster.
#[derive(Copy, Clone, Debug)]
pub(crate) enum Quorum {
    /// Both the read quorum and the write quorums are a majority of nodes
    Majority(usize),
    /// The read and write quorum sizes are defined by a `FlexibleQuorum`
    Flexible(FlexibleQuorum),
}

impl Quorum {
    pub(crate) fn with(flexible_quorum_config: Option<FlexibleQuorum>, num_nodes: usize) -> Self {
        match flexible_quorum_config {
            Some(FlexibleQuorum {
                read_quorum_size,
                write_quorum_size,
            }) => Quorum::Flexible(FlexibleQuorum {
                read_quorum_size,
                write_quorum_size,
            }),
            None => Quorum::Majority(num_nodes / 2 + 1),
        }
    }

    pub(crate) fn is_prepare_quorum(&self, num_nodes: usize) -> bool {
        match self {
            Quorum::Majority(majority) => num_nodes >= *majority,
            Quorum::Flexible(flex_quorum) => num_nodes >= flex_quorum.read_quorum_size,
        }
    }

    pub(crate) fn is_accept_quorum(&self, num_nodes: usize) -> bool {
        match self {
            Quorum::Majority(majority) => num_nodes >= *majority,
            Quorum::Flexible(flex_quorum) => num_nodes >= flex_quorum.write_quorum_size,
        }
    }
}

/// The entries flushed due to an append operation
pub(crate) struct AcceptedMetaData<T: Entry> {
    pub accepted_idx: usize,
    #[cfg(not(feature = "unicache"))]
    pub entries: Vec<T>,
}

#[cfg(not(feature = "unicache"))]
#[cfg(test)]
mod tests {
    use super::*; // Import functions and types from this module
    use crate::storage::NoSnapshot;

    impl Entry for () {
        type Snapshot = NoSnapshot;
    }

    #[test]
    fn preparable_peers_test() {
        type Value = ();

        let nodes = vec![6, 7, 8];
        let quorum = Quorum::Majority(2);
        let max_pid = 8;
        let leader_state =
            LeaderState::<Value>::with(Ballot::with(1, 1, 1, max_pid), max_pid as usize, quorum);
        let prep_peers = leader_state.get_preparable_peers(&nodes);
        assert_eq!(prep_peers, nodes);

        let nodes = vec![7, 1, 100, 4, 6];
        let quorum = Quorum::Majority(3);
        let max_pid = 100;
        let leader_state =
            LeaderState::<Value>::with(Ballot::with(1, 1, 1, max_pid), max_pid as usize, quorum);
        let prep_peers = leader_state.get_preparable_peers(&nodes);
        assert_eq!(prep_peers, nodes);
    }

    #[test]
    fn pending_accept_meta_queue_preserves_order() {
        type Value = ();

        let quorum = Quorum::Majority(2);
        let mut leader_state = LeaderState::<Value>::with(Ballot::with(1, 1, 1, 3), 3, quorum);
        let meta_1 = AcceptedEntryMeta {
            entry_id: EntryId {
                client_id: 7,
                command_id: 1,
            },
            deadline: 11,
        };
        let meta_2 = AcceptedEntryMeta {
            entry_id: EntryId {
                client_id: 7,
                command_id: 2,
            },
            deadline: 12,
        };

        leader_state.push_pending_accept_meta(meta_1);
        leader_state.push_pending_accept_meta(meta_2);

        assert_eq!(leader_state.take_pending_accept_meta(1), vec![meta_1]);
        assert_eq!(leader_state.take_pending_accept_meta(1), vec![meta_2]);
    }
}
