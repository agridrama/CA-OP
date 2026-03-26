use crate::{
    messages::{ballot_leader_election::BLEMessage, sequence_paxos::PaxosMessage},
    storage::Entry,
    util::NodeId,
};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Internal component for log replication
pub mod sequence_paxos {
    use crate::{
        ballot_leader_election::Ballot,
        storage::{Entry, StopSign},
        util::{DOMHash, LogSync, NodeId, SequenceNumber, UnsyncedLogEntry},
    };
    #[cfg(feature = "serde")]
    use serde::{Deserialize, Serialize};
    use std::{collections::HashMap, fmt::Debug};

    /// Message sent by a follower on crash-recovery or dropped messages to request its leader to re-prepare them.
    #[derive(Copy, Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct PrepareReq {
        /// The current round.
        pub n: Ballot,
    }

    /// Prepare message sent by a newly-elected leader to initiate the Prepare phase.
    #[derive(Copy, Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct Prepare {
        /// The current round.
        pub n: Ballot,
        /// The decided index of this leader.
        pub decided_idx: usize,
        /// The latest round in which an entry was accepted.
        pub n_accepted: Ballot,
        /// The log length of this leader.
        pub accepted_idx: usize,
    }

    /// Promise message sent by a follower in response to a [`Prepare`] sent by the leader.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct Promise<T>
    where
        T: Entry,
    {
        /// The current round.
        pub n: Ballot,
        /// The latest round in which an entry was accepted.
        pub n_accepted: Ballot,
        /// The decided index of this follower.
        pub decided_idx: usize,
        /// The log length of this follower.
        pub accepted_idx: usize,
        /// The log update which the leader applies to its log in order to sync
        /// with this follower (if the follower is more up-to-date).
        pub log_sync: Option<LogSync<T>>,
        /// The unsynced-log entries of this follower
        pub log_unsync: Option<HashMap<usize, UnsyncedLogEntry<T>>>,
        /// The hash value for the log prefix up to the entries in `log_sync`.
        pub log_prefix_hash: DOMHash,
    }

    /// AcceptSync message sent by the leader to synchronize the logs of all replicas in the prepare phase.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct AcceptSync<T>
    where
        T: Entry,
    {
        /// The current round.
        pub n: Ballot,
        /// The sequence number of this message in the leader-to-follower accept sequence
        pub seq_num: SequenceNumber,
        /// The decided index
        pub decided_idx: usize,
        /// The log update which the follower applies to its log in order to sync
        /// with the leader.
        pub log_sync: LogSync<T>,
        /// The hash value for the log prefix up to the entries in `log_sync`.
        pub log_prefix_hash: DOMHash,
    }

    /// Message with entries to be replicated and the latest decided index sent by the leader in the accept phase.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct AcceptDecide<T>
    where
        T: Entry,
    {
        /// The current round.
        pub n: Ballot,
        /// The sequence number of this message in the leader-to-follower accept sequence
        pub seq_num: SequenceNumber,
        /// The decided index.
        pub decided_idx: usize,
        #[cfg(not(feature = "unicache"))]
        /// Entries to be replicated.
        pub entries: Vec<T>,
        /// DOM metadata for each entry in `entries`, in the same order.
        pub entry_meta: Vec<AcceptedEntryMeta>,
        /// The hash value for the log prefix up to the entries in `entries`.
        pub log_prefix_hash: DOMHash,
    }

    /// Message sent by follower to leader when entries has been accepted.
    #[derive(Copy, Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct Accepted {
        /// The current round.
        pub n: Ballot,
        /// The accepted index.
        pub accepted_idx: usize,
    }

    /// DOM metadata for a log entry replicated via `AcceptDecide`.
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct AcceptedEntryMeta {
        /// The unique id of the client request.
        pub entry_id: EntryId,
        /// The deadline assigned by DOM.
        pub deadline: i64,
    }

    /// FastAccepted message
    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct FastAccepted {
        /// The current round.
        pub n: Ballot,
        /// The index in the log where the follower placed this entry.
        pub idx: usize,
        /// Hash of the current entry
        pub entry_hash: DOMHash,
        /// Hash of the follower's log prefix including this entry
        pub prefix_hash: DOMHash,
    }

    /// Message sent by leader to followers to decide up to a certain index in the log.
    #[derive(Copy, Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct Decide {
        /// The current round.
        pub n: Ballot,
        /// The sequence number of this message in the leader-to-follower accept sequence
        pub seq_num: SequenceNumber,
        /// The decided index.
        pub decided_idx: usize,
    }

    /// Message sent by leader to followers to accept a StopSign
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct AcceptStopSign {
        /// The current round.
        pub n: Ballot,
        /// The sequence number of this message in the leader-to-follower accept sequence
        pub seq_num: SequenceNumber,
        /// The decided index.
        pub ss: StopSign,
    }

    /// Message sent by follower to leader when accepting an entry is rejected.
    /// This happens when the follower is promised to a greater leader.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct NotAccepted {
        /// The follower's current ballot
        pub n: Ballot,
    }

    /// The id of a command issued by a client. Same type as in OmniPaxos-KV
    pub type CommandId = usize;

    /// Represents a clock timestamp, with timestamp and uncertainty.
    /// TODO make this a struct and move it elsewhere as its not a message type
    pub(crate) type ClockTimestamp = (i64, i64);

    /// Type that uniquely identifies a client request.
    #[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct EntryId {
        /// The id of the client
        pub client_id: NodeId,
        /// The id of the client command
        pub command_id: CommandId,
    }

    /// Message sent by the DOM to all replicas, which is essentially a wrapper
    /// around the original command but tagged with a deadline and sent time.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct DomPropose<T>
    where
        T: Entry,
    {
        /// The unique id of the client request
        pub entry_id: EntryId,
        /// The sender of the proposal
        pub sender: NodeId,
        /// The time at which the proposal was sent by the DOM
        pub sent_time: ClockTimestamp,
        /// The global time deadline for the proposal
        pub deadline: i64,
        /// The entry to be replicated
        pub entry: T,
    }

    /// Message sent as a response to a DomPropose message.
    #[derive(Copy, Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct DomAck {
        /// The sender of the ack.
        pub sender: NodeId,
        /// The estimated one-way delay for the corresponding proposal.
        pub estimated_owd: i64,
    }

    /// Compaction Request
    #[allow(missing_docs)]
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub enum Compaction {
        Trim(usize),
        Snapshot(Option<usize>),
    }

    /// An enum for all the different message types.
    #[allow(missing_docs)]
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub enum PaxosMsg<T>
    where
        T: Entry,
    {
        /// Request a [`Prepare`] to be sent from the leader. Used for fail-recovery.
        PrepareReq(PrepareReq),
        #[allow(missing_docs)]
        Prepare(Prepare),
        Promise(Promise<T>),
        AcceptSync(AcceptSync<T>),
        AcceptDecide(AcceptDecide<T>),
        Accepted(Accepted),
        FastAccepted(FastAccepted),
        NotAccepted(NotAccepted),
        Decide(Decide),
        // We use the DOM instead of forwarding client proposals
        // /// Forward client proposals to the leader.
        // ProposalForward(Vec<T>),
        Compaction(Compaction),
        AcceptStopSign(AcceptStopSign),
        ForwardStopSign(StopSign),
        DomPropose(DomPropose<T>),
        DomAck(DomAck),
    }

    /// A struct for a Paxos message that also includes sender and receiver.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct PaxosMessage<T>
    where
        T: Entry,
    {
        /// Sender of `msg`.
        pub from: NodeId,
        /// Receiver of `msg`.
        pub to: NodeId,
        /// The message content.
        pub msg: PaxosMsg<T>,
    }
}

/// The different messages BLE uses to communicate with other servers.
pub mod ballot_leader_election {

    use crate::{ballot_leader_election::Ballot, util::NodeId};
    #[cfg(feature = "serde")]
    use serde::{Deserialize, Serialize};

    /// An enum for all the different BLE message types.
    #[allow(missing_docs)]
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub enum HeartbeatMsg {
        Request(HeartbeatRequest),
        Reply(HeartbeatReply),
    }

    /// Requests a reply from all the other servers.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct HeartbeatRequest {
        /// Number of the current round.
        pub round: u32,
    }

    /// Replies
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct HeartbeatReply {
        /// Number of the current heartbeat round.
        pub round: u32,
        /// Ballot of replying server.
        pub ballot: Ballot,
        /// Leader this server is following
        pub leader: Ballot,
        /// Whether the replying server sees a need for a new leader
        pub happy: bool,
    }

    /// A struct for a Paxos message that also includes sender and receiver.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct BLEMessage {
        /// Sender of `msg`.
        pub from: NodeId,
        /// Receiver of `msg`.
        pub to: NodeId,
        /// The message content.
        pub msg: HeartbeatMsg,
    }
}

#[allow(missing_docs)]
/// Message in OmniPaxos. Can be either a `SequencePaxos` message (for log replication) or `BLE` message (for leader election)
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Message<T>
where
    T: Entry,
{
    SequencePaxos(PaxosMessage<T>),
    BLE(BLEMessage),
}

impl<T> Message<T>
where
    T: Entry,
{
    /// Get the sender id of the message
    pub fn get_sender(&self) -> NodeId {
        match self {
            Message::SequencePaxos(p) => p.from,
            Message::BLE(b) => b.from,
        }
    }

    /// Get the receiver id of the message
    pub fn get_receiver(&self) -> NodeId {
        match self {
            Message::SequencePaxos(p) => p.to,
            Message::BLE(b) => b.to,
        }
    }
}
