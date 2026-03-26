use crate::dom::buffer::{EarlyBuffer, EarlyBufferInsertError};
use crate::dom::owd_estimator::{OutgoingOwdTracker, OwdEstimator, OwdSample};
use crate::messages::sequence_paxos::{ClockTimestamp, DomAck, DomPropose, EntryId};
use crate::sequence_paxos::Role;
use crate::storage::Entry;
use crate::util::{NodeId, PhysicalClock};
use std::cmp::max;
#[cfg(feature = "benchmark")]
use std::collections::HashMap;

mod buffer;
mod owd_estimator;
pub use owd_estimator::{EstimatorStrategy, OwdEstimatorConfig};

const ONE_MICRO_SECOND: i64 = 1000;

/// The Deadline-Ordered-Multicast abstraction.
pub(crate) struct Dom<'a, T, C>
where
    T: Entry,
    C: PhysicalClock,
{
    pid: NodeId,
    clock: &'a C,
    incoming_owd_estimates: OwdEstimator,
    outgoing_owd_estimates: OutgoingOwdTracker,
    early_buffer: EarlyBuffer<T>,
}

#[cfg(feature = "benchmark")]
/// Snapshot of DOM one-way delay estimates used for benchmark output.
pub struct DomOwdSnapshot {
    /// Current incoming OWD estimates keyed by sender node id.
    pub incoming_estimates: HashMap<NodeId, i64>,
    /// Current outgoing OWD estimates keyed by receiver node id.
    pub outgoing_estimates: HashMap<NodeId, i64>,
    /// Maximum outgoing OWD estimate across all peers.
    pub max_outgoing_estimate: i64,
}

impl<'a, T, C> Dom<'a, T, C>
where
    T: Entry,
    C: PhysicalClock,
{
    /// Creates a new instance of the DOM abstraction.
    pub(crate) fn new(config: OwdEstimatorConfig, clock: &'a C, pid: NodeId) -> Self {
        Self {
            pid,
            clock,
            incoming_owd_estimates: OwdEstimator::with_config(config),
            outgoing_owd_estimates: OutgoingOwdTracker::new(config.max_owd()),
            early_buffer: EarlyBuffer::new(),
        }
    }

    /// Creates a new DOM proposal for the given entry.
    pub(crate) fn create_dom_propose(&self, entry: T, entry_id: EntryId) -> DomPropose<T> {
        let sent_time: ClockTimestamp = self.clock.get_time_with_uncertainty();

        DomPropose {
            entry_id,
            sender: self.pid,
            sent_time,
            deadline: self.get_deadline(sent_time.0),
            entry,
        }
    }

    fn get_deadline(&self, time: i64) -> i64 {
        // TODO do we need this to be more pessimistic? E.g. deadline = OWD + uncertainty
        time + self.outgoing_owd_estimates.get_max_owd()
    }

    /// Returns all proposals in the early buffer with a deadline that is
    /// before the current time.
    /// TODO Should we make the return type Vec<T> instead?
    pub(crate) fn release_ready(&mut self) -> Vec<DomPropose<T>> {
        let now = self.clock.get_time_with_uncertainty();
        let mut proposals = Vec::new();

        while let Some(prop) = self.early_buffer.pop_ready(now.0 - now.1) {
            proposals.push(prop);
        }

        proposals
    }

    /// Handles a DOM proposal for the given role. Returns an ack with the
    /// estimated one-way delay of the proposal to be returned to the sender
    pub(crate) fn handle_dom_propose(&mut self, prop: DomPropose<T>, role: Role) -> DomAck {
        let received_time = self.clock.get_time_with_uncertainty();
        let owd_sample = OwdSample::new(
            prop.sent_time.0,
            received_time.0,
            prop.sent_time.1,
            received_time.1,
        );

        self.incoming_owd_estimates.update(prop.sender, owd_sample);

        let ack = DomAck {
            sender: self.pid,
            estimated_owd: self.incoming_owd_estimates.estimate_for(prop.sender),
        };

        match self.early_buffer.insert(prop) {
            Ok(_) => {}
            Err(EarlyBufferInsertError::Late(prop)) => {
                if role == Role::Leader {
                    let prop = self.extend_deadline(prop);
                    self.early_buffer
                        .insert(prop)
                        .expect("proposal cannot be late");
                }
            }
        };

        ack
    }

    fn extend_deadline(&self, prop: DomPropose<T>) -> DomPropose<T> {
        let last_deadline = self.early_buffer.last_released_deadline();
        let now = self.clock.get_time_with_uncertainty();
        let now_with_uncertainty = now.0 + now.1;

        let new_deadline = match last_deadline {
            Some(last) => max(now_with_uncertainty, last + ONE_MICRO_SECOND),
            None => now_with_uncertainty,
        };

        DomPropose {
            entry_id: prop.entry_id,
            sender: prop.sender,
            sent_time: prop.sent_time,
            deadline: new_deadline,
            entry: prop.entry,
        }
    }

    /// Handles a DOM ack.
    pub(crate) fn handle_dom_ack(&mut self, ack: DomAck) {
        self.outgoing_owd_estimates
            .update(ack.sender, ack.estimated_owd);
    }

    #[cfg(feature = "benchmark")]
    pub(crate) fn owd_snapshot(&self) -> DomOwdSnapshot {
        DomOwdSnapshot {
            incoming_estimates: self.incoming_owd_estimates.estimates_snapshot(),
            outgoing_estimates: self.outgoing_owd_estimates.estimates_snapshot(),
            max_outgoing_estimate: self.outgoing_owd_estimates.get_max_owd(),
        }
    }

    /// Removes the proposal with the given entry id from the buffers. Returns the proposal if it was found in any of the buffers.
    pub(crate) fn remove_from_buffers(&mut self, entry_id: EntryId) -> Option<DomPropose<T>> {
        if let Some(prop) = self.early_buffer.remove(entry_id) {
            Some(prop)
        } else {
            None
        }
    }
}
