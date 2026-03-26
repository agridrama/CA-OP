use crate::messages::sequence_paxos::{DomPropose, EntryId};
use crate::storage::Entry;
use serde::{Deserialize, Serialize};
use std::collections::{BinaryHeap};

/// Key used to order entries in the early buffer.
/// Ordering is based on deadline, with entry id as a tie-breaker.
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct BufferKey {
    deadline: i64,
    entry_id: EntryId,
}

impl<T: Entry> From<&DomPropose<T>> for BufferKey {
    fn from(proposal: &DomPropose<T>) -> Self {
        Self {
            deadline: proposal.deadline,
            entry_id: proposal.entry_id,
        }
    }
}

/// Entry in the early buffer. Ordering is based only on the buffer key:
/// deadline first, then entry id as a tie-breaker.
struct EBEntry<T: Entry> {
    key: BufferKey,
    proposal: DomPropose<T>,
}

impl<T: Entry> PartialEq for EBEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<T: Entry> Eq for EBEntry<T> {}

impl<T: Entry> PartialOrd for EBEntry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Entry> Ord for EBEntry<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.key.cmp(&self.key) // Reverse order, smallest deadline first
    }
}

impl<T: Entry> EBEntry<T> {
    fn new(proposal: DomPropose<T>) -> Self {
        let key = BufferKey::from(&proposal);

        Self { key, proposal }
    }
}

/// A buffer that stores client requests whose deadline has not yet passed.
pub(crate) struct EarlyBuffer<T: Entry> {
    heap: BinaryHeap<EBEntry<T>>,
    last_released: Option<BufferKey>,
}

impl<T: Entry> EarlyBuffer<T> {
    /// Creates a new early buffer.
    pub(crate) fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            last_released: None,
        }
    }

    /// Inserts the proposal into the early buffer.
    /// Returns an error if the proposal is late.
    pub(crate) fn insert(
        &mut self,
        proposal: DomPropose<T>,
    ) -> Result<(), EarlyBufferInsertError<T>> {
        // TODO check for duplicates
        if self.is_late(&proposal) {
            return Err(EarlyBufferInsertError::Late(proposal));
        }

        self.heap.push(EBEntry::new(proposal));
        Ok(())
    }

    pub(crate) fn last_released_deadline(&self) -> Option<i64> {
        self.last_released.map(|key| key.deadline)
    }

    fn is_late(&self, proposal: &DomPropose<T>) -> bool {
        self.last_released
            .is_some_and(|last| BufferKey::from(proposal) <= last)
    }

    /// Returns the earliest proposal in the early buffer, if any.
    pub(crate) fn peek(&self) -> Option<&DomPropose<T>> {
        self.heap.peek().map(|entry| &entry.proposal)
    }

    /// Removes and returns the earliest proposal in the early buffer, if any.
    pub(crate) fn pop(&mut self) -> Option<DomPropose<T>> {
        let entry = self.heap.pop()?;
        self.last_released = Some(entry.key);
        Some(entry.proposal)
    }

    /// Removes and returns the earliest proposal in the early buffer that is
    /// ready to be released, if any.
    pub(crate) fn pop_ready(&mut self, now: i64) -> Option<DomPropose<T>> {
        if self.peek().is_some_and(|next| next.deadline <= now) {
            self.pop()
        } else {
            None
        }
    }

    pub(crate) fn remove(&mut self, entry_id: EntryId) -> Option<DomPropose<T>> {
        // This is inefficient, but we expect the early buffer to be small.
        let mut removed = None;
        let mut new_heap = BinaryHeap::new();
        while let Some(entry) = self.heap.pop() {
            if entry.proposal.entry_id == entry_id {
                removed = Some(entry.proposal);
            } else {
                new_heap.push(entry);
            }
        }
        self.heap = new_heap;
        removed
    }
}

/// Error signalling something went wrong when inserting a request into the early buffer.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) enum EarlyBufferInsertError<T: Entry> {
    /// The request was late and could not be inserted.
    Late(DomPropose<T>),
}
