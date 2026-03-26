use super::super::{
    ballot_leader_election::Ballot,
    util::{LeaderState, PromiseMetaData},
};
use crate::util::{AcceptedMetaData, DOMHash, WRITE_ERROR_MSG};

use super::*;
use std::collections::HashSet;

impl<'a, T, B, C> SequencePaxos<'a, T, B, C>
where
    T: Entry,
    B: Storage<T>,
    C: PhysicalClock,
{
    /// Handle a new leader. Should be called when the leader election has elected a new leader with the ballot `n`
    /*** Leader ***/
    pub(crate) fn handle_leader(&mut self, n: Ballot) {
        if n <= self.leader_state.n_leader || n <= self.internal_storage.get_promise() {
            return;
        }
        #[cfg(feature = "logging")]
        debug!(self.logger, "Newly elected leader: {:?}", n);
        if self.pid == n.pid {
            self.leader_state =
                LeaderState::with(n, self.leader_state.max_pid, self.leader_state.quorum);
            // Flush any pending writes
            // Don't have to handle flushed entries here because we will sync with followers
            let _ = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
            self.internal_storage.set_promise(n).expect(WRITE_ERROR_MSG);
            /* insert my promise */
            let na = self.internal_storage.get_accepted_round();
            let decided_idx = self.get_decided_idx();
            let accepted_idx = self.internal_storage.get_accepted_idx();
            let my_promise = Promise {
                n,
                n_accepted: na,
                decided_idx,
                accepted_idx,
                log_sync: None,
                log_unsync: Some(self.unsynced_log.clone()),
                log_prefix_hash: self.accepted_prefix_hash.clone(),
            };
            self.leader_state.set_promise(my_promise, self.pid, true);
            self.leader_state
                .set_unsynced_log(self.pid, self.unsynced_log.clone());
            /* initialise longest chosen sequence and update state */
            self.state = (Role::Leader, Phase::Prepare);
            let prep = Prepare {
                n,
                decided_idx,
                n_accepted: na,
                accepted_idx,
            };
            /* send prepare */
            for pid in &self.peers {
                self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                    from: self.pid,
                    to: *pid,
                    msg: PaxosMsg::Prepare(prep),
                }));
            }
        } else {
            self.become_follower();
        }
    }

    pub(crate) fn become_follower(&mut self) {
        self.state.0 = Role::Follower;
    }

    pub(crate) fn handle_preparereq(&mut self, prepreq: PrepareReq, from: NodeId) {
        #[cfg(feature = "logging")]
        debug!(self.logger, "Incoming message PrepareReq from {}", from);
        if self.state.0 == Role::Leader && prepreq.n <= self.leader_state.n_leader {
            self.leader_state.reset_promise(from);
            self.leader_state.set_latest_accept_meta(from, None);
            self.send_prepare(from);
        }
    }

    pub(crate) fn handle_forwarded_stopsign(&mut self, ss: StopSign) {
        if self.accepted_reconfiguration() {
            return;
        }
        match self.state {
            (Role::Leader, Phase::Prepare) => self.buffered_stopsign = Some(ss),
            (Role::Leader, Phase::Accept) => self.accept_stopsign_leader(ss),
            _ => self.forward_stopsign(ss),
        }
    }

    pub(crate) fn handle_fast_accepted(&mut self, fast_acc: FastAccepted, from: NodeId) {
        if !self.enable_fast_path {
            return;
        }
        #[cfg(feature = "logging")]
        debug!(
            self.logger,
            "Got FastAccepted from {}, idx: {}, prefix_hash: {:?}, chosen_idx: {}, accepted: {:?}",
            from,
            fast_acc.idx,
            fast_acc.prefix_hash,
            self.internal_storage.get_decided_idx(),
            self.leader_state.accepted_indexes
        );

        //if currentRnd != n then return
        if self.leader_state.n_leader != fast_acc.n {
            return;
        }

        // only handle if we are leader in accept phase
        if self.state != (Role::Leader, Phase::Accept) {
            return;
        }

        let entry = self.leader_state.set_accepted_map(
            fast_acc.idx,
            fast_acc.entry_hash,
            fast_acc.prefix_hash,
            from,
            true,
        );
        let _ = entry;
        self.try_fast_decide_idx(fast_acc.idx);
    }

    pub(crate) fn send_prepare(&mut self, to: NodeId) {
        let prep = Prepare {
            n: self.leader_state.n_leader,
            decided_idx: self.internal_storage.get_decided_idx(),
            n_accepted: self.internal_storage.get_accepted_round(),
            accepted_idx: self.internal_storage.get_accepted_idx(),
        };
        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::Prepare(prep),
        }));
    }

    pub(crate) fn accept_entry_leader(&mut self, entry: T) {
        let accepted_metadata = self
            .internal_storage
            .append_entry_with_batching(entry)
            .expect(WRITE_ERROR_MSG);
        if let Some(metadata) = accepted_metadata {
            let accepted_idx = metadata.accepted_idx;
            let num_entries = metadata.entries.len();
            self.leader_state
                .set_accepted_idx(self.pid, accepted_idx);
            self.send_acceptdecide(metadata);

            // Re-check fast quorum when the leader has locally accepted these entries.
            let first_idx = accepted_idx + 1 - num_entries;
            for idx in first_idx..=accepted_idx {
                self.try_fast_decide_idx(idx);
            }
        }
    }

    fn try_fast_decide_idx(&mut self, idx: usize) -> bool {
        if !self.enable_fast_path {
            return false;
        }
        if self.state != (Role::Leader, Phase::Accept) {
            return false;
        }

        let decided_idx = self.internal_storage.get_decided_idx();
        let accepted_idx = self.internal_storage.get_accepted_idx();
        if idx <= decided_idx || idx > accepted_idx {
            return false;
        }

        let Some(entry) = self.leader_state.get_accepted_map(idx).cloned() else {
            return false;
        };

        let num_nodes = self.peers.len() + 1;
        let f = num_nodes / 2;
        let fast_quorum = f + (f + 1) / 2 + 1;

        let leader_candidate = (entry.prefix_hash, entry.entry_hash);
        let fast_voters: Set<NodeId> = entry
            .fast
            .get(&leader_candidate)
            .cloned()
            .unwrap_or_default();
        let combined: Set<NodeId> = fast_voters.union(&entry.slow).cloned().collect();

        if !entry.slow.contains(&self.pid) || combined.len() < fast_quorum {
            return false;
        }

        #[cfg(feature = "logging")]
        debug!(self.logger, "Fast quorum reached for idx {}: {:?}", idx, combined);

        #[cfg(feature = "benchmark")]
        {
            self.fast_path_decisions += (idx - decided_idx) as u64;
        }

        self.internal_storage
            .set_decided_idx(idx)
            .expect(WRITE_ERROR_MSG);
        self.leader_state.prune_accepted_map(idx);

        for pid in self.leader_state.get_promised_followers() {
            self.send_decide(pid, idx, false);
        }
        true
    }

    pub(crate) fn accept_stopsign_leader(&mut self, ss: StopSign) {
        let accepted_metadata = self
            .internal_storage
            .append_stopsign(ss.clone())
            .expect(WRITE_ERROR_MSG);
        if let Some(metadata) = accepted_metadata {
            self.send_acceptdecide(metadata);
        }
        let accepted_idx = self.internal_storage.get_accepted_idx();
        self.leader_state.set_accepted_idx(self.pid, accepted_idx);
        for pid in self.leader_state.get_promised_followers() {
            self.send_accept_stopsign(pid, ss.clone(), false);
        }
    }

    fn send_accsync(&mut self, to: NodeId) {
        let current_n = self.leader_state.n_leader;
        let PromiseMetaData {
            n_accepted: prev_round_max_promise_n,
            accepted_idx: prev_round_max_accepted_idx,
            ..
        } = &self.leader_state.get_max_promise_meta();
        let PromiseMetaData {
            n_accepted: followers_promise_n,
            accepted_idx: followers_accepted_idx,
            pid,
            ..
        } = self.leader_state.get_promise_meta(to);
        let followers_decided_idx = self
            .leader_state
            .get_decided_idx(*pid)
            .expect("Received PromiseMetaData but not found in ld");
        // Follower can have valid accepted entries depending on which leader they were previously following
        let followers_valid_entries_idx = if *followers_promise_n == current_n {
            *followers_accepted_idx
        } else if *followers_promise_n == *prev_round_max_promise_n {
            *prev_round_max_accepted_idx.min(followers_accepted_idx)
        } else {
            followers_decided_idx
        };
        let log_sync = self.create_log_sync(followers_valid_entries_idx, followers_decided_idx);
        self.leader_state.increment_seq_num_session(to);
        let acc_sync = AcceptSync {
            n: current_n,
            seq_num: self.leader_state.next_seq_num(to),
            decided_idx: self.get_decided_idx(),
            log_sync,
            log_prefix_hash: self.accepted_prefix_hash,
            #[cfg(feature = "unicache")]
            unicache: self.internal_storage.get_unicache(),
        };
        let msg = Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::AcceptSync(acc_sync),
        });
        self.outgoing.push(msg);
    }

    fn send_acceptdecide(&mut self, accepted: AcceptedMetaData<T>) {
        let decided_idx = self.internal_storage.get_decided_idx();
        let accepted_prefix_hash = self.accepted_prefix_hash;
        let entry_meta = self
            .leader_state
            .take_pending_accept_meta(accepted.entries.len());
        for pid in self.leader_state.get_promised_followers() {
            let latest_accdec = self.get_latest_accdec_message(pid);
            match latest_accdec {
                // Modify existing AcceptDecide message to follower
                Some(accdec) => {
                    accdec.entries.extend(accepted.entries.iter().cloned());
                    accdec.entry_meta.extend(entry_meta.iter().copied());
                    accdec.decided_idx = decided_idx;
                    accdec.log_prefix_hash = accepted_prefix_hash;
                }
                // Add new AcceptDecide message to follower
                None => {
                    self.leader_state
                        .set_latest_accept_meta(pid, Some(self.outgoing.len()));
                    let acc = AcceptDecide {
                        n: self.leader_state.n_leader,
                        seq_num: self.leader_state.next_seq_num(pid),
                        decided_idx,
                        entries: accepted.entries.clone(),
                        entry_meta: entry_meta.clone(),
                        log_prefix_hash: accepted_prefix_hash,
                    };
                    self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                        from: self.pid,
                        to: pid,
                        msg: PaxosMsg::AcceptDecide(acc),
                    }));
                }
            }
        }
    }

    fn send_accept_stopsign(&mut self, to: NodeId, ss: StopSign, resend: bool) {
        let seq_num = match resend {
            true => self.leader_state.get_seq_num(to),
            false => self.leader_state.next_seq_num(to),
        };
        let acc_ss = PaxosMsg::AcceptStopSign(AcceptStopSign {
            seq_num,
            n: self.leader_state.n_leader,
            ss,
        });
        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: acc_ss,
        }));
    }

    pub(crate) fn send_decide(&mut self, to: NodeId, decided_idx: usize, resend: bool) {
        let seq_num = match resend {
            true => self.leader_state.get_seq_num(to),
            false => self.leader_state.next_seq_num(to),
        };
        let d = Decide {
            n: self.leader_state.n_leader,
            seq_num,
            decided_idx,
        };
        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::Decide(d),
        }));
    }

    fn handle_majority_promises(&mut self) {
        let max_promise_sync = self.leader_state.take_max_promise_sync();
        let decided_idx = self.leader_state.get_max_decided_idx();

        // Update log and accepted_idx according to the highest accepted_idx among promises
        let mut new_accepted_idx = self
            .internal_storage
            .sync_log(self.leader_state.n_leader, decided_idx, max_promise_sync)
            .expect(WRITE_ERROR_MSG);
        // Update log from unsynced log if necessary
        let num_nodes = self.peers.len() + 1;
        let f = num_nodes / 2;
        let recover_threshold = (f + 1) / 2 + 1; // ceil(f/2) + 1

        let mut expected_prev_hash = self.leader_state.get_max_promise_accepted_hash().clone();
        let mut recovered_entry_ids: Set<EntryId> = HashSet::new();
        let mut recovered_idx = new_accepted_idx + 1;
        'recover: loop {
            let entries_count = self
                .leader_state
                .get_matched_unsynced_entries(recovered_idx, expected_prev_hash.clone());
            let mut winners = vec::Vec::<(DOMHash, EntryId, T)>::new();
            for (e_hash, e_id, e, cnt) in entries_count {
                if cnt >= recover_threshold {
                    winners.push((e_hash, e_id, e));
                }
            }
            if winners.is_empty() {
                break 'recover;
            }
            if winners.len() >= 2 {
                // collision: multiple candidates for same idx with enough votes.
                break 'recover;
            }
            let e = winners.pop().unwrap();
            if recovered_entry_ids.contains(&e.1) {
                // Stop recovery if the same logical request appears again in the
                // recovered tail. Appending it again would duplicate the command.
                break 'recover;
            }
            if !self
                .unsynced_log
                .values()
                .any(|entry| entry.entry_id == e.1)
            {
                self.leader_state.insert_recovered_remote_entry_id(e.1);
            }
            recovered_entry_ids.insert(e.1);

            new_accepted_idx = self
                .internal_storage
                .append_entries_without_batching(vec![e.2.clone()])
                .expect(WRITE_ERROR_MSG);

            expected_prev_hash.extend_hash(&e.0);
            recovered_idx = new_accepted_idx + 1;
        }
        self.accepted_prefix_hash = expected_prev_hash;

        // Remove entries from unsynced logs that are duplicated in synced-log
        self.unsynced_log.retain(|idx, entry| {
            *idx > new_accepted_idx && !recovered_entry_ids.contains(&entry.entry_id)
        });
        // Also remove duplicates from early/late buffers once implemented.
        for eid in recovered_entry_ids {
            self.dom.remove_from_buffers(eid);
        }

        // Re-append the remaining unsynced entries to the DOM buffer to update their deadlines
        let dom_props: Vec<_> = self
            .unsynced_log
            .values()
            .map(|ulog| {
                self.dom
                    .create_dom_propose(ulog.entry.clone(), ulog.entry_id)
            })
            .collect();

        for dom_prop in dom_props {
            self.send_dom_propose(dom_prop);
        }

        if !self.accepted_reconfiguration() {
            if !self.buffered_proposals.is_empty() {
                let entries = std::mem::take(&mut self.buffered_proposals);
                new_accepted_idx = self
                    .internal_storage
                    .append_entries_without_batching(entries)
                    .expect(WRITE_ERROR_MSG);
            }
            if let Some(ss) = self.buffered_stopsign.take() {
                self.internal_storage
                    .append_stopsign(ss)
                    .expect(WRITE_ERROR_MSG);
                new_accepted_idx = self.internal_storage.get_accepted_idx();
            }
        }
        self.state = (Role::Leader, Phase::Accept);
        self.leader_state
            .set_accepted_idx(self.pid, new_accepted_idx);
        for pid in self.leader_state.get_promised_followers() {
            self.send_accsync(pid);
        }
    }

    pub(crate) fn handle_promise_prepare(&mut self, prom: Promise<T>, from: NodeId) {
        #[cfg(feature = "logging")]
        debug!(
            self.logger,
            "Handling promise from {} in Prepare phase", from
        );
        if prom.n == self.leader_state.n_leader {
            let received_majority = self.leader_state.set_promise(prom, from, true);
            if received_majority {
                self.handle_majority_promises();
            }
        }
    }

    pub(crate) fn handle_promise_accept(&mut self, prom: Promise<T>, from: NodeId) {
        #[cfg(feature = "logging")]
        {
            let (r, p) = &self.state;
            debug!(
                self.logger,
                "Self role {:?}, phase {:?}. Incoming message Promise Accept from {}", r, p, from
            );
        }
        if prom.n == self.leader_state.n_leader {
            self.leader_state.set_promise(prom, from, false);
            self.send_accsync(from);
        }
    }

    pub(crate) fn handle_accepted(&mut self, accepted: Accepted, from: NodeId) {
        #[cfg(feature = "logging")]
        debug!(
            self.logger,
            "Got Accepted from {}, idx: {}, chosen_idx: {}, accepted: {:?}",
            from,
            accepted.accepted_idx,
            self.internal_storage.get_decided_idx(),
            self.leader_state.accepted_indexes
        );
        if accepted.n == self.leader_state.n_leader && self.state == (Role::Leader, Phase::Accept) {
            self.leader_state
                .set_accepted_idx(from, accepted.accepted_idx);
            if accepted.accepted_idx > self.internal_storage.get_decided_idx()
                && self.leader_state.is_chosen(accepted.accepted_idx)
            {
                let decided_idx = accepted.accepted_idx;
                #[cfg(feature = "benchmark")]
                {
                    let old_decided_idx = self.internal_storage.get_decided_idx();
                    self.slow_path_decisions += (decided_idx - old_decided_idx) as u64;
                    // benchmark
                }
                self.internal_storage
                    .set_decided_idx(decided_idx)
                    .expect(WRITE_ERROR_MSG);
                self.leader_state.prune_accepted_map(decided_idx);

                for pid in self.leader_state.get_promised_followers() {
                    let latest_accdec = self.get_latest_accdec_message(pid);
                    match latest_accdec {
                        Some(accdec) => accdec.decided_idx = decided_idx,
                        None => self.send_decide(pid, decided_idx, false),
                    }
                }
            }
        }
    }

    fn get_latest_accdec_message(&mut self, to: NodeId) -> Option<&mut AcceptDecide<T>> {
        if let Some((bal, outgoing_idx)) = self.leader_state.get_latest_accept_meta(to) {
            if bal == self.leader_state.n_leader {
                if let Message::SequencePaxos(PaxosMessage {
                    msg: PaxosMsg::AcceptDecide(accdec),
                    ..
                }) = self.outgoing.get_mut(outgoing_idx).unwrap()
                {
                    return Some(accdec);
                } else {
                    #[cfg(feature = "logging")]
                    debug!(self.logger, "Cached idx is not an AcceptedDecide!");
                }
            }
        }
        None
    }

    pub(crate) fn handle_notaccepted(&mut self, not_acc: NotAccepted, from: NodeId) {
        if self.state.0 == Role::Leader && self.leader_state.n_leader < not_acc.n {
            self.leader_state.lost_promise(from);
        }
    }

    pub(crate) fn resend_messages_leader(&mut self) {
        match self.state.1 {
            Phase::Prepare => {
                // Resend Prepare
                let preparable_peers = self.leader_state.get_preparable_peers(&self.peers);
                for peer in preparable_peers {
                    self.send_prepare(peer);
                }
            }
            Phase::Accept => {
                // Resend AcceptStopSign or StopSign's decide
                if let Some(ss) = self.internal_storage.get_stopsign() {
                    let decided_idx = self.internal_storage.get_decided_idx();
                    for follower in self.leader_state.get_promised_followers() {
                        if self.internal_storage.stopsign_is_decided() {
                            self.send_decide(follower, decided_idx, true);
                        } else if self.leader_state.get_accepted_idx(follower)
                            != self.internal_storage.get_accepted_idx()
                        {
                            self.send_accept_stopsign(follower, ss.clone(), true);
                        }
                    }
                }
                // Resend Prepare
                let preparable_peers = self.leader_state.get_preparable_peers(&self.peers);
                for peer in preparable_peers {
                    self.send_prepare(peer);
                }
            }
            Phase::Recover => (),
            Phase::None => (),
        }
    }

    pub(crate) fn flush_batch_leader(&mut self) {
        let accepted_metadata = self
            .internal_storage
            .flush_batch_and_get_entries()
            .expect(WRITE_ERROR_MSG);
        if let Some(metadata) = accepted_metadata {
            self.leader_state
                .set_accepted_idx(self.pid, metadata.accepted_idx);
            self.send_acceptdecide(metadata);
        }
    }
}
