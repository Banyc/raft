use crate::{
    election,
    log::Log,
    log_replication::{self, EntryMeta},
    Facts, Node, Term,
};

use super::candidate::Candidate;

pub struct Follower {
    peers: Vec<Node>,
    election: election::Follower,
    log_replication: log_replication::Follower,
}

impl Follower {
    #[must_use]
    pub fn new(peers: Vec<Node>, facts: Facts, term: Term, log: Log) -> Self {
        Self {
            peers,
            election: election::Follower::new(facts, term),
            log_replication: log_replication::Follower::new(log),
        }
    }

    #[must_use]
    pub fn receive_vote_req(
        self,
        from: Node,
        term: Term,
        last_log_index: Option<usize>,
        last_log_term: Option<Term>,
    ) -> (ReceiveVoteReqRes, bool) {
        let valid_log = self.valid_log(last_log_index, last_log_term);

        let (res, vote_granted) = self
            .election
            .try_upgrade_term_and_receive_vote_req(from, term, !valid_log);

        let res = match res {
            election::follower::TryUpgradeTermAndReceiveVoteReqRes::TermUpgraded(election) => {
                ReceiveVoteReqRes::TermUpgraded(Self {
                    peers: self.peers,
                    election,
                    log_replication: self.log_replication,
                })
            }
            election::follower::TryUpgradeTermAndReceiveVoteReqRes::NotUpgraded(election) => {
                ReceiveVoteReqRes::NotUpgraded(Self {
                    peers: self.peers,
                    election,
                    log_replication: self.log_replication,
                })
            }
        };

        (res, vote_granted)
    }

    #[must_use]
    pub fn receive_append_entries_req(
        mut self,
        term: Term,
        new_entries: Vec<Term>,
        prev_entry: Option<EntryMeta>,
        commit_index: Option<usize>,
    ) -> (ReceiveAppendEntriesReqRes, bool) {
        let election = match self.election.try_upgrade_term(term) {
            election::follower::TryUpgradeTermRes::Upgraded(v) => v,
            election::follower::TryUpgradeTermRes::SameTermNotUpgraded(v) => v,
            election::follower::TryUpgradeTermRes::StaleTermNotUpgraded(v) => {
                return (
                    ReceiveAppendEntriesReqRes::StaleTermNotUpgraded(Self {
                        peers: self.peers,
                        election: v,
                        log_replication: self.log_replication,
                    }),
                    false,
                )
            }
        };

        let success = match self.log_replication.receive_append_entries_req(
            new_entries,
            prev_entry,
            commit_index,
        ) {
            Ok(success) => success,
            Err(e) => panic!("{:?}", e),
        };

        (
            ReceiveAppendEntriesReqRes::LogHandled(Self {
                peers: self.peers,
                election,
                log_replication: self.log_replication,
            }),
            success,
        )
    }

    #[must_use]
    pub fn start_election(self) -> Candidate {
        let election_candidate = self.election.start_election();

        Candidate::new(
            self.peers,
            *election_candidate.facts(),
            election_candidate.term(),
            self.log_replication.into_log(),
        )
    }

    #[must_use]
    fn valid_log(&self, last_log_index: Option<usize>, last_log_term: Option<Term>) -> bool {
        // Raft determines which of two logs is more up-to-date
        //   by comparing the index and term of the last entries in the
        //   logs. If the logs have last entries with different terms, then
        //   the log with the later term is more up-to-date. If the logs
        //   end with the same term, then whichever log is longer is
        //   more up-to-date.

        let my_last_log_term = match self.log_replication.log().last_entry() {
            Some((_, v, _)) => Some(v),
            None => None,
        };

        //   valid_last_log_term = { if my_last_log_term < last_log_term }
        // invalid_last_log_term = { if last_log_term < my_last_log_term }
        match (my_last_log_term, last_log_term) {
            (None, None) => (),
            (None, Some(_)) => return true,
            (Some(_), None) => return false,
            (Some(mine), Some(peers_)) => {
                if mine < peers_ {
                    return true;
                } else if peers_ < mine {
                    return false;
                }
            }
        };

        let my_last_log_index = if self.log_replication.log().len() == 0 {
            None
        } else {
            Some(self.log_replication.log().len() - 1)
        };

        //   valid_last_log_index = { if my_last_log_index <= last_log_index }
        // invalid_last_log_index = { if last_log_index < my_last_log_index }
        match (my_last_log_index, last_log_index) {
            (None, None) | (None, Some(_)) => true,
            (Some(_), None) => false,
            (Some(mine), Some(peers_)) => {
                if mine <= peers_ {
                    true
                } else {
                    false
                }
            }
        }
    }
}

pub enum ReceiveVoteReqRes {
    /// - The follower should reset its election timer
    TermUpgraded(Follower),

    /// - The follower should not reset its election timer
    NotUpgraded(Follower),
}

pub enum ReceiveAppendEntriesReqRes {
    /// - The follower should not reset its election timer
    StaleTermNotUpgraded(Follower),

    /// - The follower should reset its election timer
    LogHandled(Follower),
}
