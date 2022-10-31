use crate::{
    election,
    log::Log,
    log_replication::{self, EntryMeta},
    Facts, Node, Term,
};

use super::{
    follower::{self, Follower},
    leader::Leader,
};

pub struct Candidate {
    peers: Vec<Node>,
    election: election::Candidate,
    log_replication: log_replication::Follower,
}

impl Candidate {
    #[must_use]
    pub fn new(peers: Vec<Node>, facts: Facts, term: Term, log: Log) -> Self {
        Self {
            peers,
            election: election::Candidate::new(facts, term),
            log_replication: log_replication::Follower::new(log),
        }
    }

    #[must_use]
    pub fn emit(&self) -> VoteReq {
        let election_emit = self.election.emit();

        let last_log = self.log_replication.log().last_entry();

        let (last_log_index, last_log_term) = match last_log {
            Some((index, term, _)) => (Some(index), Some(term)),
            None => (None, None),
        };

        VoteReq {
            term: election_emit.term,
            from: election_emit.from,
            last_log_index,
            last_log_term,
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
        let election = match self.election.try_upgrade_term(term) {
            election::candidate::TryUpgradeTermRes::Upgraded(election) => {
                let follower = Follower::new(
                    self.peers,
                    *election.facts(),
                    term,
                    self.log_replication.into_log(),
                );

                let (res, vote_granted) =
                    follower.receive_vote_req(from, term, last_log_index, last_log_term);

                let follower = match res {
                    // SAFETY: We know that the term is the same as the one we just upgraded to.
                    follower::ReceiveVoteReqRes::TermUpgraded(_) => unreachable!(),

                    follower::ReceiveVoteReqRes::NotUpgraded(v) => v,
                };

                return (ReceiveVoteReqRes::TermUpgraded(follower), vote_granted);
            }
            election::candidate::TryUpgradeTermRes::StaleTermNotUpgraded(v) => v,
            election::candidate::TryUpgradeTermRes::SameTermNotUpgraded(v) => v,
        };

        let candidate = Self {
            peers: self.peers,
            election,
            log_replication: self.log_replication,
        };

        (ReceiveVoteReqRes::NotUpgraded(candidate), false)
    }

    pub fn receive_vote_resp(
        self,
        from: Node,
        term: Term,
        vote_granted: bool,
    ) -> Result<ReceiveVoteRespRes, log_replication::leader::NewError> {
        let election =
            match self
                .election
                .try_upgrade_term_and_receive_vote_resp(from, term, vote_granted)
            {
                election::candidate::TryUpgradeTermAndReceiveVoteRespRes::TermUpgraded(
                    election,
                ) => {
                    let follower = Follower::new(
                        self.peers,
                        *election.facts(),
                        election.term(),
                        self.log_replication.into_log(),
                    );

                    return Ok(ReceiveVoteRespRes::TermUpgraded(follower));
                }
                election::candidate::TryUpgradeTermAndReceiveVoteRespRes::StaleTermNotUpgraded(
                    v,
                ) => v,
                election::candidate::TryUpgradeTermAndReceiveVoteRespRes::Elected(election) => {
                    let leader = Leader::new(
                        self.peers,
                        *election.facts(),
                        election.term(),
                        self.log_replication.into_log(),
                    )?;

                    return Ok(ReceiveVoteRespRes::Elected(leader));
                }
                election::candidate::TryUpgradeTermAndReceiveVoteRespRes::NotElectedYet(v) => v,
            };

        let candidate = Candidate {
            peers: self.peers,
            election,
            log_replication: self.log_replication,
        };

        Ok(ReceiveVoteRespRes::StaleTermNotUpgradedOrNotElectedYet(
            candidate,
        ))
    }

    #[must_use]
    pub fn receive_append_entries_req(
        self,
        term: Term,
        new_entries: Vec<Term>,
        prev_entry: Option<EntryMeta>,
        commit_index: Option<usize>,
    ) -> (ReceiveAppendEntriesReqRes, bool) {
        let election = match self.election.try_upgrade_term_and_receive_ping(term) {
            election::candidate::TryUpgradeTermAndReceivePingRes::TermUpgraded(v) => v,
            election::candidate::TryUpgradeTermAndReceivePingRes::StaleTermNotUpgraded(
                election,
            ) => {
                let candidate = Candidate {
                    peers: self.peers,
                    election,
                    log_replication: self.log_replication,
                };

                return (
                    ReceiveAppendEntriesReqRes::StaleTermNotUpgraded(candidate),
                    false,
                );
            }
            election::candidate::TryUpgradeTermAndReceivePingRes::LostElection(v) => v,
        };

        let follower = Follower::new(
            self.peers,
            *election.facts(),
            election.term(),
            self.log_replication.into_log(),
        );

        let (res, success) =
            follower.receive_append_entries_req(term, new_entries, prev_entry, commit_index);

        let follower = match res {
            // SAFETY: We know that the term is the same as the one we just upgraded to.
            follower::ReceiveAppendEntriesReqRes::StaleTermNotUpgraded(_) => unreachable!(),

            follower::ReceiveAppendEntriesReqRes::LogHandled(v) => v,
        };

        (
            ReceiveAppendEntriesReqRes::TermUpgradedOrLostElection(follower),
            success,
        )
    }
}

pub struct VoteReq {
    pub from: Node,
    pub term: Term,
    pub last_log_index: Option<usize>,
    pub last_log_term: Option<Term>,
}

pub enum ReceiveVoteReqRes {
    /// - The follower should reset its election timer
    TermUpgraded(Follower),

    /// - The candidate should not reset its election timer
    NotUpgraded(Candidate),
}

pub enum ReceiveVoteRespRes {
    /// - The follower should reset its election timer
    TermUpgraded(Follower),

    /// - The candidate should not reset its election timer
    StaleTermNotUpgradedOrNotElectedYet(Candidate),

    /// - The leader should cancel its election timer
    Elected(Leader),
}

pub enum ReceiveAppendEntriesReqRes {
    /// - The follower should reset its election timer
    TermUpgradedOrLostElection(Follower),

    /// - The candidate should not reset its election timer
    StaleTermNotUpgraded(Candidate),
}
