use std::collections::HashSet;

use crate::{Facts, Node, Term};

use super::{Follower, Leader};

pub struct Candidate {
    facts: Facts,
    term: Term,
    votes_from: HashSet<Node>,
}

impl Candidate {
    #[must_use]
    pub fn new(facts: Facts, term: Term) -> Self {
        Self {
            facts,
            term,
            votes_from: HashSet::from_iter(vec![facts.id]),
        }
    }

    /// - Upgrades the term if the given term is greater than the current term.
    /// - `self` becomes Follower if the given term is greater than the current term.
    #[must_use]
    pub fn try_upgrade_term(self, term: Term) -> TryUpgradeTermRes {
        if self.term < term {
            // follow the new term
            let follower = Follower::new(self.facts, term);

            TryUpgradeTermRes::Upgraded(follower)
        } else if term < self.term {
            TryUpgradeTermRes::StaleTermNotUpgraded(self)
        } else {
            TryUpgradeTermRes::SameTermNotUpgraded(self)
        }
    }

    #[must_use]
    pub fn emit(&self) -> RequestVote {
        RequestVote {
            term: self.term,
            from: self.facts.id,
        }
    }

    #[must_use]
    pub fn start_new_election(self) -> Candidate {
        Candidate::new(self.facts, self.term + 1)
    }

    fn receive_vote_resp(
        mut self,
        from: Node,
        term: Term,
        vote_granted: bool,
    ) -> Result<ReceiveVoteRespRes, ReceiveVoteError> {
        if self.term < term {
            return Err(ReceiveVoteError::UpgradeTerm);
        }

        if term < self.term {
            return Ok(ReceiveVoteRespRes::NotUpgraded(self));
        }

        if vote_granted {
            // add up the vote
            self.votes_from.insert(from);

            // check if we have enough votes
            if self.votes_from.len() * 2 > self.facts.nodes {
                // become leader
                let leader = Leader::new(self.facts, self.term);

                Ok(ReceiveVoteRespRes::Upgraded(leader))
            } else {
                // keep waiting for more votes
                Ok(ReceiveVoteRespRes::NotUpgraded(self))
            }
        } else {
            // keep waiting for more votes
            Ok(ReceiveVoteRespRes::NotUpgraded(self))
        }
    }

    fn receive_ping(self, term: Term) -> Result<ReceivePingRes, ReceivePingError> {
        if self.term < term {
            return Err(ReceivePingError::UpgradeTerm);
        }

        if term < self.term {
            return Ok(ReceivePingRes::StaleTermNotUpgraded(self));
        }

        // become follower
        let follower = Follower::new(self.facts, self.term);

        Ok(ReceivePingRes::Upgraded(follower))
    }

    #[must_use]
    pub fn try_upgrade_term_and_receive_vote_resp(
        self,
        from: Node,
        term: Term,
        vote_granted: bool,
    ) -> TryUpgradeTermAndReceiveVoteRespRes {
        let this = match self.try_upgrade_term(term) {
            TryUpgradeTermRes::Upgraded(follower) => {
                return TryUpgradeTermAndReceiveVoteRespRes::TermUpgraded(follower);
            }
            TryUpgradeTermRes::SameTermNotUpgraded(candidate) => candidate,
            TryUpgradeTermRes::StaleTermNotUpgraded(candidate) => {
                return TryUpgradeTermAndReceiveVoteRespRes::StaleTermNotUpgraded(candidate)
            }
        };

        // SAFETY: term is up-to-date at this point
        match this.receive_vote_resp(from, term, vote_granted).unwrap() {
            ReceiveVoteRespRes::Upgraded(leader) => {
                TryUpgradeTermAndReceiveVoteRespRes::Elected(leader)
            }
            ReceiveVoteRespRes::NotUpgraded(candidate) => {
                TryUpgradeTermAndReceiveVoteRespRes::NotElectedYet(candidate)
            }
        }
    }

    #[must_use]
    pub fn try_upgrade_term_and_receive_ping(self, term: Term) -> TryUpgradeTermAndReceivePingRes {
        let this = match self.try_upgrade_term(term) {
            TryUpgradeTermRes::Upgraded(follower) => {
                return TryUpgradeTermAndReceivePingRes::TermUpgraded(follower);
            }
            TryUpgradeTermRes::SameTermNotUpgraded(candidate) => candidate,
            TryUpgradeTermRes::StaleTermNotUpgraded(candidate) => {
                return TryUpgradeTermAndReceivePingRes::StaleTermNotUpgraded(candidate)
            }
        };

        // SAFETY: term is up-to-date at this point
        match this.receive_ping(term).unwrap() {
            ReceivePingRes::Upgraded(follower) => {
                TryUpgradeTermAndReceivePingRes::LostElection(follower)
            }

            // SAFETY: term is the same at this point
            ReceivePingRes::StaleTermNotUpgraded(_) => unreachable!(),
        }
    }

    pub fn term(&self) -> Term {
        self.term
    }

    pub fn facts(&self) -> &Facts {
        &self.facts
    }
}

pub enum TryUpgradeTermRes {
    /// - If receiving vote request, the follower should reset its election timer.
    Upgraded(Follower),

    /// - If receiving vote request, the candidate should not reset its election timer.
    StaleTermNotUpgraded(Candidate),

    /// - If receiving vote request, the candidate should not reset its election timer.
    SameTermNotUpgraded(Candidate),
}

pub struct RequestVote {
    pub term: Term,
    pub from: Node,
}

#[derive(Debug)]
pub enum ReceiveVoteError {
    UpgradeTerm,
}

pub enum ReceiveVoteRespRes {
    Upgraded(Leader),
    NotUpgraded(Candidate),
}

#[derive(Debug)]
pub enum ReceivePingError {
    UpgradeTerm,
}

pub enum ReceivePingRes {
    Upgraded(Follower),
    StaleTermNotUpgraded(Candidate),
}

pub enum TryUpgradeTermAndReceiveVoteRespRes {
    /// - The follower should reset its election timer
    TermUpgraded(Follower),

    /// - The candidate should not reset its election timer
    StaleTermNotUpgraded(Candidate),

    /// - The leader should cancel its election timer
    Elected(Leader),

    /// - The candidate should not reset its election timer
    NotElectedYet(Candidate),
}

pub enum TryUpgradeTermAndReceivePingRes {
    /// - The follower should reset its election timer
    TermUpgraded(Follower),

    /// - The candidate should not reset its election timer
    StaleTermNotUpgraded(Candidate),

    /// - The follower should reset its election timer
    LostElection(Follower),
}
