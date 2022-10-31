use crate::{Facts, Node, Term};

use super::Candidate;

pub struct Follower {
    facts: Facts,
    term: Term,
    votes_for: Option<Node>,
}

impl Follower {
    #[must_use]
    pub fn new(facts: Facts, term: Term) -> Self {
        Self {
            facts,
            term,
            votes_for: None,
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
    pub fn start_election(self) -> Candidate {
        Candidate::new(self.facts, self.term + 1)
    }

    #[must_use]
    fn receive_vote_request(
        &mut self,
        from: Node,
        term: Term,
        disqualified: bool,
    ) -> Result<bool, ReceiveVoteRequestError> {
        if self.term < term {
            return Err(ReceiveVoteRequestError::UpgradeTerm);
        }

        if term < self.term {
            return Ok(false);
        }

        match self.votes_for {
            Some(votes_for) => {
                if votes_for == from {
                    // vote for the candidate
                    Ok(true)
                } else {
                    // reject the request
                    Ok(false)
                }
            }
            None => {
                if disqualified {
                    // reject the request
                    Ok(false)
                } else {
                    // vote for the candidate
                    self.votes_for = Some(from);

                    Ok(true)
                }
            }
        }
    }

    #[must_use]
    pub fn try_upgrade_term_and_receive_vote_request(
        self,
        from: Node,
        term: Term,
        disqualified: bool,
    ) -> (TryUpgradeTermAndReceiveVoteRequestRes, bool) {
        let (mut this, term_upgraded) = match self.try_upgrade_term(term) {
            TryUpgradeTermRes::Upgraded(follower) => (follower, true),
            TryUpgradeTermRes::SameTermNotUpgraded(follower) => (follower, false),
            TryUpgradeTermRes::StaleTermNotUpgraded(follower) => (follower, false),
        };

        // SAFETY: term is up-to-date at this point
        let vote_granted = this.receive_vote_request(from, term, disqualified).unwrap();

        match term_upgraded {
            true => (
                TryUpgradeTermAndReceiveVoteRequestRes::TermUpgraded(this),
                vote_granted,
            ),
            false => (
                TryUpgradeTermAndReceiveVoteRequestRes::NotUpgraded(this),
                vote_granted,
            ),
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
    /// - If receiving ping, the follower should reset its election timer
    Upgraded(Follower),

    /// - If receiving ping, the follower should reset its election timer
    SameTermNotUpgraded(Follower),

    /// - If receiving ping, the follower should not reset its election timer
    StaleTermNotUpgraded(Follower),
}

#[derive(Debug)]
pub enum ReceiveVoteRequestError {
    UpgradeTerm,
}

pub enum TryUpgradeTermAndReceiveVoteRequestRes {
    /// - The follower should reset its election timer
    TermUpgraded(Follower),

    /// - The follower should not reset its election timer
    NotUpgraded(Follower),
}
