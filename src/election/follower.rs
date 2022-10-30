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
        } else {
            TryUpgradeTermRes::NotUpgraded(self)
        }
    }

    #[must_use]
    pub fn start_election(self) -> Candidate {
        Candidate::new(self.facts, self.term + 1)
    }

    #[must_use]
    pub fn receive_vote_request(
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

    pub fn term(&self) -> Term {
        self.term
    }

    pub fn facts(&self) -> &Facts {
        &self.facts
    }
}

pub enum TryUpgradeTermRes {
    Upgraded(Follower),
    NotUpgraded(Follower),
}

#[derive(Debug)]
pub enum ReceiveVoteRequestError {
    UpgradeTerm,
}
