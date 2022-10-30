use crate::{Facts, Term};

use super::Follower;

pub struct Leader {
    facts: Facts,
    term: Term,
}

impl Leader {
    #[must_use]
    pub fn new(facts: Facts, term: Term) -> Self {
        Self { facts, term }
    }

    #[must_use]
    pub fn emit(&self) -> Ping {
        Ping { term: self.term }
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

    pub fn term(&self) -> Term {
        self.term
    }

    pub fn facts(&self) -> &Facts {
        &self.facts
    }
}

pub enum TryUpgradeTermRes {
    Upgraded(Follower),
    NotUpgraded(Leader),
}

pub struct Ping {
    pub term: Term,
}
