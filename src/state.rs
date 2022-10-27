use std::{collections::HashSet, time::Instant};

use crate::{Node, Term};

pub enum State {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader),
}

impl State {
    pub fn new(facts: Facts, timeout: Instant) -> State {
        State::Follower(Follower {
            facts,
            term: 0,
            timeout,
            votes_for: None,
        })
    }

    pub fn timeout(self, now: Instant, next_timeout: Instant) -> State {
        match self {
            State::Follower(follower) => {
                if follower.timeout > now {
                    State::Follower(follower)
                } else {
                    // start election
                    let votes_from = HashSet::from_iter(vec![follower.facts.id]);

                    State::Candidate(Candidate {
                        facts: follower.facts,
                        term: follower.term + 1,
                        timeout: next_timeout,
                        votes_from,
                    })
                }
            }
            State::Candidate(candidate) => {
                if candidate.timeout > now {
                    State::Candidate(candidate)
                } else {
                    // new election
                    let votes_from = HashSet::from_iter(vec![candidate.facts.id]);

                    State::Candidate(Candidate {
                        facts: candidate.facts,
                        term: candidate.term + 1,
                        timeout: next_timeout,
                        votes_from,
                    })
                }
            }
            State::Leader(leader) => State::Leader(leader),
        }
    }

    pub fn facts(&self) -> &Facts {
        match self {
            State::Follower(follower) => &follower.facts,
            State::Candidate(candidate) => &candidate.facts,
            State::Leader(leader) => &leader.facts,
        }
    }

    pub fn term(&self) -> Term {
        match self {
            State::Follower(follower) => follower.term,
            State::Candidate(candidate) => candidate.term,
            State::Leader(leader) => leader.term,
        }
    }

    pub fn request_vote(self, from: Node, term: Term, timeout: Instant) -> (State, bool) {
        if self.term() < term {
            // follow the new term
            let follower = Follower {
                facts: *self.facts(),
                term,
                timeout,
                votes_for: Some(from),
            };

            // vote for the candidate
            return (State::Follower(follower), true);
        }

        if self.term() > term {
            // reject the request
            return (self, false);
        }

        match self {
            State::Follower(follower) => {
                match follower.votes_for {
                    Some(votes_for) => {
                        if votes_for == from {
                            // vote for the candidate
                            (State::Follower(follower), true)
                        } else {
                            // reject the request
                            (State::Follower(follower), false)
                        }
                    }
                    None => {
                        // vote for the candidate
                        (State::Follower(follower), true)
                    }
                }
            }
            State::Candidate(candidate) => (State::Candidate(candidate), false),
            State::Leader(leader) => (State::Leader(leader), false),
        }
    }

    pub fn respond_vote(
        self,
        from: Node,
        term: Term,
        vote_granted: bool,
        timeout: Instant,
    ) -> State {
        if self.term() < term {
            // follow the new term
            let follower = Follower {
                facts: *self.facts(),
                term,
                timeout,
                votes_for: None,
            };

            return State::Follower(follower);
        }

        if self.term() > term {
            // ignore the response
            return self;
        }

        match self {
            State::Follower(follower) => State::Follower(follower),
            State::Candidate(candidate) => {
                if vote_granted {
                    let mut votes_from = candidate.votes_from;
                    votes_from.insert(from);

                    if votes_from.len() * 2 > candidate.facts.nodes {
                        // become leader
                        let leader = Leader {
                            facts: candidate.facts,
                            term: candidate.term,
                        };

                        return State::Leader(leader);
                    }

                    State::Candidate(Candidate {
                        facts: candidate.facts,
                        term: candidate.term,
                        timeout: candidate.timeout,
                        votes_from,
                    })
                } else {
                    State::Candidate(candidate)
                }
            }
            State::Leader(leader) => State::Leader(leader),
        }
    }
}

pub struct Follower {
    facts: Facts,
    term: Term,
    timeout: Instant,
    votes_for: Option<Node>,
}

pub struct Candidate {
    facts: Facts,
    term: Term,
    timeout: Instant,
    votes_from: HashSet<Node>,
}

pub struct Leader {
    facts: Facts,
    term: Term,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Facts {
    pub id: Node,
    pub nodes: usize,
}
