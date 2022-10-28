use std::{collections::HashSet, time::Instant};

use crate::{Msg, Node, Term};

pub enum State {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader),
}

impl State {
    /// - Client code should spawn a election timer expiring at `timeout`
    pub fn new(facts: Facts, timeout: Instant) -> State {
        State::Follower(Follower {
            facts,
            term: 0,
            election_timeout: timeout,
            votes_for: None,
        })
    }

    /// - Client code should spawn a new emit timer expiring at `timeout` when some message is returned.
    /// - Client code should call this method when the emit timer expires.
    /// - Client code should call this method after state transitions.
    pub fn emit(&mut self, now: Instant, timeout: Instant) -> Option<Msg> {
        match self {
            State::Follower(_) => None,
            State::Candidate(candidate) => {
                if now < candidate.emission_timeout {
                    return None;
                }

                candidate.emission_timeout = timeout;

                let msg = Msg::RequestVote {
                    term: candidate.term,
                    from: candidate.facts.id,
                };
                Some(msg)
            }
            State::Leader(_) => None,
        }
    }

    /// - When the method returns true, client code should spawn a new election timer expiring at `timeout`.
    /// - Client code should call this method when the election timer expires.
    pub fn elect(self, now: Instant, timeout: Instant) -> (State, bool) {
        match self {
            State::Follower(follower) => {
                if follower.election_timeout > now {
                    (State::Follower(follower), false)
                } else {
                    // start election
                    let votes_from = HashSet::from_iter(vec![follower.facts.id]);

                    (
                        State::Candidate(Candidate {
                            facts: follower.facts,
                            term: follower.term + 1,
                            election_timeout: timeout,
                            votes_from,
                            emission_timeout: now,
                        }),
                        true,
                    )
                }
            }
            State::Candidate(candidate) => {
                if candidate.election_timeout > now {
                    (State::Candidate(candidate), false)
                } else {
                    // new election
                    let votes_from = HashSet::from_iter(vec![candidate.facts.id]);

                    (
                        State::Candidate(Candidate {
                            facts: candidate.facts,
                            term: candidate.term + 1,
                            election_timeout: timeout,
                            votes_from,
                            emission_timeout: now,
                        }),
                        true,
                    )
                }
            }
            State::Leader(leader) => (State::Leader(leader), false),
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

    /// - Client code should spawn a new election timer expiring at `timeout` when `spawn_election_timer` is true.
    pub fn request_vote(self, from: Node, term: Term, timeout: Instant) -> (State, RequestVoteRes) {
        if self.term() < term {
            // follow the new term
            let follower = Follower {
                facts: *self.facts(),
                term,
                election_timeout: timeout,
                votes_for: Some(from),
            };

            // vote for the candidate
            return (
                State::Follower(follower),
                RequestVoteRes {
                    vote_granted: true,
                    spawn_election_timer: true,
                },
            );
        }

        if self.term() > term {
            // reject the request
            return (
                self,
                RequestVoteRes {
                    vote_granted: false,
                    spawn_election_timer: false,
                },
            );
        }

        match self {
            State::Follower(follower) => {
                match follower.votes_for {
                    Some(votes_for) => {
                        if votes_for == from {
                            // vote for the candidate
                            (
                                State::Follower(follower),
                                RequestVoteRes {
                                    vote_granted: true,
                                    spawn_election_timer: false,
                                },
                            )
                        } else {
                            // reject the request
                            (
                                State::Follower(follower),
                                RequestVoteRes {
                                    vote_granted: false,
                                    spawn_election_timer: false,
                                },
                            )
                        }
                    }
                    None => {
                        // vote for the candidate
                        (
                            State::Follower(follower),
                            RequestVoteRes {
                                vote_granted: true,
                                spawn_election_timer: false,
                            },
                        )
                    }
                }
            }
            State::Candidate(candidate) => (
                State::Candidate(candidate),
                RequestVoteRes {
                    vote_granted: false,
                    spawn_election_timer: false,
                },
            ),
            State::Leader(leader) => (
                State::Leader(leader),
                RequestVoteRes {
                    vote_granted: false,
                    spawn_election_timer: false,
                },
            ),
        }
    }

    /// - Client code should spawn a new election timer expiring at `timeout` when this method returns true.
    pub fn respond_vote(
        self,
        from: Node,
        term: Term,
        vote_granted: bool,
        timeout: Instant,
    ) -> (State, bool) {
        if self.term() < term {
            // follow the new term
            let follower = Follower {
                facts: *self.facts(),
                term,
                election_timeout: timeout,
                votes_for: None,
            };

            return (State::Follower(follower), true);
        }

        if self.term() > term {
            // ignore the response
            return (self, false);
        }

        match self {
            State::Follower(follower) => (State::Follower(follower), false),
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

                        return (State::Leader(leader), false);
                    }

                    (
                        State::Candidate(Candidate {
                            facts: candidate.facts,
                            term: candidate.term,
                            election_timeout: candidate.election_timeout,
                            votes_from,
                            emission_timeout: candidate.emission_timeout,
                        }),
                        false,
                    )
                } else {
                    (State::Candidate(candidate), false)
                }
            }
            State::Leader(leader) => (State::Leader(leader), false),
        }
    }
}

pub struct Follower {
    facts: Facts,
    term: Term,
    election_timeout: Instant,
    votes_for: Option<Node>,
}

pub struct Candidate {
    facts: Facts,
    term: Term,
    election_timeout: Instant,
    votes_from: HashSet<Node>,
    emission_timeout: Instant,
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

pub struct RequestVoteRes {
    pub vote_granted: bool,
    pub spawn_election_timer: bool,
}
