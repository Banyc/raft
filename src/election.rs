use std::{collections::HashSet, time::Instant};

use crate::{Facts, Node, Term};

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
    pub fn emit(&mut self, now: Instant, timeout: Instant) -> Option<BroadcastMsg> {
        match self {
            State::Follower(_) => None,
            State::Candidate(candidate) => {
                if now < candidate.emission_timeout {
                    return None;
                }

                candidate.emission_timeout = timeout;

                let msg = BroadcastMsg::RequestVote {
                    term: candidate.term,
                    from: candidate.facts.id,
                };
                Some(msg)
            }
            State::Leader(leader) => {
                if now < leader.emission_timeout {
                    return None;
                }

                leader.emission_timeout = timeout;

                let msg = BroadcastMsg::Ping { term: leader.term };
                Some(msg)
            }
        }
    }

    /// - When the method returns true, client code should spawn a new election timer expiring at `timeout`.
    /// - Client code should call this method when the election timer expires.
    pub fn elect(&mut self, now: Instant, timeout: Instant) -> bool {
        match self {
            State::Follower(follower) => {
                if follower.election_timeout > now {
                    // We are not yet timed out.
                    false
                } else {
                    // start election
                    let votes_from = HashSet::from_iter(vec![follower.facts.id]);

                    let candidate = Candidate {
                        facts: follower.facts,
                        term: follower.term + 1,
                        election_timeout: timeout,
                        votes_from,
                        emission_timeout: now,
                    };
                    *self = State::Candidate(candidate);

                    true
                }
            }
            State::Candidate(candidate) => {
                if candidate.election_timeout > now {
                    // We are not yet timed out.
                    false
                } else {
                    // new election
                    let votes_from = HashSet::from_iter(vec![candidate.facts.id]);

                    let candidate = Candidate {
                        facts: candidate.facts,
                        term: candidate.term + 1,
                        election_timeout: timeout,
                        votes_from,
                        emission_timeout: now,
                    };
                    *self = State::Candidate(candidate);

                    true
                }
            }
            State::Leader(_) => {
                // We are already leader, no need to start a new election.
                false
            }
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
    pub fn request_vote(
        &mut self,
        from: Node,
        term: Term,
        timeout: Instant,
        disqualified: bool,
    ) -> RequestVoteRes {
        if self.try_upgrade_term(term, timeout, Some(from)) {
            // vote for the candidate
            return RequestVoteRes {
                vote_granted: true,
                spawn_election_timer: true,
            };
        }

        if self.term() > term {
            // reject the request
            return RequestVoteRes {
                vote_granted: false,
                spawn_election_timer: false,
            };
        }

        match self {
            State::Follower(follower) => {
                match follower.votes_for {
                    Some(votes_for) => {
                        if votes_for == from {
                            // vote for the candidate
                            RequestVoteRes {
                                vote_granted: true,
                                spawn_election_timer: false,
                            }
                        } else {
                            // reject the request
                            RequestVoteRes {
                                vote_granted: false,
                                spawn_election_timer: false,
                            }
                        }
                    }
                    None => {
                        if disqualified {
                            // reject the request
                            RequestVoteRes {
                                vote_granted: false,
                                spawn_election_timer: false,
                            }
                        } else {
                            // vote for the candidate
                            follower.votes_for = Some(from);

                            RequestVoteRes {
                                vote_granted: true,
                                spawn_election_timer: false,
                            }
                        }
                    }
                }
            }
            State::Candidate(_) => {
                // already voted for themselves
                RequestVoteRes {
                    vote_granted: false,
                    spawn_election_timer: false,
                }
            }
            State::Leader(_) => {
                // already voted for themselves
                RequestVoteRes {
                    vote_granted: false,
                    spawn_election_timer: false,
                }
            }
        }
    }

    /// - Client code should spawn a new election timer expiring at `timeout` when this method returns true.
    pub fn respond_vote(
        &mut self,
        from: Node,
        term: Term,
        vote_granted: bool,
        now: Instant,
        timeout: Instant,
    ) -> bool {
        if self.try_upgrade_term(term, timeout, None) {
            return true;
        }

        if self.term() > term {
            // ignore the response
            return false;
        }

        match self {
            State::Follower(_) => {
                // ignore the response
            }
            State::Candidate(candidate) => {
                if vote_granted {
                    // add up the vote
                    candidate.votes_from.insert(from);

                    // check if we have enough votes
                    if candidate.votes_from.len() * 2 > candidate.facts.nodes {
                        // become leader
                        let leader = Leader {
                            facts: candidate.facts,
                            term: candidate.term,
                            emission_timeout: now,
                        };

                        *self = State::Leader(leader);
                    } else {
                        // keep waiting for more votes
                    }
                } else {
                    // keep waiting for more votes
                }
            }
            State::Leader(_) => {
                // ignore the response
            }
        }
        false
    }

    /// - Client code should call this method when receiving a AppendEntries request.
    /// - Client code should spawn a new election timer expiring at `timeout` when this method returns true.
    pub fn ping(&mut self, term: Term, timeout: Instant) -> Result<bool, PingError> {
        if self.try_upgrade_term(term, timeout, None) {
            return Ok(true);
        }

        if self.term() > term {
            // ignore the ping
            return Ok(false);
        }

        match self {
            State::Follower(follower) => {
                // update the election timeout
                follower.election_timeout = timeout;

                Ok(true)
            }
            State::Candidate(candidate) => {
                // become follower
                let follower = Follower {
                    facts: candidate.facts,
                    term: candidate.term,
                    election_timeout: timeout,
                    votes_for: Some(candidate.facts.id),
                };
                *self = State::Follower(follower);

                Ok(true)
            }
            State::Leader(_) => {
                // there should be only one leader sending heartbeats
                Err(PingError::MultiLeaders)
            }
        }
    }

    /// - Client code should call this method when receiving a AppendEntries response.
    /// - Client code should spawn a new election timer expiring at `timeout` when this method returns true.
    pub fn pong(&mut self, term: Term, timeout: Instant) -> Result<bool, PongError> {
        if self.try_upgrade_term(term, timeout, None) {
            return Ok(true);
        }

        if self.term() > term {
            // ignore the pong
            return Ok(false);
        }

        match self {
            State::Follower(_) => {
                // only leader can send heartbeats
                Err(PongError::NotLeader)
            }
            State::Candidate(_) => {
                // only leader can send heartbeats
                Err(PongError::NotLeader)
            }
            State::Leader(_) => {
                // ignore the pong
                Ok(false)
            }
        }
    }

    /// - Upgrades the term if the given term is greater than the current term.
    /// - `self` becomes Follower if the given term is greater than the current term.
    fn try_upgrade_term(&mut self, term: Term, timeout: Instant, votes_for: Option<Node>) -> bool {
        if self.term() < term {
            // follow the new term
            let follower = Follower {
                facts: *self.facts(),
                term,
                election_timeout: timeout,
                votes_for,
            };

            *self = State::Follower(follower);

            true
        } else {
            false
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
    emission_timeout: Instant,
}

pub struct RequestVoteRes {
    pub vote_granted: bool,
    pub spawn_election_timer: bool,
}

pub enum BroadcastMsg {
    RequestVote { term: Term, from: Node },
    Ping { term: Term },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PingError {
    MultiLeaders,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PongError {
    NotLeader,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_election() {
        let now = Instant::now();
        let mut s1 = State::new(
            Facts {
                id: Node(1),
                nodes: 3,
            },
            now,
        );
        let mut s2 = State::new(
            Facts {
                id: Node(2),
                nodes: 3,
            },
            now,
        );
        let mut s3 = State::new(
            Facts {
                id: Node(3),
                nodes: 3,
            },
            now,
        );

        assert!(s1.elect(now, now));
        match s1 {
            State::Follower(_) => panic!(),
            State::Candidate(_) => {}
            State::Leader(_) => panic!(),
        }

        assert!(s2.elect(now, now));
        match s2 {
            State::Follower(_) => panic!(),
            State::Candidate(_) => {}
            State::Leader(_) => panic!(),
        }

        let msg = s1.emit(now, now).unwrap();
        match msg {
            BroadcastMsg::RequestVote { term, from } => {
                assert_eq!(term, 1);
                assert_eq!(from, Node(1));
            }
            BroadcastMsg::Ping { .. } => panic!(),
        }

        let resp = s2.request_vote(Node(1), 1, now, false);
        assert_eq!(resp.vote_granted, false);
        assert_eq!(resp.spawn_election_timer, false);

        assert!(!s1.respond_vote(Node(2), s2.term(), resp.vote_granted, now, now));
        match s1 {
            State::Follower(_) => panic!(),
            State::Candidate(_) => {}
            State::Leader(_) => panic!(),
        }

        let resp = s3.request_vote(Node(1), 1, now, false);
        assert_eq!(resp.vote_granted, true);
        assert_eq!(resp.spawn_election_timer, true);
        assert_eq!(s3.term(), 1);

        assert!(!s1.respond_vote(Node(3), s3.term(), resp.vote_granted, now, now));
        match s1 {
            State::Follower(_) => panic!(),
            State::Candidate(_) => panic!(),
            State::Leader(_) => {}
        }

        let msg = s2.emit(now, now).unwrap();
        match msg {
            BroadcastMsg::RequestVote { term, from } => {
                assert_eq!(term, 1);
                assert_eq!(from, Node(2));
            }
            BroadcastMsg::Ping { .. } => panic!(),
        }

        let resp = s1.request_vote(Node(2), 1, now, false);
        assert_eq!(resp.vote_granted, false);
        assert_eq!(resp.spawn_election_timer, false);

        assert!(!s2.respond_vote(Node(1), s1.term(), resp.vote_granted, now, now));
        match s2 {
            State::Follower(_) => panic!(),
            State::Candidate(_) => {}
            State::Leader(_) => panic!(),
        }

        let resp = s3.request_vote(Node(2), 1, now, false);
        assert_eq!(resp.vote_granted, false);
        assert_eq!(resp.spawn_election_timer, false);

        assert!(!s2.respond_vote(Node(3), s3.term(), resp.vote_granted, now, now));
        match s2 {
            State::Follower(_) => panic!(),
            State::Candidate(_) => {}
            State::Leader(_) => panic!(),
        }

        assert!(s3.emit(now, now).is_none());

        let msg = s1.emit(now, now).unwrap();
        match msg {
            BroadcastMsg::RequestVote { .. } => panic!(),
            BroadcastMsg::Ping { term } => assert_eq!(term, 1),
        }

        assert!(s2.ping(s1.term(), now).unwrap());
        match s2 {
            State::Follower(_) => {}
            State::Candidate(_) => panic!(),
            State::Leader(_) => panic!(),
        }

        assert!(!s1.pong(s2.term(), now).unwrap());

        assert!(s3.ping(s1.term(), now).unwrap());

        assert!(!s1.pong(s3.term(), now).unwrap());
    }
}
