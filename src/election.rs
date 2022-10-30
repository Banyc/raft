use std::collections::HashSet;

use crate::{Facts, Node, Term};

pub enum State {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader),
}

impl State {
    /// - Client code should spawn a election timer expiring at `timeout`
    pub fn new(facts: Facts) -> State {
        State::Follower(Follower {
            facts,
            term: 0,
            votes_for: None,
        })
    }

    /// - Client code should spawn a new emit timer expiring at `timeout` when some message is returned.
    /// - Client code should call this method when the emit timer expires.
    /// - Client code should call this method after state transitions.
    pub fn emit(&mut self) -> Option<BroadcastMsg> {
        match self {
            State::Follower(_) => None,
            State::Candidate(candidate) => {
                let msg = BroadcastMsg::RequestVote {
                    term: candidate.term,
                    from: candidate.facts.id,
                };
                Some(msg)
            }
            State::Leader(leader) => {
                let msg = BroadcastMsg::Ping { term: leader.term };
                Some(msg)
            }
        }
    }

    /// - When the method returns true, client code should reset the election timer.
    /// - Client code should call this method when the election timer expires.
    pub fn elect(&mut self) -> bool {
        match self {
            State::Follower(follower) => {
                // start election
                let votes_from = HashSet::from_iter(vec![follower.facts.id]);

                let candidate = Candidate {
                    facts: follower.facts,
                    term: follower.term + 1,
                    votes_from,
                };
                *self = State::Candidate(candidate);

                true
            }
            State::Candidate(candidate) => {
                // new election
                let votes_from = HashSet::from_iter(vec![candidate.facts.id]);

                let candidate = Candidate {
                    facts: candidate.facts,
                    term: candidate.term + 1,
                    votes_from,
                };
                *self = State::Candidate(candidate);

                true
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

    /// - Client code should reset the election timer when `spawn_election_timer` is true.
    pub fn request_vote(&mut self, from: Node, term: Term, disqualified: bool) -> RequestVoteRes {
        if self.try_upgrade_term(term, Some(from)) {
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

    /// - Client code should reset the election timer when this method returns true.
    pub fn respond_vote(&mut self, from: Node, term: Term, vote_granted: bool) -> bool {
        if self.try_upgrade_term(term, None) {
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
    /// - Client code should reset the election timer when this method returns true.
    pub fn ping(&mut self, term: Term) -> Result<bool, PingError> {
        if self.try_upgrade_term(term, None) {
            return Ok(true);
        }

        if self.term() > term {
            // ignore the ping
            return Ok(false);
        }

        match self {
            State::Follower(_) => {
                // update the election timeout

                Ok(true)
            }
            State::Candidate(candidate) => {
                // become follower
                let follower = Follower {
                    facts: candidate.facts,
                    term: candidate.term,
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
    /// - Client code should reset the election timer when this method returns true.
    pub fn pong(&mut self, term: Term) -> Result<bool, PongError> {
        if self.try_upgrade_term(term, None) {
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
    fn try_upgrade_term(&mut self, term: Term, votes_for: Option<Node>) -> bool {
        if self.term() < term {
            // follow the new term
            let follower = Follower {
                facts: *self.facts(),
                term,
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
    votes_for: Option<Node>,
}

pub struct Candidate {
    facts: Facts,
    term: Term,
    votes_from: HashSet<Node>,
}

pub struct Leader {
    facts: Facts,
    term: Term,
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
        let mut s1 = State::new(Facts {
            id: Node(1),
            nodes: 3,
        });
        let mut s2 = State::new(Facts {
            id: Node(2),
            nodes: 3,
        });
        let mut s3 = State::new(Facts {
            id: Node(3),
            nodes: 3,
        });

        assert!(s1.elect());
        match s1 {
            State::Follower(_) => panic!(),
            State::Candidate(_) => {}
            State::Leader(_) => panic!(),
        }

        assert!(s2.elect());
        match s2 {
            State::Follower(_) => panic!(),
            State::Candidate(_) => {}
            State::Leader(_) => panic!(),
        }

        let msg = s1.emit().unwrap();
        match msg {
            BroadcastMsg::RequestVote { term, from } => {
                assert_eq!(term, 1);
                assert_eq!(from, Node(1));
            }
            BroadcastMsg::Ping { .. } => panic!(),
        }

        let resp = s2.request_vote(Node(1), 1, false);
        assert_eq!(resp.vote_granted, false);
        assert_eq!(resp.spawn_election_timer, false);

        assert!(!s1.respond_vote(Node(2), s2.term(), resp.vote_granted));
        match s1 {
            State::Follower(_) => panic!(),
            State::Candidate(_) => {}
            State::Leader(_) => panic!(),
        }

        let resp = s3.request_vote(Node(1), 1, false);
        assert_eq!(resp.vote_granted, true);
        assert_eq!(resp.spawn_election_timer, true);
        assert_eq!(s3.term(), 1);

        assert!(!s1.respond_vote(Node(3), s3.term(), resp.vote_granted,));
        match s1 {
            State::Follower(_) => panic!(),
            State::Candidate(_) => panic!(),
            State::Leader(_) => {}
        }

        let msg = s2.emit().unwrap();
        match msg {
            BroadcastMsg::RequestVote { term, from } => {
                assert_eq!(term, 1);
                assert_eq!(from, Node(2));
            }
            BroadcastMsg::Ping { .. } => panic!(),
        }

        let resp = s1.request_vote(Node(2), 1, false);
        assert_eq!(resp.vote_granted, false);
        assert_eq!(resp.spawn_election_timer, false);

        assert!(!s2.respond_vote(Node(1), s1.term(), resp.vote_granted,));
        match s2 {
            State::Follower(_) => panic!(),
            State::Candidate(_) => {}
            State::Leader(_) => panic!(),
        }

        let resp = s3.request_vote(Node(2), 1, false);
        assert_eq!(resp.vote_granted, false);
        assert_eq!(resp.spawn_election_timer, false);

        assert!(!s2.respond_vote(Node(3), s3.term(), resp.vote_granted,));
        match s2 {
            State::Follower(_) => panic!(),
            State::Candidate(_) => {}
            State::Leader(_) => panic!(),
        }

        assert!(s3.emit().is_none());

        let msg = s1.emit().unwrap();
        match msg {
            BroadcastMsg::RequestVote { .. } => panic!(),
            BroadcastMsg::Ping { term } => assert_eq!(term, 1),
        }

        assert!(s2.ping(s1.term()).unwrap());
        match s2 {
            State::Follower(_) => {}
            State::Candidate(_) => panic!(),
            State::Leader(_) => panic!(),
        }

        assert!(!s1.pong(s2.term()).unwrap());

        assert!(s3.ping(s1.term()).unwrap());

        assert!(!s1.pong(s3.term()).unwrap());
    }
}
