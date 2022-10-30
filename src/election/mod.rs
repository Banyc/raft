use crate::{Node, Term};

pub mod candidate;
pub mod follower;
pub mod leader;

pub use self::candidate::Candidate;
pub use self::follower::Follower;
pub use self::leader::Leader;

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
    use crate::Facts;

    use super::*;

    #[test]
    fn test_full_election() {
        let s1 = Follower::new(
            Facts {
                id: Node(1),
                nodes: 3,
            },
            0,
        );
        let s2 = Follower::new(
            Facts {
                id: Node(2),
                nodes: 3,
            },
            0,
        );
        let s3 = Follower::new(
            Facts {
                id: Node(3),
                nodes: 3,
            },
            0,
        );

        let s1 = s1.start_election();

        let s2 = s2.start_election();

        // s1 broadcasts a vote request

        let request_vote = s1.emit();

        assert_eq!(request_vote.term, 1);
        assert_eq!(request_vote.from, Node(1));

        // s2 receives the vote request from s1

        let s2 = match s2.try_upgrade_term(request_vote.term) {
            candidate::TryUpgradeTermRes::Upgraded(_) => panic!(),
            candidate::TryUpgradeTermRes::NotUpgraded(v) => v,
        };

        // s2 responds s1 with a vote rejection

        let mut s1 = match s1.receive_vote(Node(2), s2.term(), false).unwrap() {
            candidate::ReceiveVoteRes::Upgraded(_) => panic!(),
            candidate::ReceiveVoteRes::NotUpgraded(v) => v,
        };

        // s3 receives the vote request from s1

        let mut s3 = match s3.try_upgrade_term(request_vote.term) {
            follower::TryUpgradeTermRes::Upgraded(v) => v,
            follower::TryUpgradeTermRes::NotUpgraded(_) => panic!(),
        };

        assert_eq!(s3.term(), 1);

        let vote_granted = s3
            .receive_vote_request(request_vote.from, request_vote.term, false)
            .unwrap();

        assert!(vote_granted);

        // s3 responds s1 with a vote acceptance

        s1 = match s1.try_upgrade_term(s3.term()) {
            candidate::TryUpgradeTermRes::Upgraded(_) => panic!(),
            candidate::TryUpgradeTermRes::NotUpgraded(v) => v,
        };

        let s1 = match s1
            .receive_vote(s3.facts().id, s3.term(), vote_granted)
            .unwrap()
        {
            candidate::ReceiveVoteRes::Upgraded(v) => v,
            candidate::ReceiveVoteRes::NotUpgraded(_) => todo!(),
        };

        // s2 broadcasts a vote request

        let request_vote = s2.emit();

        assert_eq!(request_vote.term, 1);
        assert_eq!(request_vote.from, Node(2));

        // s1 receives the vote request from s2

        let s1 = match s1.try_upgrade_term(request_vote.term) {
            leader::TryUpgradeTermRes::Upgraded(_) => panic!(),
            leader::TryUpgradeTermRes::NotUpgraded(v) => v,
        };

        // s1 responds s2 with a vote rejection

        let s2 = match s2.try_upgrade_term(s1.term()) {
            candidate::TryUpgradeTermRes::Upgraded(_) => panic!(),
            candidate::TryUpgradeTermRes::NotUpgraded(v) => v,
        };

        let s2 = match s2.receive_vote(s1.facts().id, s1.term(), false).unwrap() {
            candidate::ReceiveVoteRes::Upgraded(_) => panic!(),
            candidate::ReceiveVoteRes::NotUpgraded(v) => v,
        };

        // s3 receives the vote request from s2

        let mut s3 = match s3.try_upgrade_term(request_vote.term) {
            follower::TryUpgradeTermRes::Upgraded(_) => panic!(),
            follower::TryUpgradeTermRes::NotUpgraded(v) => v,
        };

        let vote_granted = s3
            .receive_vote_request(request_vote.from, request_vote.term, false)
            .unwrap();

        assert!(!vote_granted);

        // s2 receives the vote rejection from s3

        let s2 = match s2.try_upgrade_term(s3.term()) {
            candidate::TryUpgradeTermRes::Upgraded(_) => panic!(),
            candidate::TryUpgradeTermRes::NotUpgraded(v) => v,
        };

        let s2 = match s2
            .receive_vote(s3.facts().id, s3.term(), vote_granted)
            .unwrap()
        {
            candidate::ReceiveVoteRes::Upgraded(_) => panic!(),
            candidate::ReceiveVoteRes::NotUpgraded(v) => v,
        };

        // s1 broadcasts a ping

        let ping = s1.emit();

        assert_eq!(ping.term, 1);

        // s2 receives the ping from s1

        let s2 = match s2.try_upgrade_term(ping.term) {
            candidate::TryUpgradeTermRes::Upgraded(_) => panic!(),
            candidate::TryUpgradeTermRes::NotUpgraded(v) => v,
        };

        let s2 = match s2.receive_ping(ping.term).unwrap() {
            candidate::ReceivePingRes::Upgraded(v) => v,
            candidate::ReceivePingRes::NotUpgraded(_) => panic!(),
        };

        // s1 receives the pong from s2

        let s1 = match s1.try_upgrade_term(s2.term()) {
            leader::TryUpgradeTermRes::Upgraded(_) => panic!(),
            leader::TryUpgradeTermRes::NotUpgraded(v) => v,
        };

        // s3 receives the ping from s1

        let s3 = match s3.try_upgrade_term(ping.term) {
            follower::TryUpgradeTermRes::Upgraded(_) => panic!(),
            follower::TryUpgradeTermRes::NotUpgraded(v) => v,
        };

        // s1 receives the pong from s3

        let _s1 = match s1.try_upgrade_term(s3.term()) {
            leader::TryUpgradeTermRes::Upgraded(_) => panic!(),
            leader::TryUpgradeTermRes::NotUpgraded(v) => v,
        };
    }
}
