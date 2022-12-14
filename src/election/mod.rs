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

        // s1 election timer expires

        let s1 = s1.start_election();

        // s1 resets its election timer

        // s2 election timer expires

        let s2 = s2.start_election();

        // s2 resets its election timer

        // s1 broadcasts a vote request

        let request_vote = s1.emit();

        assert_eq!(request_vote.term, 1);
        assert_eq!(request_vote.from, Node(1));

        // s2 receives the vote request from s1

        let s2 = match s2.try_upgrade_term(request_vote.term) {
            candidate::TryUpgradeTermRes::Upgraded(_) => panic!(),
            candidate::TryUpgradeTermRes::StaleTermNotUpgraded(_) => panic!(),
            candidate::TryUpgradeTermRes::SameTermNotUpgraded(v) => v, // s2 does not reset its election timer
        };

        // s2 responds s1 with a vote rejection

        let s1 = match s1.try_upgrade_term_and_receive_vote_resp(Node(2), s2.term(), false) {
            candidate::TryUpgradeTermAndReceiveVoteRespRes::TermUpgraded(_) => panic!(),
            candidate::TryUpgradeTermAndReceiveVoteRespRes::StaleTermNotUpgraded(_) => panic!(),
            candidate::TryUpgradeTermAndReceiveVoteRespRes::Elected(_) => panic!(),
            candidate::TryUpgradeTermAndReceiveVoteRespRes::NotElectedYet(v) => v, // s1 does not reset its election timer
        };

        // s3 receives the vote request from s1

        let (res, vote_granted) =
            s3.try_upgrade_term_and_receive_vote_req(request_vote.from, request_vote.term, false);

        let s3 = match res {
            follower::TryUpgradeTermAndReceiveVoteReqRes::TermUpgraded(v) => v, // s3 resets its election timer
            follower::TryUpgradeTermAndReceiveVoteReqRes::NotUpgraded(_) => panic!(),
        };

        assert!(vote_granted);

        // s3 responds s1 with a vote acceptance

        let s1 =
            match s1.try_upgrade_term_and_receive_vote_resp(s3.facts().id, s3.term(), vote_granted)
            {
                candidate::TryUpgradeTermAndReceiveVoteRespRes::TermUpgraded(_) => panic!(),
                candidate::TryUpgradeTermAndReceiveVoteRespRes::StaleTermNotUpgraded(_) => panic!(),
                candidate::TryUpgradeTermAndReceiveVoteRespRes::Elected(v) => v, // s1 cancels its election timer
                candidate::TryUpgradeTermAndReceiveVoteRespRes::NotElectedYet(_) => panic!(),
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

        let s2 = match s2.try_upgrade_term_and_receive_vote_resp(s1.facts().id, s1.term(), false) {
            candidate::TryUpgradeTermAndReceiveVoteRespRes::TermUpgraded(_) => panic!(),
            candidate::TryUpgradeTermAndReceiveVoteRespRes::StaleTermNotUpgraded(_) => panic!(),
            candidate::TryUpgradeTermAndReceiveVoteRespRes::Elected(_) => panic!(),
            candidate::TryUpgradeTermAndReceiveVoteRespRes::NotElectedYet(v) => v, // s2 does not reset its election timer
        };

        // s3 receives the vote request from s2

        let (res, vote_granted) =
            s3.try_upgrade_term_and_receive_vote_req(request_vote.from, request_vote.term, false);

        let s3 = match res {
            follower::TryUpgradeTermAndReceiveVoteReqRes::TermUpgraded(_) => panic!(),
            follower::TryUpgradeTermAndReceiveVoteReqRes::NotUpgraded(v) => v, // s3 does not reset its election timer
        };

        assert!(!vote_granted);

        // s2 receives the vote rejection from s3

        let s2 =
            match s2.try_upgrade_term_and_receive_vote_resp(s3.facts().id, s3.term(), vote_granted)
            {
                candidate::TryUpgradeTermAndReceiveVoteRespRes::TermUpgraded(_) => panic!(),
                candidate::TryUpgradeTermAndReceiveVoteRespRes::StaleTermNotUpgraded(_) => panic!(),
                candidate::TryUpgradeTermAndReceiveVoteRespRes::Elected(_) => panic!(),
                candidate::TryUpgradeTermAndReceiveVoteRespRes::NotElectedYet(v) => v, // s2 does not reset its election timer
            };

        // s1 broadcasts a ping

        let ping = s1.emit();

        assert_eq!(ping.term, 1);

        // s2 receives the ping from s1

        let s2 = match s2.try_upgrade_term_and_receive_ping(ping.term) {
            candidate::TryUpgradeTermAndReceivePingRes::TermUpgraded(_) => panic!(),
            candidate::TryUpgradeTermAndReceivePingRes::LostElection(v) => v, // s2 resets its election timer
            candidate::TryUpgradeTermAndReceivePingRes::StaleTermNotUpgraded(_) => panic!(),
        };

        // s1 receives the pong from s2

        let s1 = match s1.try_upgrade_term(s2.term()) {
            leader::TryUpgradeTermRes::Upgraded(_) => panic!(),
            leader::TryUpgradeTermRes::NotUpgraded(v) => v,
        };

        // s3 receives the ping from s1

        let s3 = match s3.try_upgrade_term(ping.term) {
            follower::TryUpgradeTermRes::Upgraded(_) => panic!(),
            follower::TryUpgradeTermRes::SameTermNotUpgraded(v) => v, // s3 resets its election timer
            follower::TryUpgradeTermRes::StaleTermNotUpgraded(_) => panic!(),
        };

        // s1 receives the pong from s3

        let _s1 = match s1.try_upgrade_term(s3.term()) {
            leader::TryUpgradeTermRes::Upgraded(_) => panic!(),
            leader::TryUpgradeTermRes::NotUpgraded(v) => v,
        };
    }
}
