use crate::{Node, Term};

pub mod candidate;
pub mod follower;
pub mod leader;

pub struct VoteResp {
    pub from: Node,
    pub term: Term,
    pub vote_granted: bool,
}

pub struct AppendEntriesResp {
    pub from: Node,
    pub term: Term,
    pub success: bool,
}

#[cfg(test)]
mod tests {
    use crate::{
        log::Log,
        log_replication,
        raft::{candidate, follower, leader},
        Facts, Node,
    };

    use super::follower::Follower;

    #[test]
    fn paper_figure_8() {
        let s1 = {
            let peers = vec![Node(2), Node(3), Node(4), Node(5)];
            let facts = Facts {
                id: Node(1),
                nodes: 5,
            };
            Follower::new(peers, facts, 0, Log::new())
        };

        let s2 = {
            let peers = vec![Node(1), Node(3), Node(4), Node(5)];
            let facts = Facts {
                id: Node(2),
                nodes: 5,
            };
            Follower::new(peers, facts, 0, Log::new())
        };

        let s3 = {
            let peers = vec![Node(1), Node(2), Node(4), Node(5)];
            let facts = Facts {
                id: Node(3),
                nodes: 5,
            };
            Follower::new(peers, facts, 0, Log::new())
        };

        let s4 = {
            let peers = vec![Node(1), Node(2), Node(3), Node(5)];
            let facts = Facts {
                id: Node(4),
                nodes: 5,
            };
            Follower::new(peers, facts, 0, Log::new())
        };

        let s5 = {
            let peers = vec![Node(1), Node(2), Node(3), Node(4)];
            let facts = Facts {
                id: Node(5),
                nodes: 5,
            };
            Follower::new(peers, facts, 0, Log::new())
        };

        let s1 = s1.start_election();

        let vote_req = s1.emit();

        let (res, vote_resp) = s2.receive_vote_req(vote_req.from, vote_req.term, vote_req.last_log);

        let s2 = match res {
            follower::ReceiveVoteReqRes::TermUpgraded(v) => v,
            follower::ReceiveVoteReqRes::NotUpgraded(_) => panic!(),
        };

        assert!(vote_resp.vote_granted);

        let s1 = match s1
            .receive_vote_resp(vote_resp.from, vote_resp.term, vote_resp.vote_granted)
            .unwrap()
        {
            candidate::ReceiveVoteRespRes::TermUpgraded(_) => panic!(),
            candidate::ReceiveVoteRespRes::StaleTermNotUpgradedOrNotElectedYet(v) => v,
            candidate::ReceiveVoteRespRes::Elected(_) => panic!(),
        };

        let (res, vote_resp) = s3.receive_vote_req(vote_req.from, vote_req.term, vote_req.last_log);

        let s3 = match res {
            follower::ReceiveVoteReqRes::TermUpgraded(v) => v,
            follower::ReceiveVoteReqRes::NotUpgraded(_) => panic!(),
        };

        let s1 = match s1
            .receive_vote_resp(vote_resp.from, vote_resp.term, vote_resp.vote_granted)
            .unwrap()
        {
            candidate::ReceiveVoteRespRes::TermUpgraded(_) => panic!(),
            candidate::ReceiveVoteRespRes::StaleTermNotUpgradedOrNotElectedYet(_) => panic!(),
            candidate::ReceiveVoteRespRes::Elected(v) => v,
        };

        let append_entries_req = s1.emit(Node(5)).unwrap();

        assert_eq!(append_entries_req.req.new_entries, vec![]);

        let match_index = append_entries_req.req.match_index_on_success();

        let (res, append_entries_resp) = s5.receive_append_entries_req(
            append_entries_req.term,
            append_entries_req.req.new_entries,
            append_entries_req.req.prev_entry,
            append_entries_req.req.commit_index,
        );

        let s5 = match res {
            follower::ReceiveAppendEntriesReqRes::StaleTermNotUpgraded(_) => panic!(),
            follower::ReceiveAppendEntriesReqRes::LogHandled(v) => v,
        };

        assert!(append_entries_resp.success);

        let s1 = match s1
            .receive_append_entries_resp(
                Node(5),
                append_entries_req.term,
                log_replication::leader::AppendEntriesRes::Success { match_index },
            )
            .unwrap()
        {
            leader::ReceiveAppendEntriesRespRes::TermUpgraded(_) => panic!(),
            leader::ReceiveAppendEntriesRespRes::NotUpgraded(v) => v,
        };

        let mut s1 = s1;
        s1.push();

        let append_entries_req = s1.emit(Node(2)).unwrap();

        assert_eq!(append_entries_req.req.new_entries, vec![1]);

        let match_index = append_entries_req.req.match_index_on_success();

        let (res, append_entries_resp) = s2.receive_append_entries_req(
            append_entries_req.term,
            append_entries_req.req.new_entries,
            append_entries_req.req.prev_entry,
            append_entries_req.req.commit_index,
        );

        let _s2 = match res {
            follower::ReceiveAppendEntriesReqRes::StaleTermNotUpgraded(_) => panic!(),
            follower::ReceiveAppendEntriesReqRes::LogHandled(v) => v,
        };

        assert!(append_entries_resp.success);

        let _s1 = match s1
            .receive_append_entries_resp(
                append_entries_resp.from,
                append_entries_resp.term,
                log_replication::leader::AppendEntriesRes::Success { match_index },
            )
            .unwrap()
        {
            leader::ReceiveAppendEntriesRespRes::TermUpgraded(_) => panic!(),
            leader::ReceiveAppendEntriesRespRes::NotUpgraded(v) => v,
        };

        // (a)

        let s5 = s5.start_election();

        let vote_req = s5.emit();

        let (res, vote_resp) = s3.receive_vote_req(vote_req.from, vote_req.term, vote_req.last_log);

        let _s3 = match res {
            follower::ReceiveVoteReqRes::TermUpgraded(v) => v,
            follower::ReceiveVoteReqRes::NotUpgraded(_) => panic!(),
        };

        assert!(vote_resp.vote_granted);

        let s5 = match s5
            .receive_vote_resp(vote_resp.from, vote_resp.term, vote_resp.vote_granted)
            .unwrap()
        {
            candidate::ReceiveVoteRespRes::TermUpgraded(_) => panic!(),
            candidate::ReceiveVoteRespRes::StaleTermNotUpgradedOrNotElectedYet(v) => v,
            candidate::ReceiveVoteRespRes::Elected(_) => panic!(),
        };

        let (res, vote_resp) = s4.receive_vote_req(vote_req.from, vote_req.term, vote_req.last_log);

        let _s4 = match res {
            follower::ReceiveVoteReqRes::TermUpgraded(v) => v,
            follower::ReceiveVoteReqRes::NotUpgraded(_) => panic!(),
        };

        assert!(vote_resp.vote_granted);

        let s5 = match s5
            .receive_vote_resp(vote_resp.from, vote_resp.term, vote_resp.vote_granted)
            .unwrap()
        {
            candidate::ReceiveVoteRespRes::TermUpgraded(_) => panic!(),
            candidate::ReceiveVoteRespRes::StaleTermNotUpgradedOrNotElectedYet(_) => panic!(),
            candidate::ReceiveVoteRespRes::Elected(v) => v,
        };

        let mut s5 = s5;
        s5.push();

        // (b)
    }
}
