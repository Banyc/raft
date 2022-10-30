use crate::{log::Log, Node, Term};

use self::{follower::Follower, leader::Leader};

pub mod follower;
pub mod leader;

pub enum State {
    Follower(Follower),
    Leader(Leader),
}

impl State {
    pub fn new() -> Self {
        State::Follower(Follower::new(Log::new()))
    }

    pub fn into_follower(self) -> State {
        match self {
            State::Follower(_) => self,
            State::Leader(leader) => {
                let follower = Follower::new(leader.into_log());
                State::Follower(follower)
            }
        }
    }

    pub fn into_leader(self, followers: &[Node]) -> State {
        match self {
            State::Follower(follower) => {
                let leader = Leader::new(follower.into_log(), followers);
                State::Leader(leader)
            }
            State::Leader(_) => self,
        }
    }

    pub fn log(&self) -> &Log {
        match self {
            State::Follower(follower) => follower.log(),
            State::Leader(leader) => leader.log(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct EntryMeta {
    pub index: usize,
    pub term: Term,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_entries_success() {
        let mut s1 = Leader::new(Log::new(), &[Node(2)]);
        let mut s2 = Follower::new(Log::new());

        let req = s1.emit(Node(2)).unwrap();

        assert_eq!(req.prev_entry, None);
        assert_eq!(req.new_entries, vec![]);
        assert_eq!(req.commit_index, None);

        let res = s2.append(req.new_entries.into(), req.prev_entry);

        assert_eq!(res, follower::AppendRes::Success);
        assert_eq!(s2.log().committed().len(), 0);
        assert_eq!(s2.log().uncommitted().len(), 0);

        s1.append_entries_resp(
            1,
            Node(2),
            leader::AppendEntriesRes::Success { match_index: None },
        )
        .unwrap();
    }

    #[test]
    fn some_entries_success() {
        let mut s1 = Leader::new(Log::new(), &[Node(2)]);
        let mut s2 = Follower::new(Log::new());

        let idx = s1.log_push(1);

        assert_eq!(idx, 0);

        // s1: [][1]
        // s2: [][]

        let req = s1.emit(Node(2)).unwrap();

        let match_index = req.match_index_on_success();

        assert_eq!(req.prev_entry, None);
        assert_eq!(req.new_entries, vec![1]);
        assert_eq!(req.commit_index, None);

        let res = s2.append(req.new_entries.into(), req.prev_entry);

        assert_eq!(res, follower::AppendRes::Success);
        assert_eq!(s2.log().committed().len(), 0);
        assert_eq!(s2.log().uncommitted().len(), 1);

        // s1: [][1]
        // s2: [][1]

        s1.append_entries_resp(
            1,
            Node(2),
            leader::AppendEntriesRes::Success { match_index },
        )
        .unwrap();

        // s1: [1][]
        // s2: [][1]

        assert_eq!(s1.log().committed().len(), 1);
        assert_eq!(s1.log().uncommitted().len(), 0);

        let req = s1.emit(Node(2)).unwrap();

        let match_index = req.match_index_on_success();

        assert_eq!(req.prev_entry, Some(EntryMeta { index: 0, term: 1 }));
        assert_eq!(req.new_entries, vec![]);
        assert_eq!(req.commit_index, Some(0));

        let res = s2.append(req.new_entries.into(), req.prev_entry);

        s2.commit(req.commit_index.unwrap()).unwrap();

        assert_eq!(res, follower::AppendRes::Success);
        assert_eq!(s2.log().committed().len(), 1);
        assert_eq!(s2.log().uncommitted().len(), 0);

        // s1: [1][]
        // s2: [1][]

        s1.append_entries_resp(
            1,
            Node(2),
            leader::AppendEntriesRes::Success { match_index },
        )
        .unwrap();
    }
}
