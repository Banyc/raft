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

