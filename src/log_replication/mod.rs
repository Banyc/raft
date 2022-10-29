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

    pub fn into_leader(self, nodes: &[Node]) -> State {
        match self {
            State::Follower(follower) => {
                let leader = Leader::new(follower.into_log(), nodes);
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
