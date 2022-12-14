pub mod election;
pub mod log;
pub mod log_replication;
pub mod raft;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Node(pub u64);

pub type Term = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Facts {
    pub id: Node,
    pub nodes: usize,
}
