pub mod state;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Node(pub u64);

pub type Term = u64;

pub enum Msg {
    RequestVote { term: Term, from: Node },
}
