pub mod election;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Node(pub u64);

pub type Term = u64;
