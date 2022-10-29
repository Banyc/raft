use std::collections::VecDeque;

use crate::Term;

pub struct Log {
    committed: Vec<Term>,
    uncommitted: VecDeque<Term>,
}

impl Log {
    pub fn new() -> Self {
        Log {
            committed: Vec::new(),
            uncommitted: VecDeque::new(),
        }
    }

    pub fn term(&self, index: usize) -> Option<(Term, EntryState)> {
        if index < self.committed.len() {
            Some((self.committed[index], EntryState::Committed))
        } else if index < self.committed.len() + self.uncommitted.len() {
            Some((
                self.uncommitted[index - self.committed.len()],
                EntryState::Uncommitted,
            ))
        } else {
            None
        }
    }

    fn uncommitted_index(&self, index: usize) -> Option<usize> {
        if index < self.committed.len() {
            None
        } else if index < self.committed.len() + self.uncommitted.len() {
            Some(index - self.committed.len())
        } else {
            None
        }
    }

    pub fn remove_uncommitted_from(&mut self, index: usize) {
        if let Some(uncommitted_index) = self.uncommitted_index(index) {
            self.uncommitted.drain(uncommitted_index..);
        }
    }

    pub fn append(&mut self, new_entries: impl IntoIterator<Item = Term>) {
        self.uncommitted.extend(new_entries);
    }

    pub fn try_commit(&mut self, index: usize) -> bool {
        if index < self.committed.len() {
            return true;
        }
        let uncommitted_index = match self.uncommitted_index(index) {
            Some(v) => v,
            None => return false,
        };
        let mut new_committed = self.uncommitted.drain(..=uncommitted_index).collect();
        self.committed.append(&mut new_committed);

        true
    }

    pub fn committed(&self) -> &[Term] {
        &self.committed
    }

    pub fn uncommitted(&self) -> &VecDeque<Term> {
        &self.uncommitted
    }
}

pub type Command = Vec<u8>;

pub enum EntryState {
    Committed,
    Uncommitted,
}
