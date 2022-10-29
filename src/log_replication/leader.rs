use std::collections::HashMap;

use crate::{log::Log, Node, Term};

use super::EntryMeta;

pub struct Leader {
    log: Log,
    follower_logs: HashMap<Node, FollowerLog>,
}

impl Leader {
    pub fn new(log: Log, followers: &[Node]) -> Self {
        let mut follower_logs = HashMap::new();
        for id in followers {
            follower_logs.insert(
                *id,
                FollowerLog {
                    next_index: log.len(),
                    match_index: None,
                },
            );
        }
        Leader { log, follower_logs }
    }

    pub fn emit(&self, to: Node) -> Result<AppendEntriesReq, EmitError> {
        let follower_log = self.follower_logs.get(&to).ok_or(EmitError::UnknownNode)?;
        let prev_entry = if follower_log.next_index == 0 {
            None
        } else {
            let prev_index = follower_log.next_index - 1;
            let (prev_term, _) = self.log.entry(prev_index).unwrap();
            Some(EntryMeta {
                index: prev_index,
                term: prev_term,
            })
        };
        let new_entries = self
            .log
            .entries_from(follower_log.next_index)
            .map(|v| *v)
            .collect::<Vec<_>>();
        Ok(AppendEntriesReq {
            new_entries,
            prev_entry,
            commit_index: self.log.commit_index(),
        })
    }

    pub fn append_entries_resp(&mut self, term: Term, from: Node, res: AppendEntriesRes) {
        let follower_log = self.follower_logs.get_mut(&from).unwrap();
        match res {
            AppendEntriesRes::Success { match_index } => {
                follower_log.match_index = Some(match_index);
                follower_log.next_index = match_index + 1;
            }
            // AppendEntriesRes::Failure { next_index } => {
            //     follower_log.next_index = next_index;
            // }
            AppendEntriesRes::Failure => {
                follower_log.next_index -= 1;

                return;
            }
        }

        // commit
        if let Some(&last_term) = self.log.uncommitted().back() {
            if last_term != term {
                // a leader cannot determine commitment using log entries from older terms
                return;
            }
            let mut match_indices = self
                .follower_logs
                .values()
                .filter_map(|follower_log| follower_log.match_index)
                .collect::<Vec<_>>();
            match_indices.sort_unstable();
            let commit_index = match_indices[match_indices.len() / 2];
            let success = self.log.try_commit(commit_index);
            assert!(success);
        }
    }

    pub fn into_log(self) -> Log {
        self.log
    }

    pub fn log(&self) -> &Log {
        &self.log
    }
}

struct FollowerLog {
    next_index: usize,
    match_index: Option<usize>,
}

pub struct AppendEntriesReq {
    pub new_entries: Vec<Term>,
    pub prev_entry: Option<EntryMeta>,
    pub commit_index: Option<usize>,
}

pub enum AppendEntriesRes {
    Success { match_index: usize },
    // Failure { next_index: usize },
    Failure,
}

#[derive(Debug)]
pub enum EmitError {
    UnknownNode,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emit() {
        let log = Log::new();
        let leader = Leader::new(log, &[Node(1)]);
        let req = leader.emit(Node(1)).unwrap();
        assert_eq!(req.prev_entry, None);
        assert_eq!(req.new_entries, vec![]);
        assert_eq!(req.commit_index, None);
    }
}
