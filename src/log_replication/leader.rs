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

    /// - Goal: to compliment what log entries the follower lacks
    pub fn emit(&self, to: Node) -> Result<AppendEntriesReq, EmitError> {
        let follower_log = self.follower_logs.get(&to).ok_or(EmitError::UnknownNode)?;

        // Get the log index of the entry that:
        // - the follower probably has
        // - the last entry of the follower so that we can just send few entries
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

        // Get the entries that the follower lacks
        let new_entries = self
            .log
            .entries_from(follower_log.next_index)
            .map(|v| *v)
            .collect::<Vec<_>>();

        // Bring in the commit index to permit the follower to commit entries
        Ok(AppendEntriesReq {
            new_entries,
            prev_entry,
            commit_index: self.log.commit_index(),
        })
    }

    /// - Goal: a feedback to adjust:
    ///   - the entry commit when some entries have replicated in the majority of nodes
    ///   - what entries the followers lack
    pub fn append_entries_resp(
        &mut self,
        term: Term,
        from: Node,
        res: AppendEntriesRes,
    ) -> Result<(), AppendEntriesError> {
        let follower_log = self
            .follower_logs
            .get_mut(&from)
            .ok_or(AppendEntriesError::UnknownNode)?;

        // Adjust the information of what entries the follower lacks
        match res {
            AppendEntriesRes::Success { match_index } => {
                if match_index >= self.log.len() {
                    return Err(AppendEntriesError::InvalidMatchIndex);
                }
                if let Some(existing_match_index) = follower_log.match_index {
                    if match_index < existing_match_index {
                        return Err(AppendEntriesError::MatchIndexTooSmall);
                    }
                }

                // Mark all the entries up to the match index as replicated
                follower_log.match_index = Some(match_index);

                // To send the rest of the entries in the future
                follower_log.next_index = match_index + 1;
            }
            AppendEntriesRes::Failure { new_next_index } => {
                if new_next_index >= self.log.len() {
                    return Err(AppendEntriesError::InvalidNextIndex);
                }
                if follower_log.next_index <= new_next_index {
                    return Err(AppendEntriesError::NextIndexTooLarge);
                }

                // The follower lacks even more entries than anticipated before
                // To send more old entries in the future
                follower_log.next_index = new_next_index;

                return Ok(());
            }
        }

        // Commit entries that have replicated in the majority of nodes
        if let Some(&last_term) = self.log.uncommitted().back() {
            if last_term != term {
                // a leader cannot determine commitment using log entries from older terms
                return Ok(());
            }

            let mut match_indices = self
                .follower_logs
                .values()
                .filter_map(|follower_log| follower_log.match_index)
                .collect::<Vec<_>>();

            match_indices.sort_unstable();

            let commit_index = match_indices[match_indices.len() / 2];

            let success = self.log.try_commit(commit_index);

            // SAFETY: none of the `match_indices` can go beyond the length of the log
            assert!(success);
        }
        Ok(())
    }

    pub fn into_log(self) -> Log {
        self.log
    }

    pub fn log(&self) -> &Log {
        &self.log
    }
}

struct FollowerLog {
    /// - Next index to send to the follower
    next_index: usize,

    /// - Goal: the majority of them determines how many new entries the leader is allowed to commit
    match_index: Option<usize>,
}

pub struct AppendEntriesReq {
    pub new_entries: Vec<Term>,
    pub prev_entry: Option<EntryMeta>,
    pub commit_index: Option<usize>,
}

pub enum AppendEntriesRes {
    Success { match_index: usize },
    Failure { new_next_index: usize },
}

#[derive(Debug)]
pub enum EmitError {
    UnknownNode,
}

#[derive(Debug)]
pub enum AppendEntriesError {
    UnknownNode,
    InvalidMatchIndex,

    /// - Could happen if the leader receives an out-of-order response
    MatchIndexTooSmall,

    InvalidNextIndex,

    /// - Could happen if the leader receives an out-of-order response
    NextIndexTooLarge,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emit_none_prev() {
        let log = Log::new();
        let leader = Leader::new(log, &[Node(1)]);
        let req = leader.emit(Node(1)).unwrap();
        assert_eq!(req.prev_entry, None);
        assert_eq!(req.new_entries, vec![]);
        assert_eq!(req.commit_index, None);
    }

    #[test]
    fn emit_some_prev() {
        let mut log = Log::new();
        log.append(vec![1]);
        let mut leader = Leader::new(log, &[Node(1)]);

        leader.follower_logs.get_mut(&Node(1)).unwrap().next_index = 1;
        leader.follower_logs.get_mut(&Node(1)).unwrap().match_index = None;

        let req = leader.emit(Node(1)).unwrap();

        assert_eq!(req.prev_entry, Some(EntryMeta { index: 0, term: 1 }));
        assert_eq!(req.new_entries, vec![]);
        assert_eq!(req.commit_index, None);
    }

    #[test]
    fn append_entries_resp_success() {
        let mut log = Log::new();
        log.append(vec![1]);
        let mut leader = Leader::new(log, &[Node(1)]);

        assert_eq!(leader.follower_logs.get(&Node(1)).unwrap().next_index, 1);
        assert_eq!(
            leader.follower_logs.get(&Node(1)).unwrap().match_index,
            None
        );

        // Request:
        // prev_entry = None
        // new_entries = [1]

        leader
            .append_entries_resp(1, Node(1), AppendEntriesRes::Success { match_index: 0 })
            .unwrap();
        assert_eq!(leader.log().commit_index(), Some(0));
    }

    #[test]
    fn append_entries_resp_failure() {
        let mut log = Log::new();
        log.append(vec![1]);
        let mut leader = Leader::new(log, &[Node(1)]);

        assert_eq!(leader.follower_logs.get(&Node(1)).unwrap().next_index, 1);
        assert_eq!(
            leader.follower_logs.get(&Node(1)).unwrap().match_index,
            None
        );

        leader
            .append_entries_resp(1, Node(1), AppendEntriesRes::Failure { new_next_index: 0 })
            .unwrap();

        assert_eq!(leader.log().commit_index(), None);
        assert_eq!(leader.follower_logs.get(&Node(1)).unwrap().next_index, 0);
        assert_eq!(
            leader.follower_logs.get(&Node(1)).unwrap().match_index,
            None
        );
    }
}
