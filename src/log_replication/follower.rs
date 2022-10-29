use std::collections::VecDeque;

use crate::{
    log::{EntryState, Log},
    Term,
};

use super::EntryMeta;

pub struct Follower {
    log: Log,
}

impl Follower {
    #[must_use]
    pub fn new(log: Log) -> Follower {
        Follower { log }
    }

    #[must_use]
    pub fn append(
        &mut self,
        mut new_entries: VecDeque<Term>,
        prev_entry: Option<EntryMeta>,
    ) -> AppendRes {
        let mut start = match prev_entry {
            Some(prev) => {
                let (term, state) = match self.log.entry(prev.index) {
                    Some(v) => v,
                    None => return AppendRes::NewEntriesTooFarAhead,
                };
                if term != prev.term {
                    match state {
                        EntryState::Committed => return AppendRes::CommittedLogMismatch,
                        EntryState::Uncommitted => {
                            // We need to truncate the log
                            self.log.remove_uncommitted_from(prev.index);
                            return AppendRes::NewEntriesTooFarAhead;
                        }
                    }
                }
                prev.index + 1
            }
            None => 0,
        };

        // discard duplicate entries
        while let Some(&term) = new_entries.front() {
            if let Some((existing_term, state)) = self.log.entry(start) {
                if existing_term == term {
                    new_entries.pop_front();
                    start += 1;
                    continue;
                }
                match state {
                    EntryState::Committed => return AppendRes::CommittedLogMismatch,
                    EntryState::Uncommitted => {
                        // We need to truncate the log
                        self.log.remove_uncommitted_from(start);
                        break;
                    }
                }
            } else {
                break;
            }
        }

        if !new_entries.is_empty() {
            // We can append the new entries
            self.log.append(new_entries.into_iter());
        }

        AppendRes::Success
    }

    pub fn commit(&mut self, index: usize) -> Result<(), CommitError> {
        match self.log.try_commit(index) {
            true => Ok(()),
            false => Err(CommitError::LogTooShort),
        }
    }

    pub fn into_log(self) -> Log {
        self.log
    }

    pub fn log(&self) -> &Log {
        &self.log
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum AppendRes {
    CommittedLogMismatch,
    Success,
    NewEntriesTooFarAhead,
}

#[derive(Debug)]
pub enum CommitError {
    LogTooShort,
}

#[cfg(test)]
mod tests {
    use crate::log_replication::EntryMeta;

    use super::*;

    #[test]
    fn append_none_prev_to_empty_log() {
        let mut follower = Follower::new(Log::new());
        let res = follower.append(vec![0].into(), None);
        assert_eq!(res, AppendRes::Success);
        assert_eq!(follower.log.uncommitted().len(), 1);
    }

    #[test]
    fn append_none_prev_to_uncommitted_log() {
        let mut follower = Follower::new(Log::new());

        let res = follower.append(vec![0].into(), None);
        assert_eq!(res, AppendRes::Success);

        let res = follower.append(vec![0].into(), None);
        assert_eq!(res, AppendRes::Success);
        assert_eq!(follower.log.uncommitted().len(), 1);
    }

    #[test]
    fn append_none_prev_to_committed_log() {
        let mut follower = Follower::new(Log::new());

        let res = follower.append(vec![0].into(), None);
        assert_eq!(res, AppendRes::Success);

        follower.commit(0).unwrap();

        let res = follower.append(vec![0].into(), None);
        assert_eq!(res, AppendRes::Success);
        assert_eq!(follower.log.committed().len(), 1);

        let res = follower.append(vec![1].into(), None);
        assert_eq!(res, AppendRes::CommittedLogMismatch);
    }

    #[test]
    fn append_some_prev_to_empty_log() {
        let mut follower = Follower::new(Log::new());
        let res = follower.append(vec![0].into(), Some(EntryMeta { index: 0, term: 0 }));
        assert_eq!(res, AppendRes::NewEntriesTooFarAhead);
        assert_eq!(follower.log.uncommitted().len(), 0);
    }

    #[test]
    fn append_some_prev_to_uncommitted_log() {
        let mut follower = Follower::new(Log::new());

        let res = follower.append(vec![0].into(), None);
        assert_eq!(res, AppendRes::Success);

        let res = follower.append(vec![0].into(), Some(EntryMeta { index: 0, term: 0 }));
        assert_eq!(res, AppendRes::Success);
        assert_eq!(follower.log.uncommitted().len(), 2);
    }

    #[test]
    fn append_some_prev_to_committed_log() {
        let mut follower = Follower::new(Log::new());

        // [][]

        let res = follower.append(vec![0].into(), None);
        assert_eq!(res, AppendRes::Success);

        // [][0]

        follower.commit(0).unwrap();

        // [0][]

        let res = follower.append(vec![0].into(), Some(EntryMeta { index: 0, term: 0 }));
        assert_eq!(res, AppendRes::Success);
        assert_eq!(follower.log.committed().len(), 1);

        // [0][0]

        let res = follower.append(vec![1].into(), Some(EntryMeta { index: 0, term: 1 }));
        assert_eq!(res, AppendRes::CommittedLogMismatch);
    }
}
