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

    pub fn receive_append_req(
        &mut self,
        new_entries: Vec<Term>,
        prev_entry: Option<EntryMeta>,
        commit_index: Option<usize>,
    ) -> Result<bool, ReceiveAppendReqError> {
        if let Some(commit_index) = commit_index {
            if self.log.len() <= commit_index {
                return Err(ReceiveAppendReqError::CommitIndexTooLarge);
            }
        }

        let success = match self.append(new_entries.into(), prev_entry) {
            Ok(_) => true,
            Err(e) => match e {
                AppendError::NewEntriesTooFarAhead => false,
                AppendError::CommittedLogMismatch => {
                    return Err(ReceiveAppendReqError::CommittedLogMismatch)
                }
            },
        };

        if let Some(commit_index) = commit_index {
            // SAFETY: We checked that the commit index is not too large above.
            self.commit(commit_index).unwrap();
        }

        Ok(success)
    }

    fn append(
        &mut self,
        mut new_entries: VecDeque<Term>,
        prev_entry: Option<EntryMeta>,
    ) -> Result<(), AppendError> {
        let mut start = match prev_entry {
            Some(prev) => {
                let (term, state) = match self.log.entry(prev.index) {
                    Some(v) => v,
                    None => return Err(AppendError::NewEntriesTooFarAhead),
                };
                if term != prev.term {
                    match state {
                        EntryState::Committed => return Err(AppendError::CommittedLogMismatch),
                        EntryState::Uncommitted => return Err(AppendError::NewEntriesTooFarAhead),
                    }
                }
                prev.index + 1
            }
            None => 0,
        };

        while let Some(&term) = new_entries.front() {
            if let Some((existing_term, state)) = self.log.entry(start) {
                if existing_term == term {
                    // Discard duplicate entries
                    new_entries.pop_front();
                    start += 1;
                    continue;
                }
                // Overwrite existing entries
                match state {
                    EntryState::Committed => return Err(AppendError::CommittedLogMismatch),
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

        Ok(())
    }

    fn commit(&mut self, index: usize) -> Result<(), CommitError> {
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

#[derive(Debug)]
pub enum ReceiveAppendReqError {
    CommittedLogMismatch,
    CommitIndexTooLarge,
}

#[derive(Debug, PartialEq, Eq)]
pub enum AppendError {
    /// - Respond with success = false
    NewEntriesTooFarAhead,

    /// - Panic
    CommittedLogMismatch,
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
        follower.append(vec![0].into(), None).unwrap();
        assert_eq!(follower.log.uncommitted().len(), 1);
    }

    #[test]
    fn append_none_prev_to_uncommitted_log() {
        let mut follower = Follower::new(Log::new());

        follower.append(vec![0].into(), None).unwrap();

        follower.append(vec![0].into(), None).unwrap();
        assert_eq!(follower.log.uncommitted().len(), 1);
    }

    #[test]
    fn append_none_prev_to_committed_log() {
        let mut follower = Follower::new(Log::new());

        follower.append(vec![0].into(), None).unwrap();

        follower.commit(0).unwrap();

        follower.append(vec![0].into(), None).unwrap();
        assert_eq!(follower.log.committed().len(), 1);

        let res = follower.append(vec![1].into(), None);
        assert_eq!(res, Err(AppendError::CommittedLogMismatch));
    }

    #[test]
    fn append_some_prev_to_empty_log() {
        let mut follower = Follower::new(Log::new());
        let res = follower.append(vec![0].into(), Some(EntryMeta { index: 0, term: 0 }));
        assert_eq!(res, Err(AppendError::NewEntriesTooFarAhead));
        assert_eq!(follower.log.uncommitted().len(), 0);
    }

    #[test]
    fn append_some_prev_to_uncommitted_log() {
        let mut follower = Follower::new(Log::new());

        follower.append(vec![0].into(), None).unwrap();

        follower
            .append(vec![0].into(), Some(EntryMeta { index: 0, term: 0 }))
            .unwrap();
        assert_eq!(follower.log.uncommitted().len(), 2);
    }

    #[test]
    fn append_some_prev_to_committed_log() {
        let mut follower = Follower::new(Log::new());

        // [][]

        follower.append(vec![0].into(), None).unwrap();

        // [][0]

        follower.commit(0).unwrap();

        // [0][]

        follower
            .append(vec![0].into(), Some(EntryMeta { index: 0, term: 0 }))
            .unwrap();
        assert_eq!(follower.log.committed().len(), 1);

        // [0][0]

        let res = follower.append(vec![1].into(), Some(EntryMeta { index: 0, term: 1 }));
        assert_eq!(res, Err(AppendError::CommittedLogMismatch));
    }

    #[test]
    fn append_some_prev_wrong_term() {
        let mut follower = Follower::new(Log::new());

        follower.append(vec![0].into(), None).unwrap();

        // [][0]

        let res = follower.append(vec![0].into(), Some(EntryMeta { index: 0, term: 1 }));

        // [][0]

        assert_eq!(res, Err(AppendError::NewEntriesTooFarAhead));
        assert_eq!(follower.log.uncommitted().len(), 1);
    }

    #[test]
    fn append_overwrite() {
        let mut follower = Follower::new(Log::new());

        follower.append(vec![0].into(), None).unwrap();

        // [][0]

        follower.append(vec![1].into(), None).unwrap();

        // [][1]

        assert_eq!(follower.log.uncommitted().len(), 1);
    }
}
