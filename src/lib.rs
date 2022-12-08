use std::sync::Arc;

mod queue;

use queue::{InnerQueue, State};

#[derive(Debug, PartialEq, Eq)]
pub enum PushError<T> {
    Full(T),
    Busy(T),
}
#[derive(Debug, PartialEq, Eq)]
pub enum PopError {
    Empty,
    Busy,
}

/// A Block-based Bounded Queue for Exchanging Data and Profiling
pub struct BQueue<T>(Arc<InnerQueue<T>>);
impl<T> Clone for BQueue<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// There is a problem with this structure, I don't know how to release memory properly after
/// repeated writes, so I won't open it to users for now
pub(crate) struct LQueue<T>(Arc<InnerQueue<T>>);
impl<T> Clone for LQueue<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> BQueue<T> {
    pub fn new(block_size: usize, block_num: usize) -> Self {
        let q = InnerQueue::new(block_size, block_num);
        BQueue(Arc::new(q))
    }

    // default with 4 block size, so block num is cap/4 + 1
    pub fn with_capacity(cap: usize) -> Self {
        let q = InnerQueue::with_capacity(cap);
        BQueue(Arc::new(q))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn push(&self, data: T) -> Result<(), PushError<T>> {
        loop {
            let (ph, block) = self.0.get_phead_and_block();
            match unsafe { self.0.allocate_entry(block) } {
                State::Allocated(entry) => {
                    self.0.commit_entry(entry, data);
                    return Ok(());
                }
                State::BlockDone(_) => match unsafe { self.0.advance_phead_retry_new(ph) } {
                    State::NoEntry => return Err(PushError::Full(data)),
                    State::NotAvailable => return Err(PushError::Busy(data)),
                    State::Success => {
                        continue;
                    }
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            }
        }
    }

    pub fn pop(&self) -> Result<T, PopError> {
        loop {
            let (ch, block) = self.0.get_chead_and_block();
            match unsafe { self.0.reserve_entry(block) } {
                State::Reserved(entry) => {
                    let data = self.0.consume_entry_retry_new(entry);
                    return Ok(data);
                }
                State::NoEntry => return Err(PopError::Empty),
                State::NotAvailable => return Err(PopError::Busy),
                State::BlockDone(vsn) => {
                    if unsafe { !self.0.advance_chead_retry_new(ch, vsn) } {
                        return Err(PopError::Empty);
                    }
                }
                _ => unreachable!(),
            }
        }
    }
}

impl<T> LQueue<T> {
    pub fn new(block_size: usize, block_num: usize) -> Self {
        let q = InnerQueue::new(block_size, block_num).retry_new(false);
        LQueue(Arc::new(q))
    }

    pub fn with_capacity(cap: usize) -> Self {
        let q = InnerQueue::with_capacity(cap).retry_new(false);
        LQueue(Arc::new(q))
    }

    pub fn push(&self, data: T) -> Result<(), PushError<T>> {
        loop {
            let (ph, block) = self.0.get_phead_and_block();
            match unsafe { self.0.allocate_entry(block) } {
                State::Allocated(entry) => {
                    self.0.commit_entry(entry, data);
                    return Ok(());
                }
                State::BlockDone(_) => match unsafe { self.0.advance_phead_drop_old(ph) } {
                    State::NoEntry => return Err(PushError::Full(data)),
                    State::NotAvailable => return Err(PushError::Busy(data)),
                    State::Success => continue,
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            }
        }
    }

    pub fn pop(&self) -> Result<T, PopError> {
        loop {
            let (ch, block) = self.0.get_chead_and_block();
            match unsafe { self.0.reserve_entry(block) } {
                State::Reserved(entry) => {
                    if let Some(data) = self.0.consume_entry_drop_old(entry) {
                        return Ok(data);
                    }
                }
                State::NoEntry => return Err(PopError::Empty),
                State::NotAvailable => return Err(PopError::Busy),
                State::BlockDone(vsn) => {
                    if unsafe { !self.0.advance_chead_drop_old(ch, vsn) } {
                        return Err(PopError::Empty);
                    }
                }
                _ => unreachable!(),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::queue::{Cursor, Header};

    use super::*;

    #[test]
    fn test_lqueue_push_pop() {
        let (tx, rx) = {
            let tx = LQueue::new(1, 3);
            let rx = tx.clone();
            (tx, rx)
        };

        tx.push(1).unwrap();

        tx.push(2).unwrap();

        tx.push(3).unwrap();

        assert_eq!(rx.pop().unwrap(), 1);

        assert_eq!(rx.pop().unwrap(), 2);

        assert_eq!(rx.pop().unwrap(), 3);

        tx.push(1).unwrap();

        tx.push(2).unwrap();

        tx.push(3).unwrap();

        tx.push(4).unwrap();

        assert_eq!(rx.pop().unwrap(), 4);

        assert_eq!(rx.pop().unwrap_err(), PopError::Empty);
    }

    #[test]
    fn test_bqueue_push_pop_2() {
        let (tx, rx) = {
            let tx = BQueue::new(2, 2);
            let rx = tx.clone();
            (tx, rx)
        };

        assert!(tx.is_empty());
        assert_eq!(tx.len(), 0);
        tx.push(1).unwrap();
        assert_eq!(tx.len(), 1);
        assert!(!tx.is_empty());

        tx.push(2).unwrap();
        assert!(!tx.is_empty());
        assert_eq!(tx.len(), 2);

        tx.push(3).unwrap();
        assert!(!tx.is_empty());
        assert_eq!(tx.len(), 3);

        tx.push(4).unwrap();
        assert!(!tx.is_empty());
        assert_eq!(tx.len(), 4);

        assert_eq!(rx.pop().unwrap(), 1);
        assert!(!tx.is_empty());
        assert_eq!(tx.len(), 3);

        assert_eq!(rx.pop().unwrap(), 2);
        assert!(!tx.is_empty());
        assert_eq!(tx.len(), 2);

        assert_eq!(rx.pop().unwrap(), 3);
        assert!(!tx.is_empty());
        assert_eq!(tx.len(), 1);

        assert_eq!(rx.pop().unwrap(), 4);
        assert!(tx.is_empty());
        assert_eq!(tx.len(), 0);

        tx.push(1).unwrap();
        assert!(!tx.is_empty());
        assert_eq!(tx.len(), 1);

        tx.push(2).unwrap();
        assert!(!tx.is_empty());
        assert_eq!(tx.len(), 2);

        tx.push(3).unwrap();
        assert!(!tx.is_empty());
        assert_eq!(tx.len(), 3);

        tx.push(4).unwrap();
        assert!(!tx.is_empty());
        assert_eq!(tx.len(), 4);

        assert_eq!(rx.pop().unwrap(), 1);
        assert_eq!(tx.len(), 3);

        assert_eq!(rx.pop().unwrap(), 2);
        assert_eq!(tx.len(), 2);

        tx.push(4).unwrap();
        assert_eq!(tx.len(), 3);
        tx.push(5).unwrap();
        assert_eq!(tx.len(), 4);

        assert_eq!(rx.pop().unwrap(), 3);
        assert_eq!(tx.len(), 3);

        assert_eq!(rx.pop().unwrap(), 4);
        assert_eq!(tx.len(), 2);
    }

    #[test]
    fn test_bqueue_push_pop() {
        let (tx, rx) = {
            let tx = BQueue::new(1, 3);
            let rx = tx.clone();
            (tx, rx)
        };

        let assert_ph = |tx: &BQueue<i32>, ph: Header, committed: Cursor, allocated: Cursor| {
            let (ph_r, b) = tx.0.get_phead_and_block();
            assert_eq!(ph_r, ph);
            unsafe {
                assert_eq!((*b).committed.load(tx.0.offset_size), committed);

                assert_eq!((*b).allocated.load(tx.0.offset_size), allocated);
            }
        };

        let assert_ch = |tx: &BQueue<i32>, ch: Header, reserved: Cursor, consumed: Cursor| {
            let (ch_r, b) = tx.0.get_chead_and_block();
            assert_eq!(ch_r, ch);
            unsafe {
                assert_eq!((*b).reserved.load(tx.0.offset_size), reserved);

                assert_eq!((*b).consumed.load(tx.0.offset_size), consumed);
            }
        };

        assert!(tx.is_empty());
        tx.push(1).unwrap();
        assert!(!tx.is_empty());
        tx.push(2).unwrap();
        assert!(!tx.is_empty());
        tx.push(3).unwrap();
        assert!(!tx.is_empty());
        assert_eq!(tx.push(10).unwrap_err(), PushError::Full(10));
        assert!(!tx.is_empty());
        assert_ph(
            &tx,
            Header {
                version: 0,
                index: 2,
            },
            Cursor {
                version: 1,
                offset: 1,
            },
            Cursor {
                version: 1,
                offset: 1,
            },
        );

        assert_eq!(rx.pop().unwrap(), 1);
        assert!(!tx.is_empty());
        assert_ch(
            &tx,
            Header {
                version: 0,
                index: 0,
            },
            Cursor {
                version: 0,
                offset: 1,
            },
            Cursor {
                version: 0,
                offset: 1,
            },
        );

        assert_eq!(rx.pop().unwrap(), 2);
        assert!(!tx.is_empty());
        assert_ch(
            &tx,
            Header {
                version: 0,
                index: 1,
            },
            Cursor {
                version: 1,
                offset: 1,
            },
            Cursor {
                version: 1,
                offset: 1,
            },
        );
        assert_eq!(rx.pop().unwrap(), 3);
        assert!(tx.is_empty());
        assert_eq!(tx.pop().unwrap_err(), PopError::Empty);
        tx.push(4).unwrap();
        assert!(!tx.is_empty());
        assert_ph(
            &tx,
            Header {
                version: 1,
                index: 0,
            },
            Cursor {
                version: 1,
                offset: 1,
            },
            Cursor {
                version: 1,
                offset: 1,
            },
        );

        tx.push(5).unwrap();
        assert!(!tx.is_empty());
        assert_ph(
            &tx,
            Header {
                version: 1,
                index: 1,
            },
            Cursor {
                version: 2,
                offset: 1,
            },
            Cursor {
                version: 2,
                offset: 1,
            },
        );

        tx.push(6).unwrap();
        assert!(!tx.is_empty());
        assert_ph(
            &tx,
            Header {
                version: 1,
                index: 2,
            },
            Cursor {
                version: 2,
                offset: 1,
            },
            Cursor {
                version: 2,
                offset: 1,
            },
        );
        assert_ch(
            &tx,
            Header {
                version: 0,
                index: 2,
            },
            Cursor {
                version: 1,
                offset: 1,
            },
            Cursor {
                version: 1,
                offset: 1,
            },
        );

        assert_eq!(tx.push(10).unwrap_err(), PushError::Full(10));
        assert!(!tx.is_empty());

        assert_eq!(rx.pop().unwrap(), 4);
        assert!(!tx.is_empty());

        assert_eq!(rx.pop().unwrap(), 5);
        assert!(!tx.is_empty());

        assert_eq!(rx.pop().unwrap(), 6);
        assert!(tx.is_empty());
        assert_eq!(tx.pop().unwrap_err(), PopError::Empty);

        tx.push(7).unwrap();
        assert!(!tx.is_empty());

        assert_eq!(rx.pop().unwrap(), 7);
        assert!(tx.is_empty());
        assert_eq!(tx.pop().unwrap_err(), PopError::Empty);
    }

    #[test]
    fn test_bqueue_push_pop_multi_thread() {
        let (tx, rx) = {
            let tx = BQueue::new(2, 3);
            let rx = tx.clone();
            (tx, rx)
        };

        for _ in 0..2 {
            let a = tx.clone();
            std::thread::spawn(move || {
                for i in 0..100 {
                    loop {
                        match a.push(i) {
                            Ok(_) => break,
                            Err(e) => {
                                if let PushError::Busy(_) = e {
                                    std::hint::spin_loop()
                                }
                            }
                        }
                    }
                }
            });
        }

        let mut res = Vec::with_capacity(200);
        loop {
            match rx.pop() {
                Ok(r) => {
                    res.push(r);
                }
                Err(e) => {
                    if let PopError::Busy = e {
                        std::hint::spin_loop()
                    }
                }
            }
            if res.len() == 200 {
                assert_eq!(rx.pop().unwrap_err(), PopError::Empty);
                res.sort_unstable();
                let mut expect = Vec::from_iter(0..100);
                expect.extend(0..100);
                expect.sort_unstable();
                assert_eq!(res, expect);
                break;
            }
        }
    }

    #[test]
    fn test_push_full() {
        let tx = BQueue::new(3, 5);
        for i in 0..15 {
            tx.push(i).unwrap();
        }
        tx.pop().unwrap();
        assert!(tx.push(2).is_err())
    }

    #[test]
    fn test_drops_bqueue() {
        const RUNS: usize = if cfg!(miri) { 10 } else { 100 };

        static DROPS: AtomicUsize = AtomicUsize::new(0);

        use rand::{thread_rng, Rng};
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Debug, PartialEq)]
        struct DropCounter;

        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::SeqCst);
            }
        }

        let mut rng = thread_rng();

        for _ in 0..RUNS {
            let steps = rng.gen_range(0..if cfg!(miri) { 100 } else { 10_000 });
            let additional = rng.gen_range(0..50);

            DROPS.store(0, Ordering::SeqCst);
            let (mut p, c) = {
                let tx = BQueue::new(10, 6);
                let rx = tx.clone();
                (tx, rx)
            };
            let push_thread = std::thread::spawn(move || {
                for _ in 0..steps {
                    while p.push(DropCounter).is_err() {
                        DROPS.fetch_sub(1, Ordering::SeqCst);
                    }
                }
                p
            });
            let pop_thread = std::thread::spawn(move || {
                for _ in 0..steps {
                    while c.pop().is_err() {}
                }
            });

            p = push_thread.join().unwrap();
            pop_thread.join().unwrap();

            for i in 0..additional {
                p.push(DropCounter).unwrap()
                // if p.push(DropCounter).is_err() {
                //     additional = additional - (additional - i);
                //     break;
                // }
            }

            assert_eq!(DROPS.load(Ordering::SeqCst), steps);
            drop(p);
            assert_eq!(DROPS.load(Ordering::SeqCst), steps + additional);
        }
    }
}
