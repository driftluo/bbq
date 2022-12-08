use std::{
    marker::PhantomData,
    mem::ManuallyDrop,
    sync::atomic::{AtomicU64, Ordering},
};

pub(crate) enum State<T> {
    BlockDone(u64),
    NotAvailable,
    Success,
    Allocated(EntryDesc<T>),
    Reserved(EntryDesc<T>),
    NoEntry,
}

pub(crate) struct EntryDesc<T> {
    block: *mut Block<T>,
    offset: usize,
    version: u64,
}

pub(crate) struct InnerQueue<T> {
    phead: RawHeader,
    chead: RawHeader,

    retry_new: bool,
    block_size: usize,
    block_num: usize,
    pub(crate) offset_size: u32,

    blocks_ptr: *mut Block<T>,
}

impl<T> InnerQueue<T> {
    pub fn with_capacity(cap: usize) -> Self {
        let block_num = cap / 4 + 1;
        Self::new(4, block_num)
    }

    pub fn new(block_size: usize, block_num: usize) -> Self {
        assert!(block_size >= 1);
        assert!(block_num >= 1);
        let offset_size = std::cmp::max(
            64 - block_size.leading_zeros() + 1,
            64 - block_num.leading_zeros(),
        );

        let mut blocks = Vec::with_capacity(block_num);
        let uninit = blocks.spare_capacity_mut();

        for i in 0..block_num {
            let mut block = Block::new(block_size, offset_size);
            if i == 0 {
                block.committed = RawCursor::init_with_zero();
                block.allocated = RawCursor::init_with_zero();
                block.consumed = RawCursor::init_with_zero();
                block.reserved = RawCursor::init_with_zero();
            }
            uninit[i].write(block);
        }

        unsafe { blocks.set_len(block_num) }

        let ptr = ManuallyDrop::new(blocks).as_mut_ptr();

        Self {
            phead: RawHeader::init_with_zero(),
            chead: RawHeader::init_with_zero(),
            block_size,
            block_num,
            offset_size,
            blocks_ptr: ptr,
            retry_new: true,
        }
    }
    pub fn retry_new(mut self, retry_new: bool) -> Self {
        self.retry_new = retry_new;
        self
    }
}

impl<T> InnerQueue<T> {
    pub fn get_phead_and_block(&self) -> (Header, *mut Block<T>) {
        let ph = self.phead.load(self.offset_size);
        (ph, unsafe { self.blocks_ptr.add(ph.index) })
    }

    pub unsafe fn allocate_entry(&self, block: *mut Block<T>) -> State<T> {
        let allocated = (*block).allocated.load(self.offset_size);
        if allocated.offset >= self.block_size {
            return State::BlockDone(allocated.version);
        }

        let old_allocated = version_offset((*block).allocated.faa(1), self.offset_size);

        if old_allocated.offset >= self.block_size {
            return State::BlockDone(old_allocated.version);
        }

        State::Allocated(EntryDesc {
            block,
            offset: old_allocated.offset,
            version: old_allocated.version,
        })
    }

    pub fn commit_entry(&self, e: EntryDesc<T>, data: T) {
        unsafe {
            assert!(!(*e.block).data_ptr.is_null());
            let ptr = (*e.block).data_ptr.add(e.offset);
            assert!(!ptr.is_null());
            ptr.write(data);
            (*e.block).committed.faa(1);
        }
    }

    pub unsafe fn advance_phead_retry_new(&self, mut ph: Header) -> State<T> {
        let nblock = self.blocks_ptr.add((ph.index + 1) % self.block_num);

        let cons = (*nblock).consumed.load(self.offset_size);

        if cons.version < ph.version
            || (cons.version == ph.version && cons.offset != self.block_size)
        {
            let reserved = (*nblock).reserved.load(self.offset_size);
            if reserved.offset == cons.offset {
                return State::NoEntry;
            } else {
                return State::NotAvailable;
            }
        }

        (*nblock).committed.max(
            Cursor {
                version: ph.version + 1,
                offset: 0,
            }
            .into_u64(self.offset_size),
        );

        (*nblock).allocated.max(
            Cursor {
                version: ph.version + 1,
                offset: 0,
            }
            .into_u64(self.offset_size),
        );

        ph.index = (ph.index + 1) % self.block_num;
        if ph.index == 0 {
            ph.version += 1
        }

        self.phead.max(ph.into_u64(self.offset_size));
        State::Success
    }

    pub unsafe fn advance_phead_drop_old(&self, mut ph: Header) -> State<T> {
        let nblock = self.blocks_ptr.add((ph.index + 1) % self.block_num);

        let cmtd = (*nblock).committed.load(self.offset_size);

        if cmtd.version == ph.version && cmtd.offset != self.block_size {
            return State::NotAvailable;
        }

        (*nblock).committed.max(
            Cursor {
                version: ph.version + 1,
                offset: 0,
            }
            .into_u64(self.offset_size),
        );

        (*nblock).allocated.max(
            Cursor {
                version: ph.version + 1,
                offset: 0,
            }
            .into_u64(self.offset_size),
        );

        ph.index = (ph.index + 1) % self.block_num;
        if ph.index == 0 {
            ph.version += 1
        }
        self.phead.max(ph.into_u64(self.offset_size));
        State::Success
    }
}

impl<T> InnerQueue<T> {
    pub fn get_chead_and_block(&self) -> (Header, *mut Block<T>) {
        let ch = self.chead.load(self.offset_size);
        (ch, unsafe { self.blocks_ptr.add(ch.index) })
    }

    pub unsafe fn reserve_entry(&self, block: *mut Block<T>) -> State<T> {
        loop {
            let reserved = (*block).reserved.load(self.offset_size);
            if reserved.offset < self.block_size {
                let committed = (*block).committed.load(self.offset_size);
                if reserved.offset == committed.offset {
                    return State::NoEntry;
                }

                if committed.offset != self.block_size {
                    let allocated = (*block).allocated.load(self.offset_size);
                    if allocated.offset != committed.offset {
                        return State::NotAvailable;
                    }
                }

                let raw_reserved = reserved.into_u64(self.offset_size);

                if (*block).reserved.max(raw_reserved + 1) == raw_reserved {
                    return State::Reserved(EntryDesc {
                        block,
                        offset: reserved.offset,
                        version: reserved.version,
                    });
                } else {
                    continue;
                }
            }
            return State::BlockDone(reserved.version);
        }
    }

    pub fn consume_entry_retry_new(&self, e: EntryDesc<T>) -> T {
        unsafe {
            let ptr = (*e.block).data_ptr.add(e.offset);
            debug_assert!(!ptr.is_null());
            let data = ptr.read();
            (*e.block).consumed.faa(1);
            data
        }
    }

    pub fn consume_entry_drop_old(&self, e: EntryDesc<T>) -> Option<T> {
        unsafe {
            let ptr = (*e.block).data_ptr.add(e.offset);
            debug_assert!(!ptr.is_null());
            let data = ptr.read();
            let allocated = (*e.block).allocated.load(self.offset_size);
            if allocated.version != e.version {
                return None;
            }
            Some(data)
        }
    }

    pub unsafe fn advance_chead_retry_new(&self, mut ch: Header, _version: u64) -> bool {
        let nblock = self.blocks_ptr.add((ch.index + 1) % self.block_num);

        let committed = (*nblock).committed.load(self.offset_size);

        if committed.version != ch.version + 1 {
            return false;
        }

        (*nblock).consumed.max(
            Cursor {
                version: ch.version + 1,
                offset: 0,
            }
            .into_u64(self.offset_size),
        );
        (*nblock).reserved.max(
            Cursor {
                version: ch.version + 1,
                offset: 0,
            }
            .into_u64(self.offset_size),
        );

        ch.index = (ch.index + 1) % self.block_num;
        if ch.index == 0 {
            ch.version += 1
        }
        self.chead.max(ch.into_u64(self.offset_size));
        true
    }

    pub unsafe fn advance_chead_drop_old(&self, mut ch: Header, version: u64) -> bool {
        let nblock = self.blocks_ptr.add((ch.index + 1) % self.block_num);

        let committed = (*nblock).committed.load(self.offset_size);

        if committed.version < version + if ch.index == 0 { 1 } else { 0 } {
            return false;
        }
        (*nblock).reserved.max(
            Cursor {
                version: committed.version,
                offset: 0,
            }
            .into_u64(self.offset_size),
        );
        ch.index = (ch.index + 1) % self.block_num;
        if ch.index == 0 {
            ch.version += 1
        }
        self.chead.max(ch.into_u64(self.offset_size));
        true
    }
}

impl<T> InnerQueue<T> {
    pub fn is_empty(&self) -> bool {
        let (ch, cb) = self.get_chead_and_block();
        let reserved = unsafe { (*cb).reserved.load(self.offset_size) };

        if reserved.offset < self.block_size {
            let committed = unsafe { (*cb).committed.load(self.offset_size) };
            reserved.offset == committed.offset
        } else {
            let nblock = unsafe { self.blocks_ptr.add((ch.index + 1) % self.block_num) };
            let committed = unsafe { (*nblock).committed.load(self.offset_size) };

            committed.version != ch.version + 1
        }
    }

    pub fn len(&self) -> usize {
        let (ch, cb) = self.get_chead_and_block();
        let (ph, pb) = self.get_phead_and_block();
        let consumed = unsafe { (*cb).consumed.load(self.offset_size) };
        let committed = unsafe { (*pb).committed.load(self.offset_size) };

        if ch.index < ph.index {
            let mut sum = (ph.index - ch.index).saturating_sub(1) * self.block_size;
            sum += self.block_size.saturating_sub(consumed.offset);
            sum += committed.offset;
            sum
        } else if ch.index == ph.index {
            if committed.version > consumed.version {
                // ring back on same block
                let mut sum =
                    (ph.index + (self.block_num - ch.index).saturating_sub(1)) * self.block_size;
                sum += self.block_size.saturating_sub(consumed.offset);
                sum += committed.offset;
                sum
            } else {
                // on same version, not ring back
                if consumed.offset >= self.block_size && committed.offset != consumed.offset {
                    committed.offset
                } else {
                    committed.offset - consumed.offset
                }
            }
        } else {
            // ring back
            let mut sum =
                (ph.index + (self.block_num - ch.index).saturating_sub(1)) * self.block_size;
            sum += self.block_size.saturating_sub(consumed.offset);
            sum += committed.offset;
            sum
        }
    }
}

impl<T> Drop for InnerQueue<T> {
    fn drop(&mut self) {
        unsafe {
            Vec::from_raw_parts(self.blocks_ptr, self.block_num, self.block_num);
        }
    }
}

pub(crate) struct Block<T> {
    pub(crate) allocated: RawCursor,
    pub(crate) committed: RawCursor,

    pub(crate) reserved: RawCursor,
    pub(crate) consumed: RawCursor,

    capacity: usize,
    offset: u32,

    data_ptr: *mut T,

    _marker: PhantomData<T>,
}

impl<T> Block<T> {
    fn new(capacity: usize, offset: u32) -> Self {
        Block {
            allocated: RawCursor::init(capacity),
            committed: RawCursor::init(capacity),
            reserved: RawCursor::init(capacity),
            consumed: RawCursor::init(capacity),
            data_ptr: ManuallyDrop::new(Vec::with_capacity(capacity)).as_mut_ptr(),
            capacity,
            offset,
            _marker: PhantomData::default(),
        }
    }
}

impl<T> Drop for Block<T> {
    fn drop(&mut self) {
        let consumed = version_offset(self.consumed.0.load(Ordering::Relaxed), self.offset);
        let committed = version_offset(self.committed.0.load(Ordering::Relaxed), self.offset);

        // drop all consumed to committed
        for i in consumed.offset..std::cmp::min(committed.offset, self.capacity) {
            unsafe {
                self.data_ptr.add(i).drop_in_place();
            }
        }

        // if block consumed doesn't start, drop all committed
        // here must be version bump on committed
        if consumed.version < committed.version {
            for i in 0..std::cmp::min(committed.offset, self.capacity) {
                unsafe {
                    self.data_ptr.add(i).drop_in_place();
                }
            }
        }

        unsafe {
            Vec::from_raw_parts(self.data_ptr, 0, self.capacity);
        }
    }
}

unsafe impl<T> Send for Block<T> where T: Send {}
unsafe impl<T> Sync for Block<T> where T: Send {}
unsafe impl<T> Send for InnerQueue<T> where T: Send {}
unsafe impl<T> Sync for InnerQueue<T> where T: Send {}

/// offset and version
pub(crate) struct RawCursor(AtomicU64);

impl RawCursor {
    fn init_with_zero() -> Self {
        RawCursor(AtomicU64::new(0))
    }

    fn init(block_size: usize) -> Self {
        RawCursor(AtomicU64::new(block_size as u64))
    }

    pub(crate) fn load(&self, off: u32) -> Cursor {
        let raw = self.0.load(Ordering::Acquire);
        version_offset(raw, off)
    }

    fn faa(&self, num: u64) -> u64 {
        self.0.fetch_add(num, Ordering::AcqRel)
    }

    fn max(&self, cur: u64) -> u64 {
        let mut ret = 0;
        while let Err(_) = self
            .0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                ret = old;
                Some(std::cmp::max(cur, old))
            })
        {}

        ret
    }
}

/// index and version
pub(crate) struct RawHeader(AtomicU64);

impl RawHeader {
    fn init_with_zero() -> Self {
        RawHeader(AtomicU64::new(0))
    }

    fn load(&self, off: u32) -> Header {
        let raw = self.0.load(Ordering::Acquire);
        version_index(raw, off)
    }

    fn max(&self, header: u64) -> u64 {
        let mut ret = 0;
        while let Err(_) = self
            .0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                ret = old;
                Some(std::cmp::max(header, old))
            })
        {}
        ret
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) struct Cursor {
    pub(crate) version: u64,
    pub(crate) offset: usize,
}

impl Cursor {
    #[cfg(test)]
    fn into_raw(self, off: u32) -> RawCursor {
        let raw = self.into_u64(off);

        RawCursor(AtomicU64::new(raw))
    }

    fn into_u64(self, off: u32) -> u64 {
        self.version << off | self.offset as u64
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) struct Header {
    pub(crate) version: u64,
    pub(crate) index: usize,
}

impl Header {
    #[cfg(test)]
    fn into_raw(self, off: u32) -> RawHeader {
        let raw = self.into_u64(off);

        RawHeader(AtomicU64::new(raw))
    }

    fn into_u64(self, off: u32) -> u64 {
        self.version << off | self.index as u64
    }
}

fn version_offset(raw: u64, offset: u32) -> Cursor {
    Cursor {
        version: raw >> offset,
        offset: (raw & !(u64::MAX << offset)) as usize,
    }
}

fn version_index(raw: u64, offset: u32) -> Header {
    Header {
        version: raw >> offset,
        index: (raw & !(u64::MAX << offset)) as usize,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_raw_to_human() {
        let a = Cursor {
            version: 3,
            offset: 0,
        };

        let a_raw = a.into_raw(20);

        assert_eq!(a_raw.load(20), a);

        let b = Header {
            version: 3,
            index: 6,
        };
        let b_raw = b.into_raw(8);

        assert_eq!(b_raw.load(8), b);
    }
}
