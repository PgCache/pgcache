use std::collections::VecDeque;
use std::io::IoSlice;

use tokio_util::bytes::{Buf, Bytes};

/// Number of `Bytes` chunks held inline before spilling to the heap. A typical
/// cache-hit response enqueues ~4–6 chunks (ParseComplete, BindComplete,
/// [ParameterDescription], RowDescription, one or two ≤64 KiB `DataRows` frames,
/// CommandComplete), and the codec coalesces consecutive `DataRow`s into one
/// frame — so 8 covers the common case with headroom while keeping the inline
/// array (and thus the serve future it lives in) small.
const INLINE_CAP: usize = 8;

/// Queue of `Bytes` chunks for zero-copy vectored writes.
///
/// Instead of copying frame data into a contiguous write buffer, push zero-copy
/// `BytesMut` splits directly. When the writer supports vectored I/O
/// (`is_write_vectored`), `tokio::io::write_buf` uses `chunks_vectored` to issue
/// a single `writev` syscall across all queued chunks.
///
/// Accepts both `BytesMut` (frame data from the codec) and `Bytes` (e.g.
/// `Bytes::from_static` for fixed protocol messages).
///
/// Storage is an inline ring buffer with a heap spill: up to [`INLINE_CAP`]
/// *live* (undrained) chunks live in the inline array with no allocation; the
/// `spill` `VecDeque` is only allocated if a serve exceeds that many live chunks
/// at once (large result drained by a slow client). It's a ring — not a Vec with
/// a read cursor — so slots are reused as chunks are advanced out the front,
/// bounding the inline capacity by live chunks rather than total pushes.
pub struct WriteQueue {
    /// Ring of live chunks: FIFO order is `inline[head], inline[(head+1) % CAP],
    /// …` for `len` elements.
    inline: [Bytes; INLINE_CAP],
    /// Index of the front (next-to-drain) chunk in `inline`.
    head: usize,
    /// Number of live chunks in `inline`.
    len: usize,
    /// Overflow once `inline` fills. Once non-empty it takes all new pushes
    /// (even after `inline` frees slots) to preserve FIFO order.
    spill: VecDeque<Bytes>,
}

impl WriteQueue {
    /// Create an empty queue. Performs no heap allocation — the inline ring holds
    /// the first [`INLINE_CAP`] live chunks, and the spill `VecDeque` stays at
    /// capacity 0 until a push overflows the ring.
    pub fn new() -> Self {
        Self {
            inline: [const { Bytes::new() }; INLINE_CAP],
            head: 0,
            len: 0,
            spill: VecDeque::new(),
        }
    }

    /// Push a chunk onto the back of the queue. Empty chunks are silently ignored.
    ///
    /// Accepts `BytesMut` (zero-cost freeze) or `Bytes` (e.g. `from_static`).
    pub fn push(&mut self, buf: impl Into<Bytes>) {
        let buf = buf.into();
        if !buf.has_remaining() {
            return;
        }
        // Inline only while the ring has room and nothing has spilled yet;
        // otherwise everything trails into `spill` to keep FIFO order.
        if self.spill.is_empty() && self.len < INLINE_CAP {
            let idx = (self.head + self.len) % INLINE_CAP;
            if let Some(slot) = self.inline.get_mut(idx) {
                *slot = buf;
                self.len += 1;
                return;
            }
        }
        self.spill.push_back(buf);
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0 && self.spill.is_empty()
    }

    /// Drop all queued chunks (used when the client is gone and the remaining
    /// cache-DB response is being drained rather than relayed).
    pub fn clear(&mut self) {
        for slot in &mut self.inline {
            *slot = Bytes::new();
        }
        self.head = 0;
        self.len = 0;
        self.spill.clear();
    }

    /// Mutable reference to the front (next-to-drain) chunk, if any.
    fn front_mut(&mut self) -> Option<&mut Bytes> {
        if self.len > 0 {
            self.inline.get_mut(self.head)
        } else {
            self.spill.front_mut()
        }
    }

    /// Drop the fully-consumed front chunk, advancing to the next.
    fn pop_front(&mut self) {
        if self.len > 0 {
            if let Some(slot) = self.inline.get_mut(self.head) {
                *slot = Bytes::new(); // release the consumed chunk
            }
            self.head = (self.head + 1) % INLINE_CAP;
            self.len -= 1;
        } else {
            self.spill.pop_front();
        }
    }
}

impl Buf for WriteQueue {
    fn remaining(&self) -> usize {
        let inline: usize = (0..self.len)
            .filter_map(|i| self.inline.get((self.head + i) % INLINE_CAP))
            .map(Buf::remaining)
            .sum();
        inline + self.spill.iter().map(Buf::remaining).sum::<usize>()
    }

    fn chunk(&self) -> &[u8] {
        if self.len > 0 {
            self.inline.get(self.head).map_or(&[], Buf::chunk)
        } else {
            self.spill.front().map_or(&[], Buf::chunk)
        }
    }

    fn advance(&mut self, mut cnt: usize) {
        while cnt > 0 {
            let front = self.front_mut().expect("advance within queued bytes");
            let n = cnt.min(front.remaining());
            front.advance(n);
            cnt -= n;
            if !front.has_remaining() {
                self.pop_front();
            }
        }
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        let mut filled = 0;
        for i in 0..self.len {
            let (Some(b), Some(slot)) =
                (self.inline.get((self.head + i) % INLINE_CAP), dst.get_mut(filled))
            else {
                return filled;
            };
            *slot = IoSlice::new(b.chunk());
            filled += 1;
        }
        for b in &self.spill {
            let Some(slot) = dst.get_mut(filled) else {
                return filled;
            };
            *slot = IoSlice::new(b.chunk());
            filled += 1;
        }
        filled
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::cast_possible_truncation)] // small loop indices fit u8 by construction

    use tokio_util::bytes::BytesMut;

    use super::*;

    #[test]
    fn empty_queue() {
        let q = WriteQueue::new();
        assert!(q.is_empty());
        assert_eq!(q.remaining(), 0);
        assert!(q.chunk().is_empty());
    }

    #[test]
    fn push_bytes_mut() {
        let mut q = WriteQueue::new();
        q.push(BytesMut::from(&b"hello"[..]));
        q.push(BytesMut::from(&b"world"[..]));
        assert!(!q.is_empty());
        assert_eq!(q.remaining(), 10);
        assert_eq!(q.chunk(), b"hello");
    }

    #[test]
    fn push_static() {
        let mut q = WriteQueue::new();
        q.push(Bytes::from_static(b"static"));
        assert_eq!(q.remaining(), 6);
        assert_eq!(q.chunk(), b"static");
    }

    #[test]
    fn push_empty_ignored() {
        let mut q = WriteQueue::new();
        q.push(BytesMut::new());
        q.push(Bytes::new());
        assert!(q.is_empty());
    }

    #[test]
    fn advance_within_chunk() {
        let mut q = WriteQueue::new();
        q.push(BytesMut::from(&b"hello"[..]));
        q.advance(3);
        assert_eq!(q.remaining(), 2);
        assert_eq!(q.chunk(), b"lo");
    }

    #[test]
    fn advance_across_chunks() {
        let mut q = WriteQueue::new();
        q.push(BytesMut::from(&b"ab"[..]));
        q.push(Bytes::from_static(b"cde"));
        q.advance(3); // consumes "ab" + "c"
        assert_eq!(q.remaining(), 2);
        assert_eq!(q.chunk(), b"de");
    }

    #[test]
    fn advance_drains_all() {
        let mut q = WriteQueue::new();
        q.push(BytesMut::from(&b"abc"[..]));
        q.advance(3);
        assert!(q.is_empty());
        assert_eq!(q.remaining(), 0);
    }

    #[test]
    fn chunks_vectored_fills_dst() {
        let mut q = WriteQueue::new();
        q.push(BytesMut::from(&b"aa"[..]));
        q.push(Bytes::from_static(b"bb"));
        q.push(BytesMut::from(&b"cc"[..]));

        let mut slices = [IoSlice::new(&[]); 4];
        let n = q.chunks_vectored(&mut slices);
        assert_eq!(n, 3);
        assert_eq!(&*slices[0], b"aa");
        assert_eq!(&*slices[1], b"bb");
        assert_eq!(&*slices[2], b"cc");
    }

    #[test]
    fn chunks_vectored_limited_by_dst_len() {
        let mut q = WriteQueue::new();
        q.push(BytesMut::from(&b"aa"[..]));
        q.push(BytesMut::from(&b"bb"[..]));
        q.push(BytesMut::from(&b"cc"[..]));

        let mut slices = [IoSlice::new(&[]); 2];
        let n = q.chunks_vectored(&mut slices);
        assert_eq!(n, 2);
        assert_eq!(&*slices[0], b"aa");
        assert_eq!(&*slices[1], b"bb");
    }

    /// Pushing past INLINE_CAP spills to the heap while preserving FIFO order.
    #[test]
    fn spills_past_inline_cap_in_order() {
        let mut q = WriteQueue::new();
        let total = INLINE_CAP + 3;
        for i in 0..total {
            q.push(Bytes::from(vec![i as u8]));
        }
        assert_eq!(q.remaining(), total);
        // Drain one byte at a time; bytes must come back 0,1,2,...
        for i in 0..total {
            assert_eq!(q.chunk(), &[i as u8][..], "chunk {i}");
            q.advance(1);
        }
        assert!(q.is_empty());
    }

    /// The ring reuses slots: interleaving drains with pushes keeps total live
    /// chunks low, so a long stream never spills as long as it's drained.
    #[test]
    fn ring_reuses_slots_without_spilling() {
        let mut q = WriteQueue::new();
        // Push/drain far more than INLINE_CAP total, but never more than 2 live.
        for i in 0..(INLINE_CAP * 4) {
            q.push(Bytes::from(vec![i as u8]));
            assert_eq!(q.chunk(), &[i as u8][..]);
            q.advance(1);
            assert!(q.is_empty());
        }
        assert!(q.spill.is_empty(), "draining stream must not allocate spill");
    }

    /// Once anything has spilled, new pushes keep going to the spill even after
    /// the inline ring frees slots — otherwise a newer chunk could jump ahead of
    /// older spilled ones and break FIFO order.
    #[test]
    fn spill_is_sticky_until_drained() {
        let mut q = WriteQueue::new();
        for i in 0..INLINE_CAP {
            q.push(Bytes::from(vec![i as u8]));
        }
        q.push(Bytes::from(vec![100])); // overflow → spill
        assert_eq!(q.spill.len(), 1);

        // Free an inline slot, then push again: must still land in spill.
        q.advance(1); // drains inline front (byte 0)
        q.push(Bytes::from(vec![101]));
        assert_eq!(q.spill.len(), 2, "push must trail into spill, not the freed slot");

        // Remaining bytes drain in strict FIFO: 1..INLINE_CAP, then 100, 101.
        let mut got = Vec::new();
        while !q.is_empty() {
            got.push(q.chunk()[0]);
            q.advance(1);
        }
        let mut expected: Vec<u8> = (1..INLINE_CAP as u8).collect();
        expected.push(100);
        expected.push(101);
        assert_eq!(got, expected);
    }

    /// `remaining()` sums variable-length chunks across the inline/spill boundary.
    #[test]
    fn remaining_counts_inline_and_spill() {
        let mut q = WriteQueue::new();
        let mut expected = 0;
        for i in 0..(INLINE_CAP + 2) {
            let len = i + 1; // 1,2,3,... bytes per chunk
            q.push(Bytes::from(vec![0u8; len]));
            expected += len;
        }
        assert_eq!(q.remaining(), expected);
        // Partially consume the front chunk; remaining shrinks by exactly that.
        q.advance(1);
        assert_eq!(q.remaining(), expected - 1);
    }

    /// vectored chunks span the inline ring and the spill in FIFO order.
    #[test]
    fn chunks_vectored_spans_inline_and_spill() {
        let mut q = WriteQueue::new();
        let total = INLINE_CAP + 2;
        for i in 0..total {
            q.push(Bytes::from(vec![i as u8]));
        }
        let mut slices = [IoSlice::new(&[]); INLINE_CAP + 4];
        let n = q.chunks_vectored(&mut slices);
        assert_eq!(n, total);
        for (i, slot) in slices.iter().take(total).enumerate() {
            assert_eq!(&**slot, &[i as u8][..], "slice {i}");
        }
    }

    /// After the ring head wraps, vectored order is still FIFO.
    #[test]
    fn chunks_vectored_after_wraparound() {
        let mut q = WriteQueue::new();
        // Fill, drain half so head moves forward, then refill into the freed slots.
        for i in 0..INLINE_CAP {
            q.push(Bytes::from(vec![i as u8]));
        }
        for _ in 0..(INLINE_CAP / 2) {
            q.advance(1);
        }
        for i in INLINE_CAP..(INLINE_CAP + INLINE_CAP / 2) {
            q.push(Bytes::from(vec![i as u8]));
        }
        // Live chunks now: (INLINE_CAP/2)..(INLINE_CAP + INLINE_CAP/2)
        let mut slices = [IoSlice::new(&[]); INLINE_CAP];
        let n = q.chunks_vectored(&mut slices);
        assert_eq!(n, INLINE_CAP);
        for (j, slot) in slices.iter().take(n).enumerate() {
            let expected = (INLINE_CAP / 2 + j) as u8;
            assert_eq!(&**slot, &[expected][..], "slice {j}");
        }
        assert!(q.spill.is_empty(), "wraparound must reuse inline slots");
    }
}
