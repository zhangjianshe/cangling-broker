//! Process-wide, bounded-cost timing histograms. All durations are in microseconds.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const BOUNDS: [u64; 10] = [
    100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 10_000_000,
];

pub struct Timing {
    count: AtomicU64,
    total: AtomicU64,
    max: AtomicU64,
    buckets: [AtomicU64; 11],
}

impl Timing {
    const fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            total: AtomicU64::new(0),
            max: AtomicU64::new(0),
            buckets: [const { AtomicU64::new(0) }; 11],
        }
    }

    pub fn record(&self, duration: Duration) {
        let us = duration.as_micros().min(u64::MAX as u128) as u64;
        self.total.fetch_add(us, Ordering::Relaxed);
        self.max.fetch_max(us, Ordering::Relaxed);
        let bucket = BOUNDS.iter().position(|bound| us <= *bound).unwrap_or(10);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> TimingSnapshot {
        TimingSnapshot {
            count: self.count.load(Ordering::Relaxed),
            total_us: self.total.load(Ordering::Relaxed),
            max_us: self.max.load(Ordering::Relaxed),
            bucket_upper_us: BOUNDS,
            buckets: self.buckets.each_ref().map(|b| b.load(Ordering::Relaxed)),
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct TimingSnapshot {
    count: u64,
    total_us: u64,
    max_us: u64,
    bucket_upper_us: [u64; 10],
    // Exclusive buckets; the last bucket is above the largest upper bound.
    buckets: [u64; 11],
}

pub static ADMISSION: Timing = Timing::new();
pub static WRITE_QUEUE: Timing = Timing::new();
pub static PERSIST: Timing = Timing::new();
pub static CLAIM: Timing = Timing::new();
pub static QUEUE_WAIT: Timing = Timing::new();
pub static SEND: Timing = Timing::new();
pub static ACK_WAIT: Timing = Timing::new();
pub static ACK_COMMIT: Timing = Timing::new();

#[derive(Debug, serde::Serialize)]
pub struct Performance {
    admission: TimingSnapshot,
    write_queue: TimingSnapshot,
    persist_batch: TimingSnapshot,
    claim_batch: TimingSnapshot,
    queue_wait: TimingSnapshot,
    send: TimingSnapshot,
    ack_wait: TimingSnapshot,
    ack_commit_batch: TimingSnapshot,
}

pub fn snapshot() -> Performance {
    Performance {
        admission: ADMISSION.snapshot(),
        write_queue: WRITE_QUEUE.snapshot(),
        persist_batch: PERSIST.snapshot(),
        claim_batch: CLAIM.snapshot(),
        queue_wait: QUEUE_WAIT.snapshot(),
        send: SEND.snapshot(),
        ack_wait: ACK_WAIT.snapshot(),
        ack_commit_batch: ACK_COMMIT.snapshot(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn histogram_records_boundaries_and_overflow() {
        let timing = Timing::new();
        for us in [0, 100, 101, 10_000_001] {
            timing.record(Duration::from_micros(us));
        }
        let snapshot = timing.snapshot();
        assert_eq!(snapshot.count, 4);
        assert_eq!(snapshot.total_us, 10_000_202);
        assert_eq!(snapshot.max_us, 10_000_001);
        assert_eq!(snapshot.buckets[0], 2);
        assert_eq!(snapshot.buckets[1], 1);
        assert_eq!(snapshot.buckets[10], 1);
    }
}
