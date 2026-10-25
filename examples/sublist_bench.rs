//! Repeatable sublist matching harness (std::time only, no new deps).
//!
//! Builds 10,000 subscriptions — 2,500 exact, 2,500 `*`, 2,500 `>`,
//! 2,500 queue-group members on one subject — then times
//! `match_recipients` and `match_groups` over rotating matching subjects.
//! Run: `cargo run --release --example sublist_bench`
//! The delivery checksum is printed so the loop cannot be optimised away.
use rust_natsd::sublist::{Sublist, Subscription};
use std::time::Instant;

fn sub(conn: u64, sid: &str, subject: &str, queue: Option<&str>) -> Subscription {
    Subscription::new(
        conn,
        sid.into(),
        subject.into(),
        queue.map(|q| q.into()),
        "$G".into(),
    )
}

fn build() -> Sublist {
    let mut sl = Sublist::new();
    for i in 0..2500u64 {
        sl.add(sub(i, "1", &format!("svc.{i}.req"), None));
    }
    for i in 0..2500u64 {
        sl.add(sub(10_000 + i, "1", &format!("a.{i}.*"), None));
    }
    for i in 0..2500u64 {
        sl.add(sub(20_000 + i, "1", &format!("b.{i}.>"), None));
    }
    for i in 0..2500u64 {
        sl.add(sub(30_000 + i, "1", "jobs", Some("workers")));
    }
    assert_eq!(sl.len(), 10_000);
    sl
}

fn main() {
    let mut sl = build();
    let subjects = ["svc.1234.req", "a.1234.x", "b.1234.deep.path", "jobs"];
    // Warm-up.
    for s in subjects {
        let _ = sl.match_recipients(s, 999_999, true, None);
    }
    let iters = 20_000usize;
    let start = Instant::now();
    let mut checksum = 0usize;
    for i in 0..iters {
        let r = sl.match_recipients(subjects[i % subjects.len()], 999_999, true, None);
        checksum += r.deliveries.len();
    }
    let elapsed = start.elapsed();
    println!(
        "match_recipients: {iters} iters in {elapsed:?} ({:.3} us/iter, checksum {checksum})",
        elapsed.as_secs_f64() * 1e6 / iters as f64
    );

    let start = Instant::now();
    let mut checksum = 0usize;
    for i in 0..iters {
        let g = sl.match_groups(subjects[i % subjects.len()], 999_999, true, None);
        checksum += g.plain.len() + g.groups.values().map(|v| v.len()).sum::<usize>();
    }
    let elapsed = start.elapsed();
    println!(
        "match_groups:     {iters} iters in {elapsed:?} ({:.3} us/iter, checksum {checksum})",
        elapsed.as_secs_f64() * 1e6 / iters as f64
    );
    // Sparse subjects only (no 2,500-member queue fan-out): this is the
    // phase where a trie beats a linear scan, isolated from the
    // unavoidable per-member clone cost of the "jobs" group.
    let sparse = &subjects[..3];
    let start = Instant::now();
    let mut checksum = 0usize;
    for i in 0..iters {
        let r = sl.match_recipients(sparse[i % sparse.len()], 999_999, true, None);
        checksum += r.deliveries.len();
    }
    let elapsed = start.elapsed();
    println!("match_recipients (sparse): {iters} iters in {elapsed:?} ({:.3} us/iter, checksum {checksum})", elapsed.as_secs_f64() * 1e6 / iters as f64);
    let start = Instant::now();
    let mut checksum = 0usize;
    for i in 0..iters {
        let g = sl.match_groups(sparse[i % sparse.len()], 999_999, true, None);
        checksum += g.plain.len() + g.groups.values().map(|v| v.len()).sum::<usize>();
    }
    let elapsed = start.elapsed();
    println!("match_groups (sparse):     {iters} iters in {elapsed:?} ({:.3} us/iter, checksum {checksum})", elapsed.as_secs_f64() * 1e6 / iters as f64);
}
