use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main};
use criterion::{AxisScale, PlotConfiguration};

use bbq::BQueue;
use crossbeam_queue::{ArrayQueue, SegQueue};
use rtrb::RingBuffer;

pub const CAPACITY: usize = 10_000;

pub fn add_function<P, C, Create, Push, Pop, M>(
    group: &mut criterion::BenchmarkGroup<M>,
    id: impl Into<String>,
    create: Create,
    push: Push,
    pop: Pop,
) where
    P: Send + 'static,
    C: Send + 'static,
    Create: Fn() -> (P, C) + Copy,
    Push: Fn(&mut P, u8) -> bool + Send + Copy + 'static,
    Pop: Fn(&mut C) -> Option<u8> + Send + Copy + 'static,
    M: criterion::measurement::Measurement,
{
    let mut overflows = 0;

    let id = id.into();

    group.bench_function(id.clone() + "push", move |b| {
        let (mut p, mut c) = create();

        let keep_thread_running = Arc::new(AtomicBool::new(true));
        let keep_running = Arc::clone(&keep_thread_running);

        let pop_thread = std::thread::spawn(move || {
            let mut i = 0;
            while keep_running.load(Ordering::Acquire) {
                while let Some(x) = pop(&mut c) {
                    debug_assert_eq!(x, i);
                    i = i.wrapping_add(1);
                }
            }
        });

        let mut j = 0;
        b.iter(|| {
            if push(&mut p, black_box(j)) {
                j = j.wrapping_add(1);
            } else {
                overflows += 1;
                std::thread::yield_now();
            }
        });

        keep_thread_running.store(false, Ordering::Release);
        pop_thread.join().unwrap();
    });
    println!("queue was full {} time(s)", overflows);

    let mut underflows = 0;
    group.bench_function(id + "pop", |b| {
        let (mut p, mut c) = create();

        let mut i = 0;

        while push(&mut p, i) {
            i = i.wrapping_add(1);
        }

        let keep_thread_running = Arc::new(AtomicBool::new(true));
        let keep_running = Arc::clone(&keep_thread_running);

        let push_thread = std::thread::spawn(move || {
            while keep_running.load(Ordering::Acquire) {
                while push(&mut p, i) {
                    i = i.wrapping_add(1);
                }
            }
        });

        let mut j = 0;

        b.iter(|| {
            if let Some(x) = black_box(pop(&mut c)) {
                debug_assert_eq!(x, j);
                j = j.wrapping_add(1);
            } else {
                underflows += 1;
                std::thread::yield_now();
            }
        });
        keep_thread_running.store(false, Ordering::Release);
        push_thread.join().unwrap();
    });
    println!("queue was empty {} time(s)", underflows);
}

fn criterion_benchmark(criterion: &mut criterion::Criterion) {
    let mut group = criterion.benchmark_group("two-threads-single-byte");
    group.throughput(criterion::Throughput::Bytes(1));
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    add_function(
        &mut group,
        "rtrb-",
        || RingBuffer::<u8>::new(CAPACITY),
        |p, i| p.push(i).is_ok(),
        |c| c.pop().ok(),
    );

    add_function(
        &mut group,
        "bbq-",
        || {
            let tx = BQueue::new(8, CAPACITY / 8 + 1);
            let rx = tx.clone();
            (tx, rx)
        },
        |p, i| p.push(i).is_ok(),
        |c| c.pop().ok(),
    );

    add_function(
        &mut group,
        "arrayqueue-",
        || {
            let tx = Arc::new(ArrayQueue::new(CAPACITY));
            let rx = tx.clone();
            (tx, rx)
        },
        |p, i| p.push(i).is_ok(),
        |c| c.pop(),
    );

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
