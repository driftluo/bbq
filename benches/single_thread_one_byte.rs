use criterion::{black_box, criterion_group, criterion_main};
use criterion::{AxisScale, PlotConfiguration};

use bbq::BQueue;
use crossbeam_queue::{ArrayQueue, SegQueue};
use rtrb::RingBuffer;
use std::sync::Arc;

pub fn add_function<F, M>(group: &mut criterion::BenchmarkGroup<M>, id: impl Into<String>, mut f: F)
where
    F: FnMut(u8) -> u8,
    M: criterion::measurement::Measurement,
{
    group.bench_function(id, |b| {
        let mut i = 0;
        b.iter(|| {
            assert_eq!(f(black_box(i)), black_box(i));
            i = i.wrapping_add(1);
        })
    });
}

pub fn criterion_benchmark(criterion: &mut criterion::Criterion) {
    let mut group = criterion.benchmark_group("single-thread-single-byte");
    group.throughput(criterion::Throughput::Bytes(1));
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    let mut v = Vec::<u8>::with_capacity(1);
    add_function(&mut group, "0-vec", |i| {
        v.push(i);
        v.pop().unwrap()
    });

    let (tx, rx) = {
        let tx = BQueue::new(10, 6);
        let rx = tx.clone();
        (tx, rx)
    };

    add_function(&mut group, "1-push-pop", |i| {
        tx.push(i).unwrap();
        rx.pop().unwrap()
    });

    let (tx, rx) = {
        let tx = Arc::new(ArrayQueue::new(1));
        let rx = tx.clone();
        (tx, rx)
    };

    add_function(&mut group, "1-push-pop-arrayqueue", |i| {
        tx.push(i).unwrap();
        rx.pop().unwrap()
    });

    let (tx, rx) = {
        let tx = Arc::new(SegQueue::new());
        let rx = tx.clone();
        (tx, rx)
    };

    add_function(&mut group, "1-push-pop-Segqueue", |i| {
        tx.push(i);
        rx.pop().unwrap()
    });

    let (mut p, mut c) = RingBuffer::<u8>::new(1);

    add_function(&mut group, "1-push-pop-rtrb", |i| {
        p.push(i).unwrap();
        c.pop().unwrap()
    });

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
