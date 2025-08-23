extern crate test;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arc_swap::access::Access;

use crate::file::page_table::local::LocalPageTable;
use crate::layout::page_metadata::PageMetadata;
use crate::{PageGroupId, PageId};

const TABLE_SIZE: usize = 1_300_000; // Estimated max page file size.

#[bench]
fn dense_reads_only(bencher: &mut test::Bencher) {
    let table = sample_table(false);
    bench_reads(table, bencher);
}

#[bench]
fn dense_writes_only(bencher: &mut test::Bencher) {
    let table = sample_table(false);
    bench_writes(table, bencher);
}

#[bench]
fn dense_reads_with_contention(bencher: &mut test::Bencher) {
    let table = sample_table(false);
    bench_reads_with_contention(table, bencher);
}

#[bench]
fn dense_writes_with_contention(bencher: &mut test::Bencher) {
    let table = sample_table(false);
    bench_writes_with_contention(table, bencher);
}

#[bench]
fn sparse_reads_only(bencher: &mut test::Bencher) {
    let table = sample_table(true);
    bench_reads(table, bencher);
}

#[bench]
fn sparse_writes_only(bencher: &mut test::Bencher) {
    let table = sample_table(true);
    bench_writes(table, bencher);
}

#[bench]
fn sparse_reads_with_contention(bencher: &mut test::Bencher) {
    let table = sample_table(true);
    bench_reads_with_contention(table, bencher);
}

#[bench]
fn sparse_writes_with_contention(bencher: &mut test::Bencher) {
    let table = sample_table(true);
    bench_writes_with_contention(table, bencher);
}

fn bench_reads(table: LocalPageTable, bencher: &mut test::Bencher) {
    bencher.iter(|| {
        let page_id = fastrand::u32(0..TABLE_SIZE as u32);
        table.get_page(PageId(page_id))
    })
}

fn bench_writes(table: LocalPageTable, bencher: &mut test::Bencher) {
    bencher.iter(|| {
        let page_id = fastrand::u32(0..TABLE_SIZE as u32);
        let mut page = PageMetadata::empty();
        page.group = PageGroupId(fastrand::u64(1..10_000_000));
        page.id = PageId(page_id);
        table.insert_page(page);
    })
}

fn bench_reads_with_contention(table: LocalPageTable, bencher: &mut test::Bencher) {
    let table = Arc::new(table);
    let signal = Arc::new(AtomicBool::new(true));
    let signal_clone = signal.clone();
    let table_clone = table.clone();

    let handle = std::thread::spawn(move || {
        while signal_clone.load(Ordering::Relaxed) {
            let page_id = fastrand::u32(0..TABLE_SIZE as u32);
            let mut page = PageMetadata::empty();
            page.group = PageGroupId(fastrand::u64(1..10_000_000));
            page.id = PageId(page_id);
            table_clone.insert_page(page);
        }
    });

    bencher.iter(|| {
        let page_id = fastrand::u32(0..TABLE_SIZE as u32);
        table.get_page(PageId(page_id))
    });

    signal.store(false, Ordering::Relaxed);
    let _ = handle.join();
}

fn bench_writes_with_contention(table: LocalPageTable, bencher: &mut test::Bencher) {
    let table = Arc::new(table);
    let signal = Arc::new(AtomicBool::new(true));
    let signal_clone = signal.clone();
    let table_clone = table.clone();

    let handle = std::thread::spawn(move || {
        while signal_clone.load(Ordering::Relaxed) {
            let page_id = fastrand::u32(0..TABLE_SIZE as u32);
            table_clone.get_page(PageId(page_id));
        }
    });

    bencher.iter(|| {
        let page_id = fastrand::u32(0..TABLE_SIZE as u32);
        let mut page = PageMetadata::empty();
        page.group = PageGroupId(fastrand::u64(1..10_000_000));
        page.id = PageId(page_id);
        table.insert_page(page);
    });

    signal.store(false, Ordering::Relaxed);
    let _ = handle.join();
}

fn sample_table(sparse: bool) -> LocalPageTable {
    fastrand::seed(5837566238562);

    let table = LocalPageTable::default();

    for page_id in 0..TABLE_SIZE as u32 {
        let mut page = PageMetadata::empty();
        page.group = PageGroupId(fastrand::u64(1..10_000_000));
        page.id = PageId(page_id);

        if !sparse {
            table.insert_page(page);
        } else if fastrand::f32() < 0.7 {
            table.insert_page(page);
        }
    }

    table
}
