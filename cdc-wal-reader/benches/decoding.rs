use std::ffi::CStr;
use std::hint::black_box;
use std::str;

use criterion::{Criterion, criterion_group, criterion_main};
use memchr::memchr;
use simdutf8::basic::from_utf8 as simd_from_utf8;

/*
 * According to those benchmarks, in the machine I tested, in 16 bytes the scalar
 * wins while from 32 bytes onwards the simd version wins (the simd price is amortized)
 */

fn parse_cstr(data: &[u8]) -> &str {
    CStr::from_bytes_until_nul(data).unwrap().to_str().unwrap()
}

fn parse_memchr_std(data: &[u8]) -> &str {
    let nul = memchr(0, data).unwrap();
    str::from_utf8(&data[..nul]).unwrap()
}

fn bench(c: &mut Criterion) {
    for len in [8, 16, 32, 64, 128] {
        let mut data = vec![b'a'; len];
        data.push(0);

        let mut group = c.benchmark_group(format!("utf8_len_{}", len));

        group.bench_function("cstr", |b| b.iter(|| parse_cstr(black_box(&data))));

        group.bench_function("memchr_std", |b| {
            b.iter(|| parse_memchr_std(black_box(&data)))
        });

        group.bench_function("memchr_simd", |b| {
            b.iter(|| parse_memchr_simd(black_box(&data)))
        });

        group.bench_function("scalar_simd", |b| {
            b.iter(|| parse_scalar_simd(black_box(&data)))
        });

        group.finish();
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
