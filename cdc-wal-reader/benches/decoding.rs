use std::ffi::CStr;
use std::hint::black_box;
use std::str;

use criterion::{Criterion, criterion_group, criterion_main};
use memchr::memchr;
use simdutf8::basic::from_utf8 as simd_from_utf8;

/*
 * According to those benchmarks, in the machine I tested, in 16bytes the scalar
 * wins while from 32bytes onwards the simd version wins (the simd price is amortized)
 */

fn parse_cstr(data: &[u8]) -> &str {
    CStr::from_bytes_until_nul(data).unwrap().to_str().unwrap()
}

fn parse_memchr_std(data: &[u8]) -> &str {
    let nul = memchr(0, data).unwrap();
    str::from_utf8(&data[..nul]).unwrap()
}

fn parse_memchr_simd(data: &[u8]) -> &str {
    let nul = memchr(0, data).unwrap();
    simd_from_utf8(&data[..nul]).unwrap()
}

fn find_null_word(data: &[u8]) -> Option<usize> {
    let len = data.len();
    let mut i = 0;

    // head: scan until aligned
    while i < len && i % 8 != 0 {
        if data[i] == 0 {
            return Some(i);
        }
        i += 1;
    }

    // body: 8 bytes at a time
    while i + 8 <= len {
        let word = u64::from_ne_bytes(data[i..i + 8].try_into().unwrap());
        // has-zero-byte algorithm
        let mask = word.wrapping_sub(0x0101010101010101) & !word & 0x8080808080808080;
        if mask != 0 {
            let idx = i + (mask.trailing_zeros() / 8) as usize;
            return Some(idx);
        }
        i += 8;
    }

    // tail
    while i < len {
        if data[i] == 0 {
            return Some(i);
        }
        i += 1;
    }

    None
}

fn parse_scalar_simd(data: &[u8]) -> &str {
    let nul = find_null_word(data).unwrap();
    simd_from_utf8(&data[..nul]).unwrap()
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
