use std::arch::x86_64::{
    __m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_setzero_si128,
};
use std::hint::black_box;
use std::str;
use std::{cmp::min, ffi::CStr};

use criterion::{Criterion, criterion_group, criterion_main};
use memchr::memchr;

/*
 * According to those benchmarks, in the machine I tested, in 16 bytes the scalar
 * wins while from 32 bytes onwards the simd version wins (the simd price is amortized)
 */
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
        let word = u64::from_ne_bytes(data[i..i + 8].try_into().expect(""));
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

fn unaligned_u64(data: &[u8]) -> Option<usize> {
    let len = data.len();

    // Small-to-medium: unaligned u64 word scan (no alignment loop)
    let mut i = 0;
    while i + 8 <= len {
        let word = u64::from_ne_bytes(data[i..i + 8].try_into().unwrap());
        let mask = word.wrapping_sub(0x0101010101010101) & !word & 0x8080808080808080;

        if mask != 0 {
            return Some(i + (mask.trailing_zeros() / 8) as usize);
        }
        i += 8;
    }

    // Tail
    while i < len {
        if data[i] == 0 {
            return Some(i);
        }
        i += 1;
    }

    None
}

fn unaligned_u64_sse(data: &[u8]) -> Option<usize> {
    let len = data.len();

    // Fast path: strings shorter than 16 bytes
    if len < 16 {
        for i in 0..len {
            if data[i] == 0 {
                return Some(i);
            }
        }
        return None;
    }

    unsafe {
        let ptr = data.as_ptr() as *const __m128i;
        let chunk = _mm_loadu_si128(ptr); // movdqu

        let zeros = _mm_setzero_si128();
        let cmp = _mm_cmpeq_epi8(chunk, zeros); // 0xFF where byte == 0
        let mask = _mm_movemask_epi8(cmp); // 16-bit mask

        if mask != 0 {
            return Some(mask.trailing_zeros() as usize);
        }
    }

    // General case: strings longer than 16 bytes
    let mut i = 24; // This i is the last_position
    while i <= len + 7 {
        let boundary = min(i, len);
        let word = u64::from_ne_bytes(data[boundary - 8..boundary].try_into().unwrap());
        let mask = word.wrapping_sub(0x0101010101010101) & !word & 0x8080808080808080;

        if mask != 0 {
            return Some((boundary - 8) + (mask.trailing_zeros() / 8) as usize);
        }
        i += 8;
    }

    None
}

fn unaligned_u64_v2(data: &[u8]) -> Option<usize> {
    let len = data.len();

    // Fast path: strings shorter than 8 bytes
    if len < 8 {
        for i in 0..len {
            if data[i] == 0 {
                return Some(i);
            }
        }
        return None;
    }

    // Fast path: strings 8–16 bytes, two overlapping u64 reads
    let word1 = u64::from_ne_bytes(data[0..8].try_into().unwrap());
    let mask1 = word1.wrapping_sub(0x0101010101010101) & !word1 & 0x8080808080808080;
    if mask1 != 0 {
        return Some((mask1.trailing_zeros() / 8) as usize);
    }

    let boundary = min(len, 16);
    let word2 = u64::from_ne_bytes(data[boundary - 8..boundary].try_into().unwrap());
    let mask2 = word2.wrapping_sub(0x0101010101010101) & !word2 & 0x8080808080808080;
    if mask2 != 0 {
        return Some(boundary - 8 + (mask2.trailing_zeros() / 8) as usize);
    }

    // General case: strings longer than 16 bytes
    let mut i = 24; // This i is the last_position
    while i <= len + 7 {
        let word = u64::from_ne_bytes(data[i - 8..i].try_into().unwrap());
        let mask = word.wrapping_sub(0x0101010101010101) & !word & 0x8080808080808080;

        if mask != 0 {
            return Some((i - 8) + (mask.trailing_zeros() / 8) as usize);
        }
        i += 8;
    }

    None
}

fn unaligned_u32(data: &[u8]) -> Option<usize> {
    let len = data.len();

    // Small-to-medium: unaligned u32 word scans
    let mut i = 0;
    while i + 4 <= len {
        let word = u32::from_ne_bytes(data[i..i + 4].try_into().unwrap());
        let mask = word.wrapping_sub(0x01010101) & !word & 0x80808080;

        if mask != 0 {
            return Some(i + (mask.trailing_zeros() / 8) as usize);
        }

        i += 4;
    }

    // Tail
    while i < len {
        if data[i] == 0 {
            return Some(i);
        }
        i += 1;
    }

    None
}

fn linear(data: &[u8]) -> Option<usize> {
    let len = data.len();

    // Fast path: very small strings

    for i in 0..len {
        if data[i] == 0 {
            return Some(i);
        }
    }
    return None;
}

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

        group.finish();
    }
}

fn string_len(c: &mut Criterion) {
    for len in [10, 12, 15] {
        let mut data = vec![b'a'; len + 120];
        data[len] = 0;

        assert_eq!(unaligned_u64(&data), Some(len));
        assert_eq!(unaligned_u64_v2(&data), Some(len));
        assert_eq!(unaligned_u64_sse(&data), Some(len));

        let mut group = c.benchmark_group(format!("utf8_len_{}", len));

        //group.bench_function("custom", |b| b.iter(|| find_null_word(&data)));

        //group.bench_function("memchr", |b| b.iter(|| memchr::memchr(0, &data)));
        group.bench_function("unaligned_u64", |b| b.iter(|| unaligned_u64(&data)));
        group.bench_function("unaligned_u64_v2", |b| b.iter(|| unaligned_u64_v2(&data)));
        group.bench_function("unaligned_u64_sse", |b| b.iter(|| unaligned_u64_sse(&data)));
        /*group.bench_function("glibc", |b| {
            b.iter(|| unsafe { strlen_glibc(data.as_ptr()) })
        });*/
        //group.bench_function("unaligned_u32", |b| b.iter(|| unaligned_u32(&data)));
        //group.bench_function("linear", |b| b.iter(|| linear(&data)));
    }
}

//criterion_group!(benches, bench);
criterion_group!(benches, string_len);
criterion_main!(benches);
