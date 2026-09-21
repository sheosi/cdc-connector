use std::hint::black_box;
use std::str;
use std::{collections::HashMap, ffi::CStr};

use bumpalo::Bump;
use cdc_wal_reader::decoder;
use cdc_wal_reader::decoder::relation::{KeyField, RelationData};
use criterion::{Criterion, criterion_group, criterion_main};
use memchr::memchr;
use simdutf8::basic::from_utf8 as simd_from_utf8;

const INSERT_DATA: [u8; 66] = [
    b'I', 0x00, 0x00, 0x30, 0x39, // OID 12345
    b'N', 0x00, 0x06, b't', 0x00, 0x00, 0x00, 0x01, b'1', b't', 0x00, 0x00, 0x00, 0x01, b'1', b't',
    0x00, 0x00, 0x00, 0x01, b'1', b't', 0x00, 0x00, 0x00, 0x01, b'3', b't', 0x00, 0x00, 0x00, 0x04,
    b'2', b'4', b'9', b'9', b't', 0x00, 0x00, 0x00, 0x14, b'2', b'0', b'2', b'6', b'-', b'0', b'1',
    b'-', b'1', b'5', b'T', b'1', b'0', b':', b'3', b'0', b':', b'0', b'0', b'Z',
];

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
    use cdc_avro::{Field, FieldKind, Relation};
    let arena = Bump::with_capacity(2048);

    let order_items_rel = RelationData {
        inner: Relation {
            relation_oid: 12345,
            replica_id: cdc_avro::ReplicaKind::Keys,
            namespace: "public".to_string(),
            name: "order_items".to_string(),
            fields: bumpalo::vec![in &arena;
                Field {
                    is_key: true,
                    name: "id".to_string(),
                    kind: FieldKind::Int4,
                },
                Field {
                    is_key: false,
                    name: "order_id".to_string(),
                    kind: FieldKind::Int4,
                },
                Field {
                    is_key: false,
                    name: "product_id".to_string(),
                    kind: FieldKind::Int4,
                },
                Field {
                    is_key: false,
                    name: "quantity".to_string(),
                    kind: FieldKind::Int4,
                },
                Field {
                    is_key: false,
                    name: "price_cents".to_string(),
                    kind: FieldKind::Int4,
                },
                Field {
                    is_key: false,
                    name: "created_at".to_string(),
                    kind: FieldKind::Text,
                },
            ],
        },
        key_fields: bumpalo::vec![in &arena; KeyField {
            name: "id".to_string(),
            kind: FieldKind::Int4,
        }],
    };

    let data = bytes::Bytes::copy_from_slice(&INSERT_DATA);
    let mut rel_map = HashMap::default();
    rel_map.insert(12345, order_items_rel);

    // Make sure doesn't return err
    decoder::insert::parse(&data, &rel_map, &arena).unwrap();

    c.bench_function("normal", |b| {
        b.iter(|| {
            black_box(decoder::insert::parse(
                black_box(&data),
                black_box(&rel_map),
                &arena,
            ))
        })
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
