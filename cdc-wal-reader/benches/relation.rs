use std::hint::black_box;

use cdc_wal_reader::decoder::{self, relation::RelationData};
use criterion::{Criterion, criterion_group, criterion_main};

const RELATION_DATA: bytes::Bytes = bytes::Bytes::from_static(&[
    b'R', // Relation message
    0, 0, 40, 1, // Relation OID: 16385
    b'p', b'u', b'b', b'l', b'i', b'c', 0, // Namespace: public
    b'o', b'r', b'd', b'e', b'r', b's', 0, // Relation name: orders
    2, // Replica identity: 2 = Row/Full
    0, 10, // Number of columns: 10
    // Column 1: order_id (key, Int4)
    1, b'o', b'r', b'd', b'e', b'r', b'_', b'i', b'd', 0, 0, 0, 0, 23, 255, 255, 255, 255,
    // Column 2: customer_id (key, Int4)
    1, b'c', b'u', b's', b't', b'o', b'm', b'e', b'r', b'_', b'i', b'd', 0, 0, 0, 0, 23, 255, 255,
    255, 255, // Column 3: status (Int4/Text? set to Text below)
    0, b's', b't', b'a', b't', b'u', b's', 0, 0, 0, 0, 25, 255, 255, 255, 255,
    // Column 4: total_cents (Int4)
    0, b't', b'o', b't', b'a', b'l', b'_', b'c', b'e', b'n', b't', b's', 0, 0, 0, 0, 23, 255, 255,
    255, 255, // Column 5: currency (Text)
    0, b'c', b'u', b'r', b'r', b'e', b'n', b'c', b'y', 0, 0, 0, 0, 25, 255, 255, 255, 255,
    // Column 6: shipping_address (Text)
    0, b's', b'h', b'i', b'p', b'p', b'i', b'n', b'g', b'_', b'a', b'd', b'd', b'r', b'e', b's',
    b's', 0, 0, 0, 0, 25, 255, 255, 255, 255, // Column 7: billing_address (Text)
    0, b'b', b'i', b'l', b'l', b'i', b'n', b'g', b'_', b'a', b'd', b'd', b'r', b'e', b's', b's', 0,
    0, 0, 0, 25, 255, 255, 255, 255, // Column 8: created_at (Text)
    0, b'c', b'r', b'e', b'a', b't', b'e', b'd', b'_', b'a', b't', 0, 0, 0, 0, 25, 255, 255, 255,
    255, // Column 9: updated_at (Text)
    0, b'u', b'p', b'd', b'a', b't', b'e', b'd', b'_', b'a', b't', 0, 0, 0, 0, 25, 255, 255, 255,
    255, // Column 10: metadata (Text)
    0, b'm', b'e', b't', b'a', b'd', b'a', b't', b'a', 0, 0, 0, 0, 25, 255, 255, 255, 255,
]);

fn bench(c: &mut Criterion) {
    decoder::relation::RelationData::parse(RELATION_DATA).unwrap();

    c.bench_function("relation", |b| {
        b.iter(|| {
            black_box(RelationData::parse(black_box(RELATION_DATA)).unwrap());
        });
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
