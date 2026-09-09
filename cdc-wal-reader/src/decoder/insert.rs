use std::collections::HashMap;

use cdc_avro::ChangeEvent;

use crate::decoder::{relation::Relation, tuple_data::TupleData};

pub fn parse(data: bytes::Bytes, relation_map: &HashMap<u32, Relation>) -> ChangeEvent {
    let relation_oid = u32::from_be_bytes(data[1..5].try_into().unwrap());

    let relation = relation_map.get(&relation_oid).unwrap();

    assert!(data[5] == b'N');

    let row = TupleData::parse(&data[6..]).to_row(&relation);

    ChangeEvent {
        op: cdc_avro::Op::Insert { row },
        table: "users".to_string(), // TODO! Properly extract table
    }
}
