use std::collections::HashMap;

use cdc_avro::ChangeEvent;

use crate::decoder::{common::get_new_tuple_data, relation::Relation};

pub fn parse(data: bytes::Bytes, relation_map: &HashMap<u32, Relation>) -> ChangeEvent {
    let relation_oid = u32::from_be_bytes(data[1..5].try_into().unwrap());

    let relation = relation_map.get(&relation_oid).unwrap();

    let row = get_new_tuple_data(&data[5..]).to_row(&relation);

    ChangeEvent {
        op: cdc_avro::Op::Insert { row },
        table: relation.relname.clone(),
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn simple_insert() {}
}
