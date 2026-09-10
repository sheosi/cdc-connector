use std::collections::HashMap;

use cdc_avro::ChangeEvent;

use crate::decoder::{common::get_old_tuple_data, relation::Relation, tuple_data::TupleData};

pub async fn parse(data: &[u8], relation_map: &HashMap<u32, Relation>) -> ChangeEvent {
    let id = u32::from_be_bytes(data[0..4].try_into().unwrap());
    let relation_oid = u32::from_be_bytes(data[4..8].try_into().unwrap());

    let relation = relation_map.get(&relation_oid).unwrap();

    let key = get_old_tuple_data(&data[9..]);

    ChangeEvent {
        op: cdc_avro::Op::Delete { key: String::new() },
        table: "users".to_string(), // TODO! Properly extract table
    }
}

#[cfg(test)]
mod test {
    #[test]
    pub fn simple_delete() {}
}
