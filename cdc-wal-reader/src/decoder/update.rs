use std::collections::HashMap;

use cdc_avro::ChangeEvent;

use crate::decoder::{
    common::{get_new_tuple_data, get_old_tuple_data},
    relation::Relation,
};

pub fn parse(data: bytes::Bytes, relation_map: &HashMap<u32, Relation>) -> ChangeEvent {
    let id = u32::from_be_bytes(data[0..4].try_into().unwrap());
    let relation_oid = u32::from_be_bytes(data[4..8].try_into().unwrap());

    let relation = relation_map.get(&relation_oid).unwrap();

    let old_data = get_old_tuple_data(&data[9..]);
    let old_data_end = 0;

    // TODO: Were does old end?
    let new_data = get_new_tuple_data(&data[old_data_end..]).to_row(&relation);

    ChangeEvent {
        op: cdc_avro::Op::Update {
            key: String::new(), // How do we obtain this?
            row: new_data,
        },
        table: relation.relname.clone(),
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn simple_update() {}
}
