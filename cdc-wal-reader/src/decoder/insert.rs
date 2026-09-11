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
    use std::collections::HashMap;

    use cdc_avro::ChangeEvent;

    use crate::decoder::{common, insert::parse};

    #[test]
    fn simple_insert() {
        let data = bytes::Bytes::from_static(&[
            b'I', // Insert,
            0, 0, 0, 1, // Relation OID
            b'N', 0, 2, // Two columns
            // First col
            b'b', 0, 0, 0, 4, // Binary of size 4
            0, 0, 0, 1, // Int4: 1
            // Second col
            b't', 0, 0, 0, 5, // Text of length 5
            b'h', b'e', b'l', b'l', b'o', // Text hello
        ]);

        let relation_map = common::get_example_rel_map();

        let event = parse(data, &relation_map);

        let mut row = HashMap::new();

        row.insert("id".to_string(), "1".to_string());
        row.insert("name".to_string(), "hello".to_string());

        let event_example = ChangeEvent {
            op: cdc_avro::Op::Insert { row },
            table: "users".to_string(),
        };

        assert_eq!(event, event_example);
    }
}
