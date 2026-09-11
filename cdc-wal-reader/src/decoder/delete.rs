use std::collections::HashMap;

use cdc_avro::ChangeEvent;

use crate::decoder::{common::get_old_tuple_data, relation::Relation};

/// Parse the bytes of a delete command, don't include the initial 'D' present
pub fn parse(data: bytes::Bytes, relation_map: &HashMap<u32, Relation>) -> ChangeEvent {
    let id = u32::from_be_bytes(data[0..4].try_into().unwrap());
    let relation_oid = u32::from_be_bytes(data[4..8].try_into().unwrap());

    let relation = relation_map.get(&relation_oid).unwrap();

    let key = get_old_tuple_data(&data[8..]);

    // TODO: Properly obtain key
    ChangeEvent {
        op: cdc_avro::Op::Delete { key: String::new() },
        table: relation.relname.clone(),
    }
}

#[cfg(test)]
mod test {
    use cdc_avro::ChangeEvent;

    use crate::decoder::{common, delete::parse};

    #[test]
    pub fn simple_delete_key() {
        let data = bytes::Bytes::from_static(&[
            0, 0, 0, 1, // Operation ID
            0, 0, 0, 1, // Relation OID
            b'K', 0, 1, // Two columns
            // First col
            b'b', 0, 0, 0, 4, // Binary of size 4
            0, 0, 0, 1, // Int4: 1
        ]);

        let relation_map = common::get_example_rel_map();

        let event = parse(data, &relation_map);

        let event_example = ChangeEvent {
            op: cdc_avro::Op::Delete {
                key: "1".to_string(),
            },
            table: "users".to_string(),
        };

        assert_eq!(event, event_example);
    }

    #[test]
    pub fn simple_delete_object() {
        let data = bytes::Bytes::from_static(&[
            0, 0, 0, 1, // Operation ID
            0, 0, 0, 1, // Relation OID
            b'O', 0, 2, // Two columns
            // First col
            b'b', 0, 0, 0, 4, // Binary of size 4
            0, 0, 0, 1, // Int4: 1
            // Second col
            b't', 0, 0, 0, 5, // Text of length 5
            b'h', b'e', b'l', b'l', b'o', // Text hello
        ]);

        let relation_map = common::get_example_rel_map();

        let event = parse(data, &relation_map);

        let event_example = ChangeEvent {
            op: cdc_avro::Op::Delete {
                key: "1".to_string(),
            },
            table: "users".to_string(),
        };

        assert_eq!(event, event_example);
    }
}
