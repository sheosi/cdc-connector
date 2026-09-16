use std::collections::HashMap;

use cdc_avro::ChangeEvent;

use crate::decoder::{DecoderError, common::get_new_tuple_data, relation::Relation};

pub fn parse<'a, 'b>(
    data: &'a bytes::Bytes,
    relation_map: &'b HashMap<u32, Relation>,
) -> Result<ChangeEvent<'a, 'b>, DecoderError> {
    let relation_oid = u32::from_be_bytes(
        data[1..5]
            .try_into()
            .map_err(|_| DecoderError::TruncatedInput)?,
    );

    let relation = relation_map
        .get(&relation_oid)
        .ok_or_else(|| DecoderError::UnknownRelation(relation_oid))?;

    let row = get_new_tuple_data(&data[5..])?.into_row(&relation)?;

    Ok(ChangeEvent {
        op: cdc_avro::Op::Insert { row },
        table: &relation.relname,
    })
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use cdc_avro::{ChangeEvent, PgValue};

    use crate::decoder::{
        common,
        insert::parse,
        relation::{
            Field,
            FieldKind::{Int4, Text},
            KeyField, Relation,
        },
    };

    fn complex_relation() -> Relation {
        Relation {
            relation_oid: 16390,
            namespace: "public".to_string(),
            relname: "users".to_string(),
            replica_id: 100,
            fields: vec![
                Field {
                    is_key: true,
                    name: "id".to_string(),
                    kind: Int4,
                },
                Field {
                    is_key: false,
                    name: "name".to_string(),
                    kind: Text,
                },
                Field {
                    is_key: false,
                    name: "email".to_string(),
                    kind: Text,
                },
            ],
            key_fields: vec![KeyField {
                name: "id".to_string(),
                kind: Int4,
            }],
        }
    }

    fn complex_relation_map() -> HashMap<u32, Relation> {
        let mut rel_map = HashMap::new();
        rel_map.insert(16390, complex_relation());
        rel_map
    }

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

        let event = parse(&data, &relation_map);

        let mut row = HashMap::new();

        row.insert("id", PgValue::Int4(1));
        row.insert("name", PgValue::Text("hello"));

        let event_example = ChangeEvent {
            op: cdc_avro::Op::Insert { row },
            table: "users",
        };

        assert_eq!(event, Ok(event_example));
    }

    #[test]
    fn test_insert_complex() {
        let data = bytes::Bytes::from_static(&[
            b'I', 0x00, 0x00, 0x40, 0x06, b'N', 0x00, 0x03, b't', 0x00, 0x00, 0x00, 0x01, b'1',
            b't', 0x00, 0x00, 0x00, 0x03, b'a', b'd', b'a', b't', 0x00, 0x00, 0x00, 0x0F, b'a',
            b'd', b'a', b'@', b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'.', b'c', b'o', b'm',
        ]);

        let relation_map = complex_relation_map();

        let event = parse(&data, &relation_map);

        let mut row = HashMap::new();

        row.insert("id", PgValue::Int4(1));
        row.insert("name", PgValue::Text("ada"));
        row.insert("email", PgValue::Text("ada@example.com"));

        let event_example = ChangeEvent {
            op: cdc_avro::Op::Insert { row },
            table: "users",
        };

        assert_eq!(event, Ok(event_example));
    }
}
