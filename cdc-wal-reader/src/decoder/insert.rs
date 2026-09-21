use ahash::RandomState;
use std::collections::HashMap;

use bumpalo::Bump;
use cdc_avro::ChangeEvent;

use crate::decoder::{DecoderError, common::get_new_tuple_data, relation::RelationData};

pub fn parse<'a>(
    data: &'a [u8],
    relation_map: &'a HashMap<u32, RelationData, RandomState>,
    arena: &'a Bump,
) -> Result<(ChangeEvent<'a>, &'a RelationData<'a>), DecoderError> {
    let relation_oid = u32::from_be_bytes(
        data[1..5]
            .try_into()
            .map_err(|_| DecoderError::TruncatedInput)?,
    );

    let relation = relation_map
        .get(&relation_oid)
        .ok_or_else(|| DecoderError::UnknownRelation(relation_oid))?;

    let row = get_new_tuple_data(&data[5..], arena, &relation.inner.fields)?;

    let event = ChangeEvent {
        op: cdc_avro::Op::Insert { row },
        rel: relation.inner.relation_oid,
    };

    Ok((event, relation))
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use ahash::RandomState;
    use bumpalo::Bump;
    use cdc_avro::{
        ChangeEvent, Field,
        FieldKind::{Int4, Text},
        PgValue, Relation, ReplicaKind,
    };

    use bumpalo::vec;

    use crate::decoder::{
        common::{self, get_example_rel, get_example_rel_data},
        insert::parse,
        relation::{KeyField, RelationData},
    };

    fn complex_relation(arena: &Bump) -> RelationData {
        RelationData {
            inner: Relation {
                relation_oid: 16390,
                name: "users".to_string(),
                namespace: "public".to_string(),
                replica_id: ReplicaKind::Row,
                fields: vec![ in arena;
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
            },
            key_fields: vec![in arena;
                KeyField {
                    name: "id".to_string(),
                    kind: Int4,
                }
            ],
        }
    }

    fn complex_relation_map(arena: &Bump) -> HashMap<u32, RelationData, RandomState> {
        let mut rel_map = HashMap::default();
        rel_map.insert(16390, complex_relation(arena));
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

        let arena = Bump::new();

        let relation_map = common::get_example_rel_map(&arena);

        let event = parse(&data, &relation_map, &arena);

        let mut row = bumpalo::vec![in &arena;
                PgValue::Int4(1),
                PgValue::Text("hello")
        ];

        let event_example = ChangeEvent {
            op: cdc_avro::Op::Insert { row },
            rel: 1,
        };

        assert_eq!(event, Ok((event_example, &get_example_rel_data(&arena))));
    }

    #[test]
    fn test_insert_complex() {
        let data = bytes::Bytes::from_static(&[
            b'I', 0x00, 0x00, 0x40, 0x06, b'N', 0x00, 0x03, b't', 0x00, 0x00, 0x00, 0x01, b'1',
            b't', 0x00, 0x00, 0x00, 0x03, b'a', b'd', b'a', b't', 0x00, 0x00, 0x00, 0x0F, b'a',
            b'd', b'a', b'@', b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'.', b'c', b'o', b'm',
        ]);

        let arena = Bump::new();

        let relation_map = complex_relation_map(&arena);

        let event = parse(&data, &relation_map, &arena);

        let row = bumpalo::vec![in &arena;
            PgValue::Text("1"),
            PgValue::Text("ada"),
            PgValue::Text("ada@example.com")
        ];

        let event_example = ChangeEvent {
            op: cdc_avro::Op::Insert { row },
            rel: 16390,
        };

        assert_eq!(event, Ok((event_example, &complex_relation(&arena))));
    }
}
