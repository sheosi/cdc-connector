use std::{collections::HashMap, time::Instant};

use ahash::RandomState;
use bumpalo::Bump;
use cdc_avro::arena::{ChangeEvent, Op};

use crate::decoder::{
    DecoderError::{self},
    common::get_old_tuple_data,
    relation::RelationData,
};

/// Parse the bytes of a delete command, don't include the initial 'D' present
pub fn parse<'a>(
    data: &'a bytes::Bytes,
    relation_map: &'a HashMap<u32, RelationData, RandomState>,
    arena: &'a Bump,
) -> Result<(ChangeEvent<'a>, &'a RelationData), DecoderError> {
    let start = Instant::now();

    if data.len() < 5 {
        return Err(DecoderError::TruncatedInput);
    }

    let relation_oid = u32::from_be_bytes(data[1..5].try_into().expect(""));

    let relation = relation_map
        .get(&relation_oid)
        .ok_or_else(|| DecoderError::UnknownRelation(relation_oid))?;

    let (old, _) = get_old_tuple_data(&data[5..], &relation, arena)?;

    let event = ChangeEvent {
        op: Op::Delete { old },
        rel: relation_oid,
    };

    metrics::histogram!("cdc_tuple_parse_duration_seconds", "op"=>"delete")
        .record(start.elapsed().as_secs_f64());

    Ok((event, relation))
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use ahash::RandomState;
    use bumpalo::{Bump, vec};
    use bytes::Bytes;
    use cdc_avro::{
        Field, FieldKind, PgValue, Relation, ReplicaKind,
        arena::{ChangeEvent, Op},
    };

    use crate::decoder::{
        DecoderError,
        common::{self, get_example_rel_data, get_example_rel_data_keys},
        delete::parse,
        relation::{KeyField, RelationData},
    };

    fn get_tests_rel() -> Relation {
        use bumpalo::collections::String;
        Relation {
            relation_oid: 16386,
            name: "users".to_string(),
            namespace: "public".to_string(),
            replica_id: ReplicaKind::Keys,
            fields: std::vec![
                Field {
                    name: "id".to_string(),
                    is_key: true,
                    kind: FieldKind::Int4,
                },
                Field {
                    name: "name".to_string(),
                    is_key: false,
                    kind: FieldKind::Text,
                },
                Field {
                    name: "email".to_string(),
                    is_key: false,
                    kind: FieldKind::Text,
                },
            ],
        }
    }

    fn get_tests_rel_map(
        arena: &Bump,
    ) -> std::collections::HashMap<u32, RelationData, RandomState> {
        let mut relation_map = std::collections::HashMap::default();
        relation_map.insert(16386u32, get_tests_rel_data());

        relation_map
    }

    fn get_tests_rel_data() -> RelationData {
        RelationData {
            inner: get_tests_rel(),
            key_fields: std::vec![KeyField {
                name: "id".to_string(),
                kind: FieldKind::Int4,
            }],
            key_indexes: std::vec![0],
        }
    }

    #[test]
    pub fn simple_delete_key() {
        let data = Bytes::from_static(&[
            b'D', 0, 0, 0, 1, // Relation OID
            b'K', 0, 1, // One col
            // First col
            b'b', 0, 0, 0, 4, // Binary of size 4
            0, 0, 0, 1, // Int4: 1
            b'n', b'n',
        ]);

        let arena = Bump::with_capacity(512);

        let relation_map = common::get_example_rel_map_keys(&arena);

        let event = parse(&data, &relation_map, &arena);

        let event_example = ChangeEvent {
            op: Op::Delete {
                old: bumpalo::vec![in &arena; PgValue::Int4(1)],
            },
            rel: 1,
        };

        assert_eq!(event, Ok((event_example, &get_example_rel_data_keys())));
    }

    #[test]
    pub fn simple_delete_object() {
        let data = Bytes::from_static(&[
            b'D', 0, 0, 0, 1, // Relation OID
            b'O', 0, 2, // Two columns
            // First col
            b'b', 0, 0, 0, 4, // Binary of size 4
            0, 0, 0, 1, // Int4: 1
            // Second col
            b't', 0, 0, 0, 5, // Text of length 5
            b'h', b'e', b'l', b'l', b'o', // Text hello
        ]);

        let arena = Bump::new();

        let relation_map = common::get_example_rel_map();

        let event = parse(&data, &relation_map, &arena);

        let event_example = ChangeEvent {
            op: Op::Delete {
                old: vec![in &arena;
                    PgValue::Int4(1),
                    PgValue::Text("hello")
                ],
            },
            rel: 1,
        };

        assert_eq!(event, Ok((event_example, &get_example_rel_data())));
    }

    #[test]
    fn example1() {
        let data = Bytes::from_static(&[
            b'D', 0, 0, 0x40, 0x02, // D + relation OID 16386
            b'K', 0, 0x03, // K + 3 columns
            b'b', 0, 0, 0, 0x04, // id: binary, length 4
            0, 0, 0, 0x04, // id value = 4
            b'n', // name = NULL
            b'n', // email = NULL
        ]);

        let arena = Bump::new();

        let relation_map = get_tests_rel_map(&arena);

        let event = parse(&data, &relation_map, &arena);

        let event_example = ChangeEvent {
            op: Op::Delete {
                old: vec![in &arena;
                    PgValue::Int4(4)
                ],
            },
            rel: 16386,
        };

        assert_eq!(event, Ok((event_example, &get_tests_rel_data())));
    }

    // Errors
    #[test]
    fn empty() {
        let data = Bytes::from_static(&[]);

        let arena = Bump::new();

        let relation_map = common::get_example_rel_map();

        let event = parse(&data, &relation_map, &arena);

        assert_eq!(event, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn no_relation_id() {
        let data = Bytes::from_static(&[b'D']);

        let arena = Bump::new();

        let relation_map = common::get_example_rel_map();

        let event = parse(&data, &relation_map, &arena);

        assert_eq!(event, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn no_tuple() {
        let data = Bytes::from_static(&[b'D', 0, 0, 0, 1]);

        let arena = Bump::new();

        let relation_map = common::get_example_rel_map();

        let event = parse(&data, &relation_map, &arena);

        assert_eq!(event, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn wrong_rel() {
        let data = Bytes::from_static(&[b'D', 0, 0, 0, 0, 0, 0, 0, 0]);

        let arena = Bump::new();

        let relation_map = common::get_example_rel_map();

        let event = parse(&data, &relation_map, &arena);

        assert_eq!(event, Err(DecoderError::UnknownRelation(0)))
    }
}
