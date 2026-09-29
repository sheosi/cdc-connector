use std::{collections::HashMap, time::Instant};

use ahash::RandomState;
use bumpalo::Bump;
use cdc_avro::arena::{ChangeEvent, Op};

use crate::decoder::{
    DecoderError,
    common::{get_new_tuple_data, get_old_tuple_data},
    relation::RelationData,
};

pub fn parse<'a>(
    data: &'a bytes::Bytes,
    relation_map: &'a HashMap<u32, RelationData, RandomState>,
    arena: &'a Bump,
) -> Result<(ChangeEvent<'a>, &'a RelationData), DecoderError> {
    let start = Instant::now();

    if data.len() < 9 {
        return Err(DecoderError::TruncatedInput);
    }

    /*let id = u32::from_be_bytes(
        data[0..4]
            .try_into()
            .expect(""),
    );*/

    let relation_oid = u32::from_be_bytes(data[5..9].try_into().expect(""));

    let relation = relation_map
        .get(&relation_oid)
        .ok_or_else(|| DecoderError::UnknownRelation(relation_oid))?;

    let (old, old_data_end) = get_old_tuple_data(&data[9..], &relation, &arena)?;

    let new_data = get_new_tuple_data(&data[old_data_end + 9..], &relation.inner.fields, &arena)?;

    let event = ChangeEvent {
        op: Op::Update { old, row: new_data },
        rel: relation_oid,
    };

    metrics::histogram!("cdc_tuple_parse_duration_seconds", "op"=>"update")
        .record(start.elapsed().as_secs_f64());
    Ok((event, relation))
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use bumpalo::{Bump, collections::vec, vec};
    use bytes::Bytes;
    use cdc_avro::{
        Field, FieldKind, PgValue, Relation,
        arena::{ChangeEvent, Op},
    };

    use crate::decoder::{
        DecoderError,
        common::{
            self, get_example_rel, get_example_rel_data, get_example_rel_data_keys,
            get_example_rel_keys,
        },
        update::parse,
    };

    #[test]
    fn simple_update_key() {
        let data = Bytes::from_static(&[
            b'U', 0, 0, 0, 1, // Event ID
            0, 0, 0, 1, // Relation OID
            // Old tuple
            b'K', 0, 1, // Tuple with only key
            // First col
            b'b', 0, 0, 0, 4, // Binary of size 4
            0, 0, 0, 1, // Int4: 1
            // New tuple
            b'N', 0, 2, // First col
            b'b', 0, 0, 0, 4, // Binary of size 4
            0, 0, 0, 1, // Int4: 1
            // Second col
            b't', 0, 0, 0, 5, // Text of length 5
            b'h', b'e', b'l', b'l', b'o', // Text hello
        ]);

        let arena = Bump::new();
        let relation_map = common::get_example_rel_map_keys(&arena);

        let event = parse(&data, &relation_map, &arena);

        let event_example = ChangeEvent {
            op: Op::Update {
                old: bumpalo::vec![in &arena;PgValue::Int4(1)],
                row: vec![in &arena;
                    PgValue::Int4(1),
                    PgValue::Text("hello"),
                ],
            },
            rel: 1,
        };

        assert_eq!(event, Ok((event_example, &get_example_rel_data_keys())));
    }

    #[test]
    fn simple_update_object() {
        let data = Bytes::from_static(&[
            b'U', 0, 0, 0, 1, // Event ID
            0, 0, 0, 1, // Relation OID
            b'O', 0, 2, // Return Old tuple
            // First col
            b'b', 0, 0, 0, 4, // Binary of size 4
            0, 0, 0, 1, // Int4: 1
            // Second col
            b't', 0, 0, 0, 5, // Text of length 5
            b'h', b'e', b'l', b'l', b'o', // Text hello
            // New tuple
            b'N', 0, 2, // First col
            b'b', 0, 0, 0, 4, // Binary of size 4
            0, 0, 0, 1, // Int4: 1
            // Second col
            b't', 0, 0, 0, 5, // Text of length 5
            b'h', b'e', b'l', b'l', b'o', // Text hello
        ]);

        let arena = Bump::new();

        let relation_map = common::get_example_rel_map(&arena);

        let event = parse(&data, &relation_map, &arena);

        let event_example = ChangeEvent {
            op: Op::Update {
                old: vec![ in &arena;
                    cdc_avro::PgValue::Int4(1),
                    cdc_avro::PgValue::Text("hello"),
                ],

                row: vec![ in &arena;
                    PgValue::Int4(1),
                    PgValue::Text("hello"),
                ],
            },
            rel: 1,
        };

        assert_eq!(event, Ok((event_example, &get_example_rel_data(&arena))));
    }

    #[test]
    fn no_event_id() {
        let data = Bytes::from_static(&[b'U']);

        let arena = Bump::new();

        let relation_map = common::get_example_rel_map(&arena);

        let event = parse(&data, &relation_map, &arena);

        assert_eq!(event, Err(DecoderError::TruncatedInput))
    }

    #[test]
    fn no_relation_id() {
        let data = Bytes::from_static(&[b'U', 0, 0, 0, 0]);

        let arena = Bump::new();

        let relation_map = common::get_example_rel_map(&arena);

        let event = parse(&data, &relation_map, &arena);

        assert_eq!(event, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn wrong_relation_id() {
        let data = Bytes::from_static(&[b'U', 0, 0, 0, 0, 0, 0, 0, 0]);

        let arena = Bump::new();

        let relation_map = common::get_example_rel_map(&arena);

        let event = parse(&data, &relation_map, &arena);

        assert_eq!(event, Err(DecoderError::UnknownRelation(0)));
    }

    #[test]
    fn no_tuples() {
        let data = Bytes::from_static(&[b'U', 0, 0, 0, 0, 0, 0, 0, 1]);

        let arena = Bump::new();

        let relation_map = common::get_example_rel_map(&arena);

        let event = parse(&data, &relation_map, &arena);

        assert_eq!(event, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn no_new_tuple() {
        let data = Bytes::from_static(&[
            b'U', 0, 0, 0, 1, // Event ID
            0, 0, 0, 1, // Relation OID
            // Old tuple
            b'K', 0, 1, // Tuple with only key
            // First col
            b'b', 0, 0, 0, 4, // Binary of size 4
            0, 0, 0, 1, // Int4: 1
        ]);

        let arena = Bump::new();
        let relation_map = common::get_example_rel_map_keys(&arena);

        let event = parse(&data, &relation_map, &arena);

        assert_eq!(event, Err(DecoderError::TruncatedInput));
    }
}
