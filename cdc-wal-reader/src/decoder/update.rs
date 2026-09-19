use std::collections::HashMap;

use ahash::RandomState;
use bumpalo::Bump;
use cdc_avro::ChangeEvent;

use crate::decoder::{
    DecoderError,
    common::{get_new_tuple_data, get_old_tuple_data},
    relation::RelationData,
};

pub fn parse<'a>(
    data: &'a bytes::Bytes,
    relation_map: &'a HashMap<u32, RelationData, RandomState>,
    arena: &'a Bump,
) -> Result<(ChangeEvent<'a>, &'a RelationData<'a>), DecoderError> {
    /*let id = u32::from_be_bytes(
        data[0..4]
            .try_into()
            .map_err(|_| DecoderError::TruncatedInput)?,
    );*/

    let relation_oid = u32::from_be_bytes(
        data[4..8]
            .try_into()
            .map_err(|_| DecoderError::TruncatedInput)?,
    );

    let relation = relation_map
        .get(&relation_oid)
        .ok_or_else(|| DecoderError::UnknownRelation(relation_oid))?;

    let (old, old_data_end) = get_old_tuple_data(&data[8..], &relation, &arena)?;

    let new_data = get_new_tuple_data(&data[old_data_end + 8..], &arena, &relation.inner.fields)?;

    let event = ChangeEvent {
        op: cdc_avro::Op::Update { old, row: new_data },
        rel: relation_oid,
    };

    Ok((event, relation))
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use bumpalo::{Bump, collections::vec, vec};
    use cdc_avro::{ChangeEvent, Field, FieldKind, PgValue, Relation};

    use crate::decoder::{
        common::{
            self, get_example_rel, get_example_rel_data, get_example_rel_data_keys,
            get_example_rel_keys,
        },
        update::parse,
    };

    #[test]
    fn simple_update_key() {
        let data = bytes::Bytes::from_static(&[
            0, 0, 0, 1, // Event ID
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
            op: cdc_avro::Op::Update {
                old: bumpalo::vec![in &arena;PgValue::Int4(1)],
                row: vec![in &arena;
                    PgValue::Int4(1),
                    PgValue::Text("hello"),
                ],
            },
            rel: 1,
        };

        assert_eq!(
            event,
            Ok((event_example, &get_example_rel_data_keys(&arena)))
        );
    }

    #[test]
    fn simple_update_object() {
        let data = bytes::Bytes::from_static(&[
            0, 0, 0, 1, // Event ID
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

        let arena = Bump::with_capacity(1024);

        let relation_map = common::get_example_rel_map(&arena);

        let event = parse(&data, &relation_map, &arena);

        let event_example = ChangeEvent {
            op: cdc_avro::Op::Update {
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
}
