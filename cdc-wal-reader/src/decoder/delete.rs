use std::collections::HashMap;

use ahash::RandomState;
use bumpalo::Bump;
use cdc_avro::ChangeEvent;

use crate::decoder::{
    DecoderError::{self},
    common::get_old_tuple_data,
    relation::Relation,
};

/// Parse the bytes of a delete command, don't include the initial 'D' present
pub fn parse<'a>(
    data: &'a bytes::Bytes,
    relation_map: &'a HashMap<u32, Relation, RandomState>,
    arena: &'a Bump,
) -> Result<ChangeEvent<'a>, DecoderError> {
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

    let (key, _) = get_old_tuple_data(&data[8..], relation, arena)?;

    Ok(ChangeEvent {
        op: cdc_avro::Op::Delete { key },
        table: &relation.relname,
    })
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use bumpalo::Bump;
    use cdc_avro::{ChangeEvent, PgValue};

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
        let arena = Bump::with_capacity(512);

        let event = parse(&data, &relation_map, &arena);

        let event_example = ChangeEvent {
            op: cdc_avro::Op::Delete {
                key: cdc_avro::OverrideData::Key(bumpalo::vec![in &arena; PgValue::Int4(1)]),
            },
            table: "users",
        };

        assert_eq!(event, Ok(event_example));
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

        let arena = Bump::new();

        let relation_map = common::get_example_rel_map();

        let event = parse(&data, &relation_map, &arena);

        let event_example = ChangeEvent {
            op: cdc_avro::Op::Delete {
                key: cdc_avro::OverrideData::Row(vec![in &arena;
                RowEntry {
                    key: "id",
                    value: PgValue::Int4(1),
                },
                RowEntry {
                    key: "name",
                    value: PgValue::Text("hello"),
                },]),
            },
            table: "users",
        };

        assert_eq!(event, Ok(event_example));
    }
}
