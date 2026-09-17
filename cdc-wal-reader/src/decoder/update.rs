use std::collections::HashMap;

use ahash::RandomState;
use bumpalo::Bump;
use cdc_avro::ChangeEvent;

use crate::decoder::{
    DecoderError,
    common::{get_new_tuple_data, get_old_tuple_data},
    relation::Relation,
};

pub fn parse<'a, 'b>(
    data: &'a bytes::Bytes,
    relation_map: &'b HashMap<u32, Relation, RandomState>,
    arena: &'a Bump,
) -> Result<ChangeEvent<'a, 'b>, DecoderError> {
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

    let (key, old_data_end) = get_old_tuple_data(&data[8..], &relation, &arena)?;

    let new_data =
        get_new_tuple_data(&data[old_data_end + 8..], &arena)?.into_row(&relation, arena)?;
    let new_data = HashMap::new();

    Ok(ChangeEvent {
        op: cdc_avro::Op::Update { key, row: new_data },
        table: &relation.relname,
    })
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use bumpalo::Bump;
    use cdc_avro::{ChangeEvent, PgValue};

    use crate::decoder::{
        common,
        relation::{Field, FieldKind, Relation},
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

        let relation_map = common::get_example_rel_map();
        let arena = Bump::new();

        let event = parse(&data, &relation_map, &arena);

        let mut row = HashMap::new();
        row.insert("id", cdc_avro::PgValue::Int4(1));
        row.insert("name", cdc_avro::PgValue::Text("hello"));

        let event_example = ChangeEvent {
            op: cdc_avro::Op::Update {
                key: cdc_avro::OverrideData::Key(bumpalo::vec![in &b;PgValue::Int4(1)]),
                row,
            },
            table: "users",
        };

        assert_eq!(event, Ok(event_example));
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

        let relation_map = common::get_example_rel_map();

        let event = parse(&data, &relation_map, &arena);

        let mut old_row = HashMap::new();
        old_row.insert("id", PgValue::Int4(1));
        old_row.insert("name", PgValue::Text("hello"));

        let mut row = HashMap::new();
        row.insert("id", PgValue::Int4(1));
        row.insert("name", cdc_avro::PgValue::Text("hello"));

        let event_example = ChangeEvent {
            op: cdc_avro::Op::Update {
                key: cdc_avro::OverrideData::Row(old_row),
                row,
            },
            table: "users",
        };

        assert_eq!(event, Ok(event_example));
    }
}
