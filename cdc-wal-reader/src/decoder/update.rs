use std::collections::HashMap;

use cdc_avro::ChangeEvent;

use crate::decoder::{
    DecoderError,
    common::{get_new_tuple_data, get_old_tuple_data},
    relation::Relation,
};

pub fn parse(
    data: bytes::Bytes,
    relation_map: &HashMap<u32, Relation>,
) -> Result<ChangeEvent, DecoderError> {
    let id = u32::from_be_bytes(
        data[0..4]
            .try_into()
            .map_err(|_| DecoderError::TruncatedInput)?,
    );
    let relation_oid = u32::from_be_bytes(
        data[4..8]
            .try_into()
            .map_err(|_| DecoderError::TruncatedInput)?,
    );

    let relation = relation_map
        .get(&relation_oid)
        .ok_or_else(|| DecoderError::UnknownRelation(relation_oid))?;

    let (key, old_data_end) = get_old_tuple_data(&data[8..], &relation)?;

    let new_data = get_new_tuple_data(&data[old_data_end + 8..])?.into_row(&relation)?;

    Ok(ChangeEvent {
        op: cdc_avro::Op::Update { key, row: new_data },
        table: relation.relname.clone(),
    })
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

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

        let event = parse(data, &relation_map);

        let mut row = HashMap::new();
        row.insert("id".to_string(), cdc_avro::PgValue::Int4(1));
        row.insert(
            "name".to_string(),
            cdc_avro::PgValue::Text("hello".to_string()),
        );

        let event_example = ChangeEvent {
            op: cdc_avro::Op::Update {
                key: cdc_avro::OverrideData::Key(vec![PgValue::Int4(1)]),
                row,
            },
            table: "users".to_string(),
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

        let relation_map = common::get_example_rel_map();

        let event = parse(data, &relation_map);

        let mut old_row = HashMap::new();
        old_row.insert("id".to_string(), PgValue::Int4(1));
        old_row.insert("name".to_string(), PgValue::Text("hello".to_string()));

        let mut row = HashMap::new();
        row.insert("id".to_string(), PgValue::Int4(1));
        row.insert(
            "name".to_string(),
            cdc_avro::PgValue::Text("hello".to_string()),
        );

        let event_example = ChangeEvent {
            op: cdc_avro::Op::Update {
                key: cdc_avro::OverrideData::Row(old_row),
                row,
            },
            table: "users".to_string(),
        };

        assert_eq!(event, Ok(event_example));
    }
}
