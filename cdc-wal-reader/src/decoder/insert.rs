use std::collections::HashMap;

use cdc_avro::ChangeEvent;

use crate::decoder::{
    DecoderError,
    common::get_new_tuple_data,
    relation::{self, Relation},
};

pub fn parse(
    data: bytes::Bytes,
    relation_map: &HashMap<u32, Relation>,
) -> Result<ChangeEvent, DecoderError> {
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
        table: relation.relname.clone(),
    })
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use cdc_avro::{ChangeEvent, PgValue};

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

        row.insert("id".to_string(), PgValue::Int4(1));
        row.insert("name".to_string(), PgValue::Text("hello".to_string()));

        let event_example = ChangeEvent {
            op: cdc_avro::Op::Insert { row },
            table: "users".to_string(),
        };

        assert_eq!(event, event_example);
    }
}
