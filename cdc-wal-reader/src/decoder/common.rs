#[cfg(test)]
use std::collections::HashMap;

use crate::decoder::{
    DecoderError::{self, WrongOldTupleKey},
    tuple_data::TupleData,
};
#[cfg(test)]
use crate::decoder::{
    relation::{Field, FieldKind, Relation},
    tuple_data::TupleCol,
};

pub fn get_old_tuple_data(data: &[u8]) -> Result<TupleData, DecoderError> {
    match data[0] {
        b'K' => TupleData::parse(&data[1..]),
        b'O' => TupleData::parse(&data[1..]),
        a => Err(WrongOldTupleKey(a)),
    }
}

pub fn get_new_tuple_data(data: &[u8]) -> Result<TupleData, DecoderError> {
    if data[0] != b'N' {
        return Err(DecoderError::WrongNewTupleKey(data[0]));
    }

    TupleData::parse(&data[1..])
}

#[cfg(test)]
pub fn get_example_rel_map() -> HashMap<u32, Relation> {
    let mut relation_map = HashMap::new();
    relation_map.insert(
        1u32,
        Relation {
            relation_oid: 1,
            namespace: "public".to_string(),
            relname: "users".to_string(),
            replica_id: 0,
            fields: vec![
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
            ],
        },
    );

    relation_map
}

#[cfg(test)]
pub fn col_byte_one() -> TupleCol {
    TupleCol::Bytes(bytes::Bytes::from_static(&[0, 0, 0, 1]))
}

#[cfg(test)]
pub fn col_text_hello() -> TupleCol {
    TupleCol::Text("hello".to_string())
}

#[cfg(test)]
mod test {
    use crate::decoder::{
        common::{col_byte_one, col_text_hello, get_new_tuple_data, get_old_tuple_data},
        tuple_data::TupleData,
    };

    #[test]
    fn empty_old_tuple_data_key() {
        let data = [b'O', 0, 0];

        let old_tuple = get_old_tuple_data(&data);
        let old_tuple_manual = TupleData { cols: vec![] };

        assert_eq!(old_tuple, old_tuple_manual);
    }

    #[test]
    fn empty_key_tuple_data_key() {
        let data = [b'K', 0, 0];

        let key_tuple = get_old_tuple_data(&data);
        let key_tuple_manual = TupleData { cols: vec![] };

        assert_eq!(key_tuple, key_tuple_manual);
    }

    #[test]
    fn simple_old_tuple_data_object() {
        let data = [
            b'O', 0, 2, // Two columns
            // First col
            b'b', 0, 0, 0, 4, // Binary of size 4
            0, 0, 0, 1, // Int4: 1
            // Second col
            b't', 0, 0, 0, 5, // Text of length 5
            b'h', b'e', b'l', b'l', b'o', // Text hello
        ];

        let old_tuple = get_old_tuple_data(&data);
        let old_tuple_manual = TupleData {
            cols: vec![col_byte_one(), col_text_hello()],
        };

        assert_eq!(old_tuple, old_tuple_manual);
    }

    #[test]
    fn simple_get_new_tuple_data() {
        let data = [
            b'N', 0, 2, // Two columns
            // First col
            b'b', 0, 0, 0, 4, // Binary of size 4
            0, 0, 0, 1, // Int4: 1
            // Second col
            b't', 0, 0, 0, 5, // Text of length 5
            b'h', b'e', b'l', b'l', b'o', // Text hello
        ];

        let new_tuple = get_new_tuple_data(&data);
        let new_tuple_manual = TupleData {
            cols: vec![col_byte_one(), col_text_hello()],
        };

        assert_eq!(new_tuple, new_tuple_manual);
    }
}
