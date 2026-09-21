#[cfg(test)]
use ahash::RandomState;
use bumpalo::{Bump, collections::Vec};
use cdc_avro::{Field, PgValue, ReplicaKind};

use crate::decoder::{
    DecoderError::{self, WrongOldTupleKey},
    relation::RelationData,
    tuple_data,
};
use simdutf8::basic::from_utf8 as simd_from_utf8;

#[cfg(test)]
use crate::decoder::relation::KeyField;
#[cfg(test)]
use cdc_avro::{FieldKind, Relation};

pub fn get_old_tuple_data<'a>(
    data: &'a [u8],
    relation: &'a RelationData,
    arena: &'a Bump,
) -> Result<(Vec<'a, PgValue>, usize), DecoderError> {
    match data[0] {
        b'K' => {
            if relation.inner.replica_id != ReplicaKind::Keys {
                return Err(DecoderError::WrongOldTupleKind(ReplicaKind::Keys));
            }

            let (keys, size) = tuple_data::parse(&data[1..], arena, &relation.key_fields)?;
            Ok((keys, size + 1))
        }
        b'O' => {
            if relation.inner.replica_id != ReplicaKind::Row {
                return Err(DecoderError::WrongOldTupleKind(ReplicaKind::Row));
            }

            let (row, size) = tuple_data::parse(&data[1..], arena, &relation.inner.fields)?;
            Ok((row, size + 1))
        }
        a => Err(WrongOldTupleKey(a)),
    }
}

pub fn get_new_tuple_data<'a>(
    data: &[u8],
    arena: &'a Bump,
    fields: &'a [Field],
) -> Result<bumpalo::collections::Vec<'a, PgValue>, DecoderError> {
    if data[0] != b'N' {
        return Err(DecoderError::WrongNewTupleKey(data[0]));
    }

    let (tuple, _) = tuple_data::parse(&data[1..], arena, fields)?;
    Ok(tuple)
}

pub fn parse_str_long(data: &[u8]) -> Option<&str> {
    let nul = memchr::memchr(0, data).unwrap();
    simd_from_utf8(&data[..nul]).ok()
}

fn find_null_word(data: &[u8]) -> Option<usize> {
    let len = data.len();
    let mut i = 0;

    // head: scan until aligned
    while i < len && i % 8 != 0 {
        if data[i] == 0 {
            return Some(i);
        }
        i += 1;
    }

    // body: 8 bytes at a time
    while i + 8 <= len {
        let word = u64::from_ne_bytes(data[i..i + 8].try_into().unwrap());
        // has-zero-byte algorithm
        let mask = word.wrapping_sub(0x0101010101010101) & !word & 0x8080808080808080;
        if mask != 0 {
            let idx = i + (mask.trailing_zeros() / 8) as usize;
            return Some(idx);
        }
        i += 8;
    }

    // tail
    while i < len {
        if data[i] == 0 {
            return Some(i);
        }
        i += 1;
    }

    None
}

fn parse_scalar_simd(data: &[u8]) -> &str {
    let nul = find_null_word(data).unwrap();
    simd_from_utf8(&data[..nul]).unwrap()
}

#[cfg(test)]
pub fn get_example_rel_map(
    arena: &Bump,
) -> std::collections::HashMap<u32, RelationData, RandomState> {
    let mut relation_map = std::collections::HashMap::default();
    relation_map.insert(1u32, get_example_rel_data(arena));

    relation_map
}

#[cfg(test)]
pub fn get_example_rel(arena: &Bump) -> Relation {
    Relation {
        relation_oid: 1,
        name: "users".to_string(),
        namespace: "public".to_string(),
        replica_id: ReplicaKind::Row,
        fields: bumpalo::vec![in arena;
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
    }
}

#[cfg(test)]
pub fn get_example_rel_data(arena: &Bump) -> RelationData {
    RelationData {
        inner: get_example_rel(arena),
        key_fields: bumpalo::vec![in arena;
            KeyField {
                name: "id".to_string(),
                kind: FieldKind::Int4,
            }
        ],
    }
}

#[cfg(test)]
pub fn get_example_rel_map_keys(
    arena: &Bump,
) -> std::collections::HashMap<u32, RelationData, RandomState> {
    let mut relation_map = std::collections::HashMap::default();
    relation_map.insert(1u32, get_example_rel_data_keys(arena));

    relation_map
}

#[cfg(test)]
pub fn get_example_rel_keys(arena: &Bump) -> Relation {
    Relation {
        relation_oid: 1,
        name: "users".to_string(),
        namespace: "public".to_string(),
        replica_id: ReplicaKind::Keys,
        fields: bumpalo::vec![in arena;
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
    }
}

#[cfg(test)]
pub fn get_example_rel_data_keys(arena: &Bump) -> RelationData {
    RelationData {
        inner: get_example_rel_keys(arena),
        key_fields: bumpalo::vec![in arena;
            KeyField {
                name: "id".to_string(),
                kind: FieldKind::Int4,
            }
        ],
    }
}

#[cfg(test)]
pub fn col_byte_id<'a>() -> PgValue {
    cdc_avro::PgValue::Int4(1)
}

#[cfg(test)]
pub fn col_text_name<'a>() -> PgValue {
    let text = "hello";
    cdc_avro::PgValue::Text {
        ptr: text.as_ptr(),
        len: text.len(),
    }
}

#[cfg(test)]
mod test {
    use bumpalo::{Bump, vec};
    use cdc_avro::PgValue;

    use crate::decoder::common::{
        col_byte_id, col_text_name, get_example_rel, get_example_rel_data,
        get_example_rel_data_keys, get_new_tuple_data, get_old_tuple_data,
    };

    #[test]
    fn empty_old_tuple_data_key() {
        let data = bytes::Bytes::from_static(&[b'O', 0, 0]);

        let arena = Bump::new();

        let example_rel = get_example_rel_data(&arena);

        let old_tuple = get_old_tuple_data(&data, &example_rel, &arena);
        let old_tuple_manual = (vec![in &arena], 3);

        assert_eq!(old_tuple, Ok(old_tuple_manual));
    }

    #[test]
    fn empty_key_tuple_data_key() {
        let data = bytes::Bytes::from_static(&[b'K', 0, 0]);

        let arena = Bump::new();

        let example_rel = get_example_rel_data_keys(&arena);

        let key_tuple = get_old_tuple_data(&data, &example_rel, &arena);
        let key_tuple_manual = (vec![in &arena], 3);

        assert_eq!(key_tuple, Ok(key_tuple_manual));
    }

    #[test]
    fn simple_old_tuple_data_object() {
        let data = bytes::Bytes::from_static(&[
            b'O', 0, 2, // Two columns
            // First col
            b'b', 0, 0, 0, 4, // Binary of size 4
            0, 0, 0, 1, // Int4: 1
            // Second col
            b't', 0, 0, 0, 5, // Text of length 5
            b'h', b'e', b'l', b'l', b'o', // Text hello
        ]);

        let arena = Bump::new();
        let example_rel = get_example_rel_data(&arena);

        let old_tuple = get_old_tuple_data(&data, &example_rel, &arena);

        let old_tuple_row = vec![
        in &arena;
            col_byte_id(),
            col_text_name()
        ];
        let old_tuple_manual = (old_tuple_row, 22);

        assert_eq!(old_tuple, Ok(old_tuple_manual));
    }

    #[test]
    fn simple_get_new_tuple_data() {
        let data = bytes::Bytes::from_static(&[
            b'N', 0, 2, // Two columns
            // First col
            b'b', 0, 0, 0, 4, // Binary of size 4
            0, 0, 0, 1, // Int4: 1
            // Second col
            b't', 0, 0, 0, 5, // Text of length 5
            b'h', b'e', b'l', b'l', b'o', // Text hello
        ]);

        let arena = Bump::new();

        let example_rel = get_example_rel_data(&arena);

        let new_tuple = get_new_tuple_data(&data, &arena, &example_rel.inner.fields);
        let new_tuple_manual = vec![
        in &arena;
            col_byte_id(),
            col_text_name()
        ];

        assert_eq!(new_tuple, Ok(new_tuple_manual));
    }
}
