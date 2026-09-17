#[cfg(test)]
use ahash::RandomState;
use bumpalo::Bump;
use cdc_avro::{OverrideData, RowEntry};

use crate::decoder::{
    DecoderError::{self, WrongOldTupleKey},
    relation::Relation,
    tuple_data,
};
use simdutf8::basic::from_utf8 as simd_from_utf8;

#[cfg(test)]
use crate::decoder::relation::{Field, FieldKind, KeyField};

pub fn get_old_tuple_data<'a>(
    data: &'a [u8],
    relation: &'a Relation,
    arena: &'a Bump,
) -> Result<(OverrideData<'a>, usize), DecoderError> {
    match data[0] {
        b'K' => {
            let (keys, size) = tuple_data::parse_keys(&data[1..], arena, relation)?;
            Ok((OverrideData::Key(keys), size + 1))
        }
        b'O' => {
            let (row, size) = tuple_data::parse(&data[1..], arena, relation)?;
            Ok((OverrideData::Row(row), size + 1))
        }
        a => Err(WrongOldTupleKey(a)),
    }
}

pub fn get_new_tuple_data<'a>(
    data: &'a [u8],
    arena: &'a Bump,
    relation: &'a Relation,
) -> Result<bumpalo::collections::Vec<'a, RowEntry<'a>>, DecoderError> {
    if data[0] != b'N' {
        return Err(DecoderError::WrongNewTupleKey(data[0]));
    }

    let (tuple, _) = tuple_data::parse(&data[1..], arena, relation)?;
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
pub fn get_example_rel_map() -> std::collections::HashMap<u32, Relation, RandomState> {
    let mut relation_map = std::collections::HashMap::default();
    relation_map.insert(1u32, get_example_rel());

    relation_map
}

#[cfg(test)]
pub fn get_example_rel() -> Relation {
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
        key_fields: vec![KeyField {
            name: "id".to_string(),
            kind: FieldKind::Int4,
        }],
    }
}

#[cfg(test)]
pub fn col_byte_id<'a>() -> RowEntry<'a> {
    RowEntry {
        key: "id",
        value: cdc_avro::PgValue::Int4(1),
    }
}

#[cfg(test)]
pub fn col_text_name<'a>() -> RowEntry<'a> {
    RowEntry {
        key: "name",
        value: cdc_avro::PgValue::Text("hello"),
    }
}

#[cfg(test)]
mod test {
    use bumpalo::{Bump, vec};
    use cdc_avro::{OverrideData, PgValue, RowEntry};

    use crate::decoder::common::{
        col_byte_id, col_text_name, get_example_rel, get_new_tuple_data, get_old_tuple_data,
    };

    #[test]
    fn empty_old_tuple_data_key() {
        let data = [b'O', 0, 0];

        let arena = Bump::new();

        let example_rel = get_example_rel();

        let old_tuple = get_old_tuple_data(&data, &example_rel, &arena);
        let old_tuple_manual = OverrideData::Row(vec![in &arena]);

        assert_eq!(old_tuple, Ok((old_tuple_manual, 3)));
    }

    #[test]
    fn empty_key_tuple_data_key() {
        let data = [b'K', 0, 0];

        let arena = Bump::new();

        let example_rel = get_example_rel();

        let key_tuple = get_old_tuple_data(&data, &example_rel, &arena);
        let key_tuple_manual = OverrideData::Key(vec![in &arena]);

        assert_eq!(key_tuple, Ok((key_tuple_manual, 3)));
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

        let arena = Bump::new();
        let example_rel = get_example_rel();

        let old_tuple = get_old_tuple_data(&data, &example_rel, &arena);

        let old_tuple_manual = OverrideData::Row(vec![
        in &arena;
            RowEntry{key: "id", value: PgValue::Int4(1)},
            RowEntry{key:"name", value:PgValue::Text("hello")}
        ]);

        assert_eq!(old_tuple, Ok((old_tuple_manual, 22)));
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

        let arena = Bump::new();

        let example_rel = get_example_rel();

        let new_tuple = get_new_tuple_data(&data, &arena, &example_rel);
        let new_tuple_manual = vec![
        in &arena;
            col_byte_id(),
            col_text_name()
        ];

        assert_eq!(new_tuple, Ok(new_tuple_manual));
    }
}
