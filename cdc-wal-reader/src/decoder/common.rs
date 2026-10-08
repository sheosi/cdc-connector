#[cfg(test)]
use ahash::RandomState;
use bumpalo::{Bump, collections::Vec};
use cdc_avro::{Field, PgValue, ReplicaKind};

use crate::decoder::{
    DecoderError::{self, WrongOldTupleKey},
    relation::RelationData,
    tuple_data,
};

#[cfg(test)]
use crate::decoder::relation::KeyField;
#[cfg(test)]
use cdc_avro::{FieldKind, Relation};

pub fn get_old_tuple_data<'a>(
    data: &'a [u8],
    relation: &'a RelationData,
    arena: &'a Bump,
) -> Result<(Vec<'a, PgValue<'a>>, usize), DecoderError> {
    if data.len() == 0 {
        return Err(DecoderError::TruncatedInput);
    }

    match data[0] {
        b'K' => {
            if relation.inner.replica_id != ReplicaKind::Keys {
                return Err(DecoderError::WrongOldTupleKind(ReplicaKind::Keys));
            }

            let (keys, size) = tuple_data::parse_keys(
                &data[1..],
                arena,
                &relation.key_fields,
                &relation.key_indexes,
                relation.inner.fields.len(),
            )?;
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
    data: &'a [u8],
    fields: &'a [Field],
    arena: &'a Bump,
) -> Result<bumpalo::collections::Vec<'a, PgValue<'a>>, DecoderError> {
    if data.len() == 0 {
        return Err(DecoderError::TruncatedInput);
    }

    if data[0] != b'N' {
        return Err(DecoderError::WrongNewTupleKey(data[0]));
    }

    let (tuple, _) = tuple_data::parse(&data[1..], arena, fields)?;
    Ok(tuple)
}

#[cfg(test)]
pub fn get_example_rel_map() -> std::collections::HashMap<u32, RelationData, RandomState> {
    let mut relation_map = std::collections::HashMap::default();
    relation_map.insert(1u32, get_example_rel_data());

    relation_map
}

#[cfg(test)]
pub fn get_example_rel() -> Relation {
    use bumpalo::collections::String;
    Relation {
        relation_oid: 1,
        name: "users".to_string(),
        namespace: "public".to_string(),
        replica_id: ReplicaKind::Row,
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
    }
}

#[cfg(test)]
pub fn get_example_rel_data() -> RelationData {
    RelationData {
        inner: get_example_rel(),
        key_fields: vec![KeyField {
            name: "id".to_string(),
            kind: FieldKind::Int4,
        }],
        key_indexes: vec![0],
    }
}

#[cfg(test)]
pub fn get_example_rel_map_keys(
    arena: &Bump,
) -> std::collections::HashMap<u32, RelationData, RandomState> {
    let mut relation_map = std::collections::HashMap::default();
    relation_map.insert(1u32, get_example_rel_data_keys());

    relation_map
}

#[cfg(test)]
pub fn get_example_rel_keys() -> Relation {
    Relation {
        relation_oid: 1,
        name: "users".to_string(),
        namespace: "public".to_string(),
        replica_id: ReplicaKind::Keys,
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
    }
}

#[cfg(test)]
pub fn get_example_rel_data_keys() -> RelationData {
    RelationData {
        inner: get_example_rel_keys(),
        key_fields: vec![KeyField {
            name: "id".to_string(),
            kind: FieldKind::Int4,
        }],
        key_indexes: vec![0],
    }
}

#[cfg(test)]
pub fn col_byte_id<'a>() -> PgValue<'a> {
    cdc_avro::PgValue::Int4(1)
}

#[cfg(test)]
pub fn col_text_name<'a>() -> PgValue<'a> {
    cdc_avro::PgValue::Text("hello")
}

#[cfg(test)]
mod test {
    use bumpalo::{Bump, vec};
    use cdc_avro::PgValue;

    use crate::decoder::common::{
        DecoderError, col_byte_id, col_text_name, get_example_rel, get_example_rel_data,
        get_example_rel_data_keys, get_new_tuple_data, get_old_tuple_data,
    };

    #[test]
    fn empty_old_tuple_data_key() {
        let data = [b'O', 0, 0];

        let arena = Bump::new();

        let example_rel = get_example_rel_data();

        let old_tuple = get_old_tuple_data(&data, &example_rel, &arena);
        let old_tuple_manual = (vec![in &arena], 3);

        assert_eq!(old_tuple, Ok(old_tuple_manual));
    }

    #[test]
    fn empty_key_tuple_data_key() {
        let data = [b'K', 0, 0];

        let arena = Bump::new();

        let example_rel = get_example_rel_data_keys();

        let key_tuple = get_old_tuple_data(&data, &example_rel, &arena);
        let key_tuple_manual = (vec![in &arena], 3);

        assert_eq!(key_tuple, Ok(key_tuple_manual));
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
        let example_rel = get_example_rel_data();

        let old_tuple = get_old_tuple_data(&data, &example_rel, &arena);

        let old_tuple_row = vec![
        in &arena;
            PgValue::Int4(1),
            PgValue::Text("hello")
        ];
        let old_tuple_manual = (old_tuple_row, 22);

        assert_eq!(old_tuple, Ok(old_tuple_manual));
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

        let example_rel = get_example_rel_data();

        let new_tuple = get_new_tuple_data(&data, &example_rel.inner.fields, &arena);
        let new_tuple_manual = vec![
        in &arena;
            col_byte_id(),
            col_text_name()
        ];

        assert_eq!(new_tuple, Ok(new_tuple_manual));
    }

    #[test]
    fn old_tuple_no_size() {
        let data = [b'O'];

        let arena = Bump::new();

        let example_rel = get_example_rel_data();

        let new_tuple = get_old_tuple_data(&data, &example_rel, &arena);

        assert_eq!(new_tuple, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn key_no_size() {
        let data = [b'K'];

        let arena = Bump::new();

        let example_rel = get_example_rel_data_keys();

        let new_tuple = get_old_tuple_data(&data, &example_rel, &arena);

        assert_eq!(new_tuple, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn empty_old() {
        let data = [];

        let arena = Bump::new();

        let example_rel = get_example_rel_data();

        let new_tuple = get_old_tuple_data(&data, &example_rel, &arena);

        assert_eq!(new_tuple, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn empty_new() {
        let data = [];

        let arena = Bump::new();

        let example_rel = get_example_rel_data();

        let new_tuple = get_new_tuple_data(&data, &example_rel.inner.fields, &arena);

        assert_eq!(new_tuple, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn wrong_old_tuple_key() {
        let data = [b'C'];

        let arena = Bump::new();

        let example_rel = get_example_rel_data();

        let new_tuple = get_old_tuple_data(&data, &example_rel, &arena);

        assert_eq!(new_tuple, Err(DecoderError::WrongOldTupleKey(b'C')));
    }

    #[test]
    fn wrong_new_tuple_key() {
        let data = [b'C'];

        let arena = Bump::new();

        let example_rel = get_example_rel_data();

        let new_tuple = get_new_tuple_data(&data, &example_rel.inner.fields, &arena);

        assert_eq!(new_tuple, Err(DecoderError::WrongNewTupleKey(b'C')));
    }
}
