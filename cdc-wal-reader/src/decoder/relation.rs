use bumpalo::collections::Vec;
use bumpalo::{Bump, collections::CollectIn};
use bytes::Bytes;
use cdc_avro::{Field, FieldAccess, FieldKind, Relation, ReplicaKind};
use std::ffi::CStr;

use crate::decoder::DecoderError;

#[derive(Debug, PartialEq)]
pub struct RelationData<'a> {
    pub inner: Relation<'a>,

    /// A subset of above, they are the fields marked as keys, for when only keys
    /// are searched for
    pub key_fields: Vec<'a, KeyField>,
}

impl<'a> RelationData<'a> {
    pub fn parse(data: Bytes, arena: &'a Bump) -> Result<RelationData<'a>, DecoderError> {
        let relation_oid = u32::from_be_bytes(
            data[1..5]
                .try_into()
                .map_err(|_| DecoderError::TruncatedInput)?,
        );
        let namespace = CStr::from_bytes_until_nul(&data[5..])
            .map_err(|_| DecoderError::TruncatedInput)?
            .to_str()?
            .to_string();

        let relname = CStr::from_bytes_until_nul(&data[5 + namespace.len() + 1..])
            .map_err(|_| DecoderError::TruncatedInput)?
            .to_str()?
            .to_string();

        let replica_id_pos = 5 + namespace.len() + 1 + relname.len() + 1;
        let replica_id = match data[replica_id_pos] {
            0 => ReplicaKind::Keys,
            2 => ReplicaKind::Row,
            a => return Err(DecoderError::WrongReplicaId(a)),
        };

        let cols = u16::from_be_bytes(
            data[replica_id_pos + 1..replica_id_pos + 3]
                .try_into()
                .map_err(|_| DecoderError::TruncatedInput)?,
        );

        let mut col_start_id = replica_id_pos + 3;
        let mut fields = bumpalo::collections::Vec::with_capacity_in(cols as usize, arena);

        for _ in 0..cols {
            match parse_field(&data[col_start_id..]) {
                Ok(FieldParseResult::Physical((field, bytes))) => {
                    col_start_id += bytes;
                    fields.push(field);
                }
                Ok(FieldParseResult::Logical(b)) => {
                    col_start_id += b;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        Ok(RelationData {
            key_fields: fields
                .iter()
                .filter_map(|f| {
                    if f.is_key {
                        Some(KeyField {
                            name: f.name.clone(),
                            kind: f.kind.clone(),
                        })
                    } else {
                        None
                    }
                })
                .collect_in(arena),
            inner: Relation {
                relation_oid,
                name: relname,
                fields,
                replica_id,
            },
        })
    }
}

// We already know those are keys, we don't need the is_key
#[derive(Debug, PartialEq)]
pub struct KeyField {
    pub name: String,
    pub kind: FieldKind,
}

impl FieldAccess for KeyField {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn get_kind(&self) -> FieldKind {
        self.kind
    }
}

/**  A field in a relation can be physical (in that it exists on disk) or logical
 * (a computed value of sorts), logical fields aren't present in WAL outputs
 * so we are going to ignore them. However, we do need to know the ammount of space
 * they take to continue parsing
 */
#[derive(Debug, PartialEq)]
pub enum FieldParseResult {
    Physical((Field, usize)),
    Logical(usize),
}

/// The Field might be logical, and thus not present, in those cases we don't
/// store it, but we need the size of it
fn parse_field(data: &[u8]) -> Result<FieldParseResult, DecoderError> {
    let flag = data[0];
    let name = CStr::from_bytes_until_nul(&data[1..])
        .map_err(|_| DecoderError::TruncatedInput)?
        .to_str()?
        .to_string();

    let l_name = name.len();

    let is_key = match flag {
        0 => false,
        1 => true,
        // mean a logical field
        2 | 3 => return Ok(FieldParseResult::Logical(byte_size_len(l_name))),
        a => return Err(DecoderError::WrongFieldDataFlag(a)),
    };

    let after_name = 1 + name.len() + 1;

    let t_oid = u32::from_be_bytes(
        data[after_name..after_name + 4]
            .try_into()
            .map_err(|_| DecoderError::TruncatedInput)?,
    );

    let t_mod = u32::from_be_bytes(
        data[after_name + 4..after_name + 8]
            .try_into()
            .map_err(|_| DecoderError::TruncatedInput)?,
    );

    Ok(FieldParseResult::Physical((
        Field {
            is_key,
            name,
            kind: kind_from_oid_mod(t_oid, t_mod)?,
        },
        byte_size_len(l_name),
    )))
}

fn byte_size_len(str_len: usize) -> usize {
    10 + str_len
}

fn kind_from_oid_mod(t_oid: u32, t_mod: u32) -> Result<FieldKind, DecoderError> {
    match t_oid {
        23 => Ok(FieldKind::Int4),
        25 => Ok(FieldKind::Text),
        a => Err(DecoderError::InvalidOid(a)),
    }
}

#[cfg(test)]
mod test {
    use bumpalo::{Bump, vec};

    use crate::decoder::relation::{
        Field, FieldKind, FieldParseResult, KeyField, Relation, RelationData, ReplicaKind,
        parse_field,
    };

    fn field_id() -> Field {
        Field {
            is_key: true,
            kind: FieldKind::Int4,
            name: "id".to_string(),
        }
    }

    fn key_field_id() -> KeyField {
        KeyField {
            kind: FieldKind::Int4,
            name: "id".to_string(),
        }
    }

    #[test]
    fn simple_relation() {
        let data = bytes::Bytes::from_static(&[
            b'R', // Relation
            0, 0, 0, 1, // Relation OID
            b'p', b'u', b'b', b'l', b'i', b'c', 0, // Namespace
            b'u', b's', b'e', b'r', b's', 0, // Relation name
            0, // Replica identity setting
            0, 1, // Number of columns
            // Column 1
            1, // Flags: Is key
            b'i', b'd', 0, // Name of the column: id
            0, 0, 0, 23, // Type oid (int4)
            0, 0, 0, 0, // Attrmod
        ]);

        let arena = Bump::new();

        let relation = RelationData::parse(data, &arena);

        let relation_manual = RelationData {
            key_fields: vec![in &arena; key_field_id()],
            inner: Relation {
                relation_oid: 1,
                name: "users".to_string(),
                replica_id: ReplicaKind::Keys,
                fields: vec![in &arena; field_id()],
            },
        };

        assert_eq!(relation, Ok(relation_manual));
    }

    #[test]
    fn simple_field() {
        let data = [
            1, // Flags: Is key
            b'i', b'd', 0, //Name of the column: id
            0, 0, 0, 23, // Type oid (int4)
            0, 0, 0, 0, // Attrmod
        ];
        let field = parse_field(&data);

        assert_eq!(field, Ok(FieldParseResult::Physical((field_id(), 12))));
    }
}
