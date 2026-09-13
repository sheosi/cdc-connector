use bytes::Bytes;
use std::ffi::CStr;

use crate::decoder::DecoderError;

#[derive(Debug, PartialEq)]
pub struct Relation {
    pub relation_oid: u32,
    pub namespace: String,
    pub relname: String,
    pub replica_id: u8,
    pub fields: Vec<Field>,

    /// A subset of above, they are the fields marked as keys, for when only keys
    /// are searched for
    pub key_fields: Vec<KeyField>,
}

impl Relation {
    pub fn parse(data: Bytes) -> Result<Relation, DecoderError> {
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
        let replica_id = data[replica_id_pos];

        let cols = u16::from_be_bytes(
            data[replica_id_pos + 1..replica_id_pos + 3]
                .try_into()
                .map_err(|_| DecoderError::TruncatedInput)?,
        );

        let mut col_start_id = replica_id_pos + 3;
        let mut fields = Vec::with_capacity(cols as usize);

        for _ in 0..cols {
            match Field::parse(&data[col_start_id..]) {
                Ok(FieldParseResult::Physical(field)) => {
                    col_start_id += field.byte_size();
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

        Ok(Relation {
            relation_oid,
            namespace,
            relname,
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
                .collect(),

            fields,
            replica_id,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct Field {
    pub is_key: bool,
    pub name: String,
    pub kind: FieldKind,
}

// We already know those are keys, we don't need the is_key
#[derive(Debug, PartialEq)]
pub struct KeyField {
    pub name: String,
    pub kind: FieldKind,
}

/**  A field in a relation can be physical (in that it exists on disk) or logical
 * (a computed value of sorts), logical fields aren't present in WAL outputs
 * so we are going to ignore them. However, we do need to know the ammount of space
 * they take to continue parsing
 */
#[derive(Debug, PartialEq)]
pub enum FieldParseResult {
    Physical(Field),
    Logical(usize),
}

impl Field {
    /// The Field might be logical, and thus not present, in those cases we don't
    /// store it, but we need the size of it
    fn parse(data: &[u8]) -> Result<FieldParseResult, DecoderError> {
        let flag = data[0];
        let name = CStr::from_bytes_until_nul(&data[1..])
            .map_err(|_| DecoderError::TruncatedInput)?
            .to_str()?
            .to_string();

        let is_key = match flag {
            0 => false,
            1 => true,
            // mean a logical field
            2 | 3 => return Ok(FieldParseResult::Logical(Self::byte_size_len(name.len()))),
            a => return Err(DecoderError::WrongFieldDataFlag(a)),
        };

        let after_name = 1 + name.len() + 1;

        let t_oid = u32::from_be_bytes(
            data[after_name..after_name + 4]
                .try_into()
                .map_err(|_| DecoderError::TruncatedInput)?,
        );

        println!(
            "data: {:?}, oid {}",
            &data[after_name..after_name + 4],
            t_oid
        );
        let t_mod = u32::from_be_bytes(
            data[after_name + 4..after_name + 8]
                .try_into()
                .map_err(|_| DecoderError::TruncatedInput)?,
        );

        Ok(FieldParseResult::Physical(Field {
            is_key,
            name,
            kind: FieldKind::from_oid_mod(t_oid, t_mod)?,
        }))
    }

    fn byte_size_len(str_len: usize) -> usize {
        10 + str_len
    }

    fn byte_size(&self) -> usize {
        Self::byte_size_len(self.name.len())
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum FieldKind {
    Int4,
    Text,
}

impl FieldKind {
    fn from_oid_mod(t_oid: u32, t_mod: u32) -> Result<Self, DecoderError> {
        match t_oid {
            23 => Ok(Self::Int4),
            25 => Ok(Self::Text),
            a => Err(DecoderError::InvalidOid(a)),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::decoder::relation::{Field, FieldKind, KeyField, Relation};

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
        let relation = Relation::parse(data);

        let relation_manual = Relation {
            relation_oid: 1,
            namespace: "public".to_string(),
            relname: "users".to_string(),
            replica_id: 0,
            fields: vec![field_id()],
            key_fields: vec![key_field_id()],
        };

        assert_eq!(relation, Some(relation_manual));
    }

    #[test]
    fn simple_field() {
        let data = [
            1, // Flags: Is key
            b'i', b'd', 0, //Name of the column: id
            0, 0, 0, 23, // Type oid (int4)
            0, 0, 0, 0, // Attrmod
        ];
        let field = Field::parse(&data);

        assert_eq!(field, Ok(field_id()));
    }
}
