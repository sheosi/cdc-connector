use bytes::Bytes;
use cdc_avro::{Field, FieldAccess, FieldKind, Relation, ReplicaKind};
use simdutf8::basic::from_utf8 as simd_from_utf8;

use crate::decoder::DecoderError;

fn extract_string<'a>(data: &[u8]) -> Result<String, DecoderError> {
    let nul = find_null_word(data).ok_or(DecoderError::TruncatedInput)?;

    // Fast path: usually postgres identifiers are pure ascii
    if data[..nul].iter().all(|&b| b < 128) {
        let mut s = Vec::with_capacity(nul);
        s.extend_from_slice(&data[..nul]);

        // This is fine, we already checked these were all ASCII
        Ok(unsafe { String::from_utf8_unchecked(s) })
    } else {
        // Rare: quoted Unicode identifier, validate properly
        Ok(simd_from_utf8(&data[..nul])?.to_string())
    }
}

#[derive(Debug, PartialEq)]
pub struct RelationData {
    pub inner: Relation,

    /// A subset of above, they are the fields marked as keys, for when only keys
    /// are searched for
    pub key_fields: Vec<KeyField>,
}

impl RelationData {
    pub fn parse(data: Bytes) -> Result<RelationData, DecoderError> {
        // 'R' (1B) + rel oid (4B) + namespace end (1B) + relname end (1B)
        // + repl_id (1B) + num_cols (2B)
        if data.len() < 10 {
            return Err(DecoderError::TruncatedInput);
        }

        let relation_oid = u32::from_be_bytes(data[1..5].try_into().expect(""));

        let namespace = extract_string(&data[5..])?;
        let relname = extract_string(&data[5 + namespace.len() + 1..])?;

        // Check for both replica id an numbers of cols
        if data.len() < 5 + namespace.len() + 1 + relname.len() + 3 {
            return Err(DecoderError::TruncatedInput);
        }

        let replica_id_pos = 5 + namespace.len() + 1 + relname.len() + 1;
        let replica_id = match data[replica_id_pos] {
            0 => ReplicaKind::Keys,
            2 => ReplicaKind::Row,
            a => return Err(DecoderError::WrongReplicaId(a)),
        };

        let cols = u16::from_be_bytes(
            data[replica_id_pos + 1..replica_id_pos + 3]
                .try_into()
                .expect(""),
        );

        let mut col_start_id = replica_id_pos + 3;
        let mut fields = Vec::with_capacity(cols as usize);

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
                .collect(),

            inner: Relation {
                relation_oid,
                namespace,
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
    // The minimum: flag (1B) + string_end (1B) + oid (4B) + mod (4B)
    //
    if data.len() < 10 {
        return Err(DecoderError::TruncatedInput);
    }

    let flag = data[0];
    let name = extract_string(&data[1..])?;

    let l_name = name.len();

    let is_key = match flag {
        0 => false,
        1 => true,
        // mean a logical field
        2 | 3 => return Ok(FieldParseResult::Logical(byte_size_len(l_name))),
        a => return Err(DecoderError::WrongFieldDataFlag(a)),
    };

    let after_name = 1 + name.len() + 1;

    let t_oid = u32::from_be_bytes(data[after_name..after_name + 4].try_into().expect(""));

    let t_mod = u32::from_be_bytes(data[after_name + 4..after_name + 8].try_into().expect(""));

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

fn find_null_word(data: &[u8]) -> Option<usize> {
    use std::arch::x86_64::{
        __m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_setzero_si128,
    };
    use std::cmp::min;

    let len = data.len();

    // Fast path: strings shorter than 16 bytes
    if len < 16 {
        let mut buf = [0xFFu8; 16];
        buf[..len].copy_from_slice(&data[..len]);
        unsafe {
            let chunk = _mm_loadu_si128(buf.as_ptr() as *const __m128i);
            let mask = _mm_movemask_epi8(_mm_cmpeq_epi8(chunk, _mm_setzero_si128()));
            if mask != 0 {
                return Some(mask.trailing_zeros() as usize);
            }
        }
        return None;
    }

    unsafe {
        let ptr = data.as_ptr() as *const __m128i;
        let chunk = _mm_loadu_si128(ptr); // movdqu

        let zeros = _mm_setzero_si128();
        let cmp = _mm_cmpeq_epi8(chunk, zeros); // 0xFF where byte == 0
        let mask = _mm_movemask_epi8(cmp); // 16-bit mask

        if mask != 0 {
            return Some(mask.trailing_zeros() as usize);
        }
    }

    // General case: strings longer than 16 bytes
    let mut i = 24; // This i is the last_position
    while i <= len + 7 {
        let boundary = min(i, len);
        let word = u64::from_ne_bytes(data[boundary - 8..boundary].try_into().expect(""));
        let mask = word.wrapping_sub(0x0101010101010101) & !word & 0x8080808080808080;

        if mask != 0 {
            return Some((boundary - 8) + (mask.trailing_zeros() / 8) as usize);
        }
        i += 8;
    }

    None
}

#[cfg(test)]
mod test {
    use bumpalo::Bump;
    use bytes::Bytes;

    use crate::decoder::{
        DecoderError,
        relation::{
            Field, FieldKind, FieldParseResult, KeyField, Relation, RelationData, ReplicaKind,
            parse_field,
        },
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
        let data = Bytes::from_static(&[
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

        let relation = RelationData::parse(data);

        let relation_manual = RelationData {
            key_fields: vec![key_field_id()],
            inner: Relation {
                relation_oid: 1,
                namespace: "public".to_string(),
                name: "users".to_string(),
                replica_id: ReplicaKind::Keys,
                fields: vec![field_id()],
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

    #[test]
    fn no_relation() {
        let data = Bytes::from_static(&[b'R']);

        let relation = RelationData::parse(data);

        assert_eq!(relation, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn no_namespace() {
        let data = Bytes::from_static(&[b'R', 0, 0, 0, 0]);

        let relation = RelationData::parse(data);

        assert_eq!(relation, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn no_relname() {
        let data = Bytes::from_static(&[
            b'R', // Relation
            0, 0, 0, 1, // Relation OID
            b'p', b'u', b'b', b'l', b'i', b'c', 0, // Namespace
        ]);

        let relation = RelationData::parse(data);

        assert_eq!(relation, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn namespace_unfinished() {
        let data = Bytes::from_static(&[
            b'R', // Relation
            0, 0, 0, 1, // Relation OID
            b'p', b'u', b'b', b'l', b'i', b'c', // Namespace
        ]);

        let relation = RelationData::parse(data);

        assert_eq!(relation, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn relname_unfinished() {
        let data = Bytes::from_static(&[
            b'R', // Relation
            0, 0, 0, 1, // Relation OID
            b'p', b'u', b'b', b'l', b'i', b'c', 0, // Namespace
            b'u', b's', b'e', b'r', b's', // Relation name
        ]);

        let relation = RelationData::parse(data);

        assert_eq!(relation, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn no_cols() {
        let data = Bytes::from_static(&[
            b'R', // Relation
            0, 0, 0, 1, // Relation OID
            b'p', b'u', b'b', b'l', b'i', b'c', 0, // Namespace
            b'u', b's', b'e', b'r', b's', 0, // Relation name
            0, // Replica identity setting
        ]);

        let relation = RelationData::parse(data);

        assert_eq!(relation, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn wrong_cols() {
        let data = Bytes::from_static(&[
            b'R', // Relation
            0, 0, 0, 1, // Relation OID
            b'p', b'u', b'b', b'l', b'i', b'c', 0, // Namespace
            b'u', b's', b'e', b'r', b's', 0, // Relation name
            0, // Replica identity setting
            0, 1, // Number of columns
        ]);

        let relation = RelationData::parse(data);

        assert_eq!(relation, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn no_colname() {
        let data = Bytes::from_static(&[
            b'R', // Relation
            0, 0, 0, 1, // Relation OID
            b'p', b'u', b'b', b'l', b'i', b'c', 0, // Namespace
            b'u', b's', b'e', b'r', b's', 0, // Relation name
            0, // Replica identity setting
            0, 1, // Number of columns
            // Column 1
            1, // Flags: Is key
        ]);

        let relation = RelationData::parse(data);

        assert_eq!(relation, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn colname_unfinished() {
        let data = Bytes::from_static(&[
            b'R', // Relation
            0, 0, 0, 1, // Relation OID
            b'p', b'u', b'b', b'l', b'i', b'c', 0, // Namespace
            b'u', b's', b'e', b'r', b's', 0, // Relation name
            0, // Replica identity setting
            0, 1, // Number of columns
            // Column 1
            1, // Flags: Is key
            b'i', b'd',
        ]);

        let relation = RelationData::parse(data);

        assert_eq!(relation, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn no_type_oid() {
        let data = Bytes::from_static(&[
            b'R', // Relation
            0, 0, 0, 1, // Relation OID
            b'p', b'u', b'b', b'l', b'i', b'c', 0, // Namespace
            b'u', b's', b'e', b'r', b's', 0, // Relation name
            0, // Replica identity setting
            0, 1, // Number of columns
            // Column 1
            1, // Flags: Is key
            b'i', b'd', 0, // Name of the column: id
        ]);

        let relation = RelationData::parse(data);

        assert_eq!(relation, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn no_attrmod() {
        let data = Bytes::from_static(&[
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
        ]);

        let relation = RelationData::parse(data);

        assert_eq!(relation, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn wrong_type_oid() {
        let data = Bytes::from_static(&[
            b'R', // Relation
            0, 0, 0, 1, // Relation OID
            b'p', b'u', b'b', b'l', b'i', b'c', 0, // Namespace
            b'u', b's', b'e', b'r', b's', 0, // Relation name
            0, // Replica identity setting
            0, 1, // Number of columns
            // Column 1
            1, // Flags: Is key
            b'i', b'd', 0, // Name of the column: id
            0, 0, 0, 0, // WRONG! Doesn't exist
            0, 0, 0, 0, // attrmod
        ]);

        let relation = RelationData::parse(data);

        assert_eq!(relation, Err(DecoderError::InvalidOid(0)));
    }
}
