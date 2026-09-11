use bytes::Bytes;
use std::ffi::CStr;

#[derive(Debug, PartialEq)]
pub struct Relation {
    pub relation_oid: u32,
    pub namespace: String,
    pub relname: String,
    pub replica_id: u8,
    pub fields: Vec<Field>,
}

impl Relation {
    pub fn parse(data: Bytes) -> Option<Relation> {
        let relation_oid = u32::from_be_bytes(data[1..5].try_into().unwrap());
        let namespace = CStr::from_bytes_until_nul(&data[5..])
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let relname = CStr::from_bytes_until_nul(&data[5 + namespace.len() + 1..])
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let replica_id_pos = 5 + namespace.len() + 1 + relname.len() + 1;
        let replica_id = data[replica_id_pos];

        let cols = u16::from_be_bytes(
            data[replica_id_pos + 1..replica_id_pos + 3]
                .try_into()
                .unwrap(),
        );

        let mut col_start_id = replica_id_pos + 3;
        let mut fields = Vec::with_capacity(cols as usize);

        for _ in 0..cols {
            match Field::parse(&data[col_start_id..]) {
                Ok(field) => {
                    col_start_id += field.byte_size();
                    fields.push(field);
                }
                Err(b) => {
                    col_start_id += b;
                }
            }
        }

        Some(Relation {
            relation_oid,
            namespace,
            relname,
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

impl Field {
    /// The Field might be logical, and thus not present, in those cases we don't
    /// store it, but we need the size of it
    fn parse(data: &[u8]) -> Result<Field, usize> {
        let flag = data[0];
        let name = CStr::from_bytes_until_nul(&data[1..])
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let is_key = match flag {
            0 => false,
            1 => true,
            // mean a logical field
            2 | 3 => return Err(Self::byte_size_len(name.len())),
            _ => panic!("Wrong flag"),
        };

        let after_name = 1 + name.len() + 1;

        let t_oid = u32::from_be_bytes(data[after_name..after_name + 4].try_into().unwrap());

        println!(
            "data: {:?}, oid {}",
            &data[after_name..after_name + 4],
            t_oid
        );
        let t_mod = u32::from_be_bytes(data[after_name + 4..after_name + 8].try_into().unwrap());

        Ok(Field {
            is_key,
            name,
            kind: FieldKind::from_oid_mod(t_oid, t_mod),
        })
    }

    fn byte_size_len(str_len: usize) -> usize {
        10 + str_len
    }

    fn byte_size(&self) -> usize {
        Self::byte_size_len(self.name.len())
    }
}

#[derive(Debug, PartialEq)]
pub enum FieldKind {
    Int4,
    Text,
}

impl FieldKind {
    fn from_oid_mod(t_oid: u32, t_mod: u32) -> Self {
        match t_oid {
            23 => Self::Int4,
            25 => Self::Text,
            a => panic!("Invalid OID: {}", a),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::decoder::relation::{Field, FieldKind, Relation};

    fn field_id() -> Field {
        Field {
            is_key: true,
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
