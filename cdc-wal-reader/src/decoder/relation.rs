use bytes::Bytes;
use std::ffi::CStr;

#[derive(Debug)]
pub struct Relation {
    pub relation_oid: u32,
    namespace: String,
    relname: String,
    replica_id: String,
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

        let replica_id = CStr::from_bytes_until_nul(&data[replica_id_pos..])
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let cols = u16::from_be_bytes(
            data[replica_id_pos + replica_id.len()..replica_id_pos + replica_id.len() + 2]
                .try_into()
                .unwrap(),
        );

        let mut col_start_id = replica_id_pos + 1 + replica_id.len() + 1;
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

#[derive(Debug)]
pub struct Field {
    is_key: bool,
    pub name: String,
    kind: FieldKind,
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

#[derive(Debug)]
enum FieldKind {
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
    #[test]
    fn simple_relation() {}

    #[test]
    fn simple_field() {}
}
