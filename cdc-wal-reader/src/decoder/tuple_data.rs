use std::collections::HashMap;

use bytes::Bytes;
use cdc_avro::PgValue;

use crate::decoder::relation::{Field, Relation};

#[derive(Debug, PartialEq)]
pub struct TupleData {
    pub(crate) cols: Vec<TupleCol>,
}

impl TupleData {
    pub fn parse(data: &[u8]) -> TupleData {
        // Network byte order is be
        let n_cols = u16::from_be_bytes(data[0..2].try_into().unwrap());

        let mut last_pos = 2;
        let mut cols = Vec::with_capacity(n_cols as usize);

        for _ in 0..n_cols {
            let col = TupleCol::parse(&data[last_pos..]);
            last_pos += col.byte_size() + 1;
            cols.push(col);
        }

        TupleData { cols }
    }

    pub fn into_row(self, relation: &Relation) -> HashMap<String, PgValue> {
        self.cols
            .into_iter()
            .zip(relation.fields.iter())
            .map(|(d, r)| (r.name.clone(), d.to_pg_value(&r)))
            .collect()
    }
}

#[derive(Debug, PartialEq)]
pub enum TupleCol {
    Null,
    Toasted,
    Text(String),
    Bytes(Bytes),
}

impl TupleCol {
    fn parse(data: &[u8]) -> TupleCol {
        match data[0] {
            b'n' => TupleCol::Null,
            b'u' => TupleCol::Toasted,
            b't' => {
                // Here's be because we are using network endianness (always big endian)
                let l = u32::from_be_bytes(data[1..5].try_into().unwrap());
                TupleCol::Text(
                    str::from_utf8(&data[5..5 + (l as usize)])
                        .unwrap()
                        .to_string(),
                )
            }
            b'b' => {
                let l = u32::from_be_bytes(data[1..5].try_into().unwrap());
                TupleCol::Bytes(Bytes::copy_from_slice(&data[5..5 + (l as usize)]))
            }
            a => {
                println!("{}", a);
                panic!("Wrong letter in column")
            }
        }
    }

    fn byte_size(&self) -> usize {
        let inner = match self {
            TupleCol::Bytes(b) => b.len(),
            TupleCol::Text(t) => {
                if !t.is_empty() {
                    t.len() + 1
                } else {
                    0
                }
            }
            _ => 0,
        };

        4 + inner
    }

    fn to_pg_value(self, rel_field: &Field) -> PgValue {
        match self {
            TupleCol::Null => todo!(),
            TupleCol::Toasted => todo!(),
            TupleCol::Text(s) => PgValue::Text(s),
            TupleCol::Bytes(b) => match rel_field.kind {
                super::relation::FieldKind::Int4 => {
                    PgValue::Int4(u32::from_be_bytes(b[0..4].try_into().unwrap()))
                }
                _ => panic!("Wrong kind of field kind"),
            },
        }
    }
}

#[cfg(test)]
mod test {
    use crate::decoder::common::{
        col_byte_one, col_text_hello, get_new_tuple_data, get_old_tuple_data,
    };
    use crate::decoder::tuple_data::{TupleCol, TupleData};

    #[test]
    fn empty_data() {
        let data = [0, 0];

        let tuple_data = TupleData::parse(&data);
        let tuple_data_manual = TupleData { cols: vec![] };

        assert_eq!(tuple_data, tuple_data_manual);
    }

    #[test]
    fn one_byte_data() {
        let data = [0, 1, b'b', 0, 0, 0, 4, 0, 0, 0, 1];

        let tuple_data = TupleData::parse(&data);
        let tuple_data_manual = TupleData {
            cols: vec![col_byte_one()],
        };

        assert_eq!(tuple_data, tuple_data_manual);
    }

    #[test]
    fn one_text_col() {
        let data = [b't', 0, 0, 0, 5, b'h', b'e', b'l', b'l', b'o'];

        let tuple_col = TupleCol::parse(&data);

        assert_eq!(tuple_col, col_text_hello());
    }

    #[test]
    fn byte_and_text_data() {
        let data = [
            0, 2, // Two columns
            b'b', 0, 0, 0, 4, // Col 1: Binary 4 bytes
            0, 0, 0, 1, // Int4: 1
            b't', 0, 0, 0, 5, // Col 2: Text 5 bytes
            b'h', b'e', b'l', b'l', b'o', // Text: hello
        ];

        let tuple_data = TupleData::parse(&data);
        let tuple_data_manual = TupleData {
            cols: vec![col_byte_one(), col_text_hello()],
        };

        assert_eq!(tuple_data, tuple_data_manual);
    }
}
