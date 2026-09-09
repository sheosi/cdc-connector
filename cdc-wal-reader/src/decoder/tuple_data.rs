use std::collections::HashMap;

use bytes::Bytes;

use crate::decoder::relation::Relation;

#[derive(Debug)]
pub struct TupleData {
    cols: Vec<TupleCol>,
}

impl TupleData {
    pub fn parse(data: &[u8]) -> TupleData {
        // Network byte order is be
        let n_cols = u16::from_be_bytes(data[0..2].try_into().unwrap());

        let mut last_pos = 2;
        let mut cols = Vec::with_capacity(n_cols as usize);

        for _ in 0..n_cols {
            let col = TupleCol::parse(&data[last_pos..]);
            last_pos += col.byte_size();
            cols.push(col);
        }

        TupleData { cols }
    }

    pub fn to_row(&self, relation: &Relation) -> HashMap<String, String> {
        self.cols
            .iter()
            .zip(relation.fields.iter())
            .map(|(d, r)| (r.name.clone(), d.to_string()))
            .collect()
    }
}

#[derive(Debug)]
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
            _ => {
                panic!("Wrong letter")
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

    fn to_string(&self) -> String {
        match self {
            TupleCol::Null => todo!(),
            TupleCol::Toasted => todo!(),
            TupleCol::Text(s) => s.clone(),
            TupleCol::Bytes(_) => todo!(),
        }
    }
}
