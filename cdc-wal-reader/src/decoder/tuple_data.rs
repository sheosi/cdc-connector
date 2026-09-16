use std::collections::HashMap;

use bytes::Bytes;
use cdc_avro::PgValue;

use crate::decoder::{
    DecoderError,
    relation::{Field, KeyField, Relation},
};

#[derive(Debug, PartialEq)]
pub struct TupleData<'a> {
    pub(crate) cols: Vec<TupleCol<'a>>,
}

impl<'a> TupleData<'a> {
    pub fn parse(data: &[u8]) -> Result<(TupleData, usize), DecoderError> {
        // Network byte order is be
        let n_cols = u16::from_be_bytes(
            data[0..2]
                .try_into()
                .map_err(|_| DecoderError::TruncatedInput)?,
        );

        let mut last_pos = 2;
        let mut cols = Vec::with_capacity(n_cols as usize);

        for _ in 0..n_cols {
            match TupleCol::parse(&data[last_pos..]) {
                Ok(col) => {
                    last_pos += col.byte_size() + 1;
                    cols.push(col);
                }
                Err(e) => return Err(e),
            }
        }

        Ok((TupleData { cols }, last_pos))
    }

    pub fn into_row<'b>(
        self,
        relation: &'b Relation,
    ) -> Result<HashMap<&'b str, PgValue<'a>>, DecoderError> {
        Ok(self
            .cols
            .into_iter()
            .zip(relation.fields.iter())
            .map(|(d, r)| d.to_pg_value(&r).map(|pg| (r.name.as_str(), pg)))
            .collect::<Result<HashMap<_, _>, _>>()?)
    }

    pub fn into_keys(self, relation: &Relation) -> Result<Vec<PgValue<'a>>, DecoderError> {
        Ok(self
            .cols
            .into_iter()
            .zip(relation.key_fields.iter())
            .map(|(c, r)| c.to_pg_value_kf(r))
            .collect::<Result<Vec<_>, _>>()?)
    }
}

#[derive(Debug, PartialEq)]
pub enum TupleCol<'a> {
    Null,
    Toasted,
    Text(&'a str),
    Bytes(Bytes),
}

impl<'a> TupleCol<'a> {
    fn parse(data: &[u8]) -> Result<TupleCol, DecoderError> {
        match data[0] {
            b'n' => Ok(TupleCol::Null),
            b'u' => Ok(TupleCol::Toasted),
            b't' => {
                // Here's be because we are using network endianness (always big endian)
                let l = u32::from_be_bytes(
                    data[1..5]
                        .try_into()
                        .map_err(|_| DecoderError::TruncatedInput)?,
                ) as usize;

                if data.len() < 5 + l {
                    return Err(DecoderError::TruncatedInput);
                }

                Ok(TupleCol::Text(str::from_utf8(&data[5..5 + l])?))
            }
            b'b' => {
                let l = u32::from_be_bytes(
                    data[1..5]
                        .try_into()
                        .map_err(|_| DecoderError::TruncatedInput)?,
                );
                Ok(TupleCol::Bytes(Bytes::copy_from_slice(
                    &data[5..5 + (l as usize)],
                )))
            }
            a => Err(DecoderError::WrongColTypeKey(a)),
        }
    }

    fn byte_size(&self) -> usize {
        let inner = match self {
            TupleCol::Bytes(b) => b.len(),
            TupleCol::Text(t) => {
                if !t.is_empty() {
                    t.len()
                } else {
                    0
                }
            }
            _ => 0,
        };

        4 + inner
    }

    fn to_pg_value(self, rel_field: &Field) -> Result<PgValue<'a>, DecoderError> {
        match self {
            TupleCol::Null => todo!(),
            TupleCol::Toasted => todo!(),
            TupleCol::Text(s) => Ok(PgValue::Text(s)),
            TupleCol::Bytes(b) => match rel_field.kind {
                super::relation::FieldKind::Int4 => Ok(PgValue::Int4(u32::from_be_bytes(
                    b[0..4]
                        .try_into()
                        .map_err(|_| DecoderError::TruncatedInput)?,
                ))),
                a => Err(DecoderError::WrongFieldKind(a)),
            },
        }
    }

    fn to_pg_value_kf(self, rel_field: &KeyField) -> Result<PgValue<'a>, DecoderError> {
        match self {
            TupleCol::Null => todo!(),
            TupleCol::Toasted => todo!(),
            TupleCol::Text(s) => Ok(PgValue::Text(s)),
            TupleCol::Bytes(b) => match rel_field.kind {
                super::relation::FieldKind::Int4 => Ok(PgValue::Int4(u32::from_be_bytes(
                    b[0..4]
                        .try_into()
                        .map_err(|_| DecoderError::TruncatedInput)?,
                ))),
                a => Err(DecoderError::WrongFieldKind(a)),
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

        assert_eq!(tuple_data, Ok((tuple_data_manual, 2)));
    }

    #[test]
    fn one_byte_data() {
        let data = [0, 1, b'b', 0, 0, 0, 4, 0, 0, 0, 1];

        let tuple_data = TupleData::parse(&data);
        let tuple_data_manual = TupleData {
            cols: vec![col_byte_one()],
        };

        assert_eq!(tuple_data, Ok((tuple_data_manual, 11)));
    }

    #[test]
    fn one_text_col() {
        let data = [b't', 0, 0, 0, 5, b'h', b'e', b'l', b'l', b'o'];

        let tuple_col = TupleCol::parse(&data);

        assert_eq!(tuple_col, Ok(col_text_hello()));
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

        assert_eq!(tuple_data, Ok((tuple_data_manual, 21)));
    }
}
