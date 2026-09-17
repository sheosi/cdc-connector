use bumpalo::{
    Bump,
    collections::{CollectIn, Vec},
};
use cdc_avro::{PgValue, RowEntry};

use crate::decoder::{
    DecoderError,
    relation::{Field, KeyField, Relation},
};

#[derive(Debug, PartialEq)]
pub struct TupleData<'a> {
    pub(crate) cols: Vec<'a, TupleCol<'a>>,
}

pub fn parse<'a>(
    data: &'a [u8],
    arena: &'a Bump,
    relation: &'a Relation,
) -> Result<(Vec<'a, RowEntry<'a>>, usize), DecoderError> {
    // Network byte order is be
    let n_cols = u16::from_be_bytes(
        data[0..2]
            .try_into()
            .map_err(|_| DecoderError::TruncatedInput)?,
    );

    let mut last_pos = 2;
    let mut cols = Vec::with_capacity_in(n_cols as usize, arena);

    for (_, r) in (0..n_cols).zip(relation.fields.iter()) {
        let (value, len) = parse_value(&data[last_pos..], &r)?;

        last_pos += len + 1;
        cols.push(RowEntry {
            key: &r.name,
            value,
        });
    }

    Ok((cols, last_pos))
}

impl<'a> TupleData<'a> {
    pub fn parse(data: &'a [u8], arena: &'a Bump) -> Result<(TupleData<'a>, usize), DecoderError> {
        // Network byte order is be
        let n_cols = u16::from_be_bytes(
            data[0..2]
                .try_into()
                .map_err(|_| DecoderError::TruncatedInput)?,
        );

        let mut last_pos = 2;
        let mut cols = Vec::with_capacity_in(n_cols as usize, arena);

        for _ in 0..n_cols {
            match TupleCol::parse(&data[last_pos..]) {
                Ok((col, len)) => {
                    last_pos += len + 1;
                    cols.push(col);
                }
                Err(e) => return Err(e),
            }
        }

        Ok((TupleData { cols }, last_pos))
    }

    pub fn into_row(
        self,
        relation: &'a Relation,
        arena: &'a Bump,
    ) -> Result<Vec<'a, RowEntry<'a>>, DecoderError> {
        Ok(self
            .cols
            .into_iter()
            .zip(relation.fields.iter())
            .map(|(d, r)| {
                d.to_pg_value(&r).map(|pg| RowEntry {
                    key: r.name.as_str(),
                    value: pg,
                })
            })
            .collect_in::<Result<Vec<'a, _>, _>>(arena)?)
    }

    pub fn into_keys(
        self,
        relation: &Relation,
        arena: &'a Bump,
    ) -> Result<Vec<'a, PgValue<'a>>, DecoderError> {
        Ok(self
            .cols
            .into_iter()
            .zip(relation.key_fields.iter())
            .map(|(c, r)| c.to_pg_value_kf(r))
            .collect_in::<Result<Vec<'a, _>, _>>(arena)?)
    }
}

fn parse_value<'a>(data: &'a [u8], field: &Field) -> Result<(PgValue<'a>, usize), DecoderError> {
    match data[0] {
        b'n' => todo!(), //Ok((TupleCol::Null, 1)),
        b'u' => todo!(), //Ok((TupleCol::Toasted, 1)),
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

            let final_l = 5 + l;
            let text = PgValue::Text(simdutf8::basic::from_utf8(&data[5..final_l])?);
            Ok((text, final_l))
        }
        b'b' => {
            let l = u32::from_be_bytes(
                data[1..5]
                    .try_into()
                    .map_err(|_| DecoderError::TruncatedInput)?,
            ) as usize;

            let final_l = 5 + l;

            let bytes = match field.kind {
                super::relation::FieldKind::Int4 => {
                    if l != 4 {
                        return Err(DecoderError::WrongFieldKind(
                            super::relation::FieldKind::Int4,
                        ));
                    }
                    PgValue::Int4(u32::from_be_bytes(
                        data[5..9]
                            .try_into()
                            .map_err(|_| DecoderError::TruncatedInput)?,
                    ))
                }
                super::relation::FieldKind::Text => todo!(),
            };

            Ok((bytes, final_l))
        }
        a => Err(DecoderError::WrongColTypeKey(a)),
    }
}

#[derive(Debug, PartialEq)]
pub enum TupleCol<'a> {
    Null,
    Toasted,
    Text(&'a str),
    Bytes(&'a [u8]),
}

impl<'a> TupleCol<'a> {
    fn parse(data: &'a [u8]) -> Result<(TupleCol<'a>, usize), DecoderError> {
        match data[0] {
            b'n' => Ok((TupleCol::Null, 1)),
            b'u' => Ok((TupleCol::Toasted, 1)),
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

                let final_l = 5 + l;
                let text = TupleCol::Text(simdutf8::basic::from_utf8(&data[5..final_l])?);
                Ok((text, final_l))
            }
            b'b' => {
                let l = u32::from_be_bytes(
                    data[1..5]
                        .try_into()
                        .map_err(|_| DecoderError::TruncatedInput)?,
                ) as usize;

                let final_l = 5 + l;
                let bytes = TupleCol::Bytes(&data[5..final_l]);

                Ok((bytes, final_l))
            }
            a => Err(DecoderError::WrongColTypeKey(a)),
        }
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
    use bumpalo::{Bump, vec};

    use crate::decoder::common::{
        col_byte_one, col_text_hello, get_new_tuple_data, get_old_tuple_data,
    };
    use crate::decoder::tuple_data::{TupleCol, TupleData};

    #[test]
    fn empty_data() {
        let data = [0, 0];

        let arena = Bump::new();

        let tuple_data = TupleData::parse(&data, &arena);
        let tuple_data_manual = TupleData {
            cols: vec![in &arena;],
        };

        assert_eq!(tuple_data, Ok((tuple_data_manual, 2)));
    }

    #[test]
    fn one_byte_data() {
        let data = [0, 1, b'b', 0, 0, 0, 4, 0, 0, 0, 1];

        let arena = Bump::new();

        let tuple_data = TupleData::parse(&data, &arena);
        let tuple_data_manual = TupleData {
            cols: vec![in &arena; col_byte_one()],
        };

        assert_eq!(tuple_data, Ok((tuple_data_manual, 11)));
    }

    #[test]
    fn one_text_col() {
        let data = [b't', 0, 0, 0, 5, b'h', b'e', b'l', b'l', b'o'];

        let tuple_col = TupleCol::parse(&data);

        assert_eq!(tuple_col, Ok((col_text_hello(), 10)));
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

        let arena = Bump::new();

        let tuple_data = TupleData::parse(&data, &arena);
        let tuple_data_manual = TupleData {
            cols: vec![in &arena; col_byte_one(), col_text_hello()],
        };

        assert_eq!(tuple_data, Ok((tuple_data_manual, 21)));
    }
}
