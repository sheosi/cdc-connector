use bumpalo::{Bump, collections::Vec};
use cdc_avro::{PgValue, RowEntry};

use crate::decoder::{
    DecoderError,
    relation::{Field, Relation},
};

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

        last_pos += len;

        cols.push(RowEntry {
            key: &r.name,
            value,
        });
    }

    Ok((cols, last_pos))
}

pub fn parse_keys<'a>(
    data: &'a [u8],
    arena: &'a Bump,
    relation: &'a Relation,
) -> Result<(Vec<'a, PgValue<'a>>, usize), DecoderError> {
    // Network byte order is be
    let n_cols = u16::from_be_bytes(
        data[0..2]
            .try_into()
            .map_err(|_| DecoderError::TruncatedInput)?,
    );

    let mut next_pos = 2;
    let mut cols = Vec::with_capacity_in(n_cols as usize, arena);

    for (_, r) in (0..n_cols).zip(relation.fields.iter()) {
        let (value, len) = parse_value(&data[next_pos..], &r)?;

        next_pos += len;
        cols.push(value);
    }

    Ok((cols, next_pos))
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

#[cfg(test)]
mod test {
    use bumpalo::{Bump, vec};
    use cdc_avro::PgValue;

    use crate::decoder::{
        common::{
            col_byte_id, col_text_name, get_example_rel, get_new_tuple_data, get_old_tuple_data,
        },
        relation,
    };

    use super::{parse, parse_keys};

    #[test]
    fn empty_data() {
        let data = [0, 0];

        let arena = Bump::new();

        let relation = get_example_rel();

        let tuple_data = parse(&data, &arena, &relation);
        let tuple_data_manual = vec![in &arena;];

        assert_eq!(tuple_data, Ok((tuple_data_manual, 2)));
    }

    #[test]
    fn one_byte_data() {
        let data = [0, 1, b'b', 0, 0, 0, 4, 0, 0, 0, 1];

        let arena = Bump::new();

        let relation = get_example_rel();

        let tuple_data = parse(&data, &arena, &relation);
        let tuple_data_manual = vec![in &arena; col_byte_id()];

        assert_eq!(tuple_data, Ok((tuple_data_manual, 11)));
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

        let relation = &get_example_rel();

        let tuple_data = parse(&data, &arena, &relation);
        let tuple_data_manual = vec![in &arena; col_byte_id(), col_text_name()];

        assert_eq!(tuple_data, Ok((tuple_data_manual, 21)));
    }
}
