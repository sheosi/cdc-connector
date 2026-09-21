use crate::decoder::DecoderError;
use bumpalo::{Bump, collections::Vec};
use cdc_avro::{FieldAccess, FieldKind, PgValue};

pub fn parse<'a, F: FieldAccess>(
    data: &[u8],
    arena: &'a Bump,
    fields: &'a [F],
) -> Result<(Vec<'a, PgValue>, usize), DecoderError> {
    #[inline(always)]
    fn num_cols<'a, const N: usize, F: FieldAccess>(
        data: &[u8],
        fields: &'a [F],
        arena: &'a Bump,
    ) -> Result<(Vec<'a, PgValue>, usize), DecoderError> {
        let mut last_pos = 2;
        let mut cols = Vec::with_capacity_in(N, arena);

        for i in 0..N {
            let (value, len) = parse_value(&data[last_pos..], &fields[i])?;

            last_pos += len;

            cols.push(value);
        }

        Ok((cols, last_pos))
    }

    // Network byte order is be
    let n_cols = u16::from_be_bytes(
        data[0..2]
            .try_into()
            .map_err(|_| DecoderError::TruncatedInput)?,
    );

    match n_cols {
        1 => num_cols::<1, F>(data, fields, arena),
        2 => num_cols::<2, F>(data, fields, arena),
        3 => num_cols::<3, F>(data, fields, arena),
        4 => num_cols::<4, F>(data, fields, arena),
        5 => num_cols::<5, F>(data, fields, arena),
        6 => num_cols::<6, F>(data, fields, arena),
        _ => {
            let mut last_pos = 2;
            let mut cols = Vec::with_capacity_in(n_cols as usize, arena);

            for i in 0..n_cols {
                let (value, len) = parse_value(&data[last_pos..], &fields[i as usize])?;

                last_pos += len;

                cols.push(value);
            }

            Ok((cols, last_pos))
        }
    }
}

// Same but with keyfield instead of field
pub fn parse_keys<'a, F: FieldAccess>(
    data: &'a [u8],
    arena: &'a Bump,
    fields: &'a [F],
) -> Result<(Vec<'a, PgValue>, usize), DecoderError> {
    #[inline(always)]
    fn num_cols<'a, const N: usize, F: FieldAccess>(
        data: &'a [u8],
        fields: &'a [F],
        arena: &'a Bump,
    ) -> Result<(Vec<'a, PgValue>, usize), DecoderError> {
        let mut last_pos = 2;
        let mut cols = Vec::with_capacity_in(N, arena);

        for i in 0..N {
            let (value, len) = parse_value(&data[last_pos..], &fields[i])?;

            last_pos += len;

            cols.push(value);
        }

        Ok((cols, last_pos))
    }

    // Network byte order is be
    let n_cols = u16::from_be_bytes(
        data[0..2]
            .try_into()
            .map_err(|_| DecoderError::TruncatedInput)?,
    );

    match n_cols {
        1 => num_cols::<1, F>(data, fields, arena),
        2 => num_cols::<2, F>(data, fields, arena),
        3 => num_cols::<3, F>(data, fields, arena),
        4 => num_cols::<4, F>(data, fields, arena),
        5 => num_cols::<5, F>(data, fields, arena),
        6 => num_cols::<6, F>(data, fields, arena),
        _ => {
            let mut last_pos = 2;
            let mut cols = Vec::with_capacity_in(n_cols as usize, arena);

            for i in 0..n_cols {
                let (value, len) =
                    parse_value(&bytes::Bytes::copy_from_slice(&data), &fields[i as usize])?;

                last_pos += len;

                cols.push(value);
            }

            Ok((cols, last_pos))
        }
    }
}

fn parse_value<'a, F: FieldAccess>(
    data: &'a [u8],
    field: &F,
) -> Result<(PgValue, usize), DecoderError> {
    match data[0] {
        b'n' => todo!(), //Ok((TupleCol::Null, 1)),
        b'u' => todo!(), //Ok((TupleCol::Toasted, 1)),
        b't' => {
            let l_end = 5;
            // Here's be because we are using network endianness (always big endian)
            let l = u32::from_be_bytes(
                data[1..l_end]
                    .try_into()
                    .map_err(|_| DecoderError::TruncatedInput)?,
            ) as usize;

            let final_l = 5 + l;
            if data.len() < final_l {
                return Err(DecoderError::TruncatedInput);
            }

            // Just using simdutf8 to check that the data is actually utf8
            let a = simdutf8::basic::from_utf8(&data[l_end..final_l])?;
            let text = PgValue::Text {
                ptr: a.as_ptr(),
                len: a.len(),
            };
            Ok((text, final_l))
        }
        b'b' => {
            let l_end = 5;
            let l = u32::from_be_bytes(
                data[1..l_end]
                    .try_into()
                    .map_err(|_| DecoderError::TruncatedInput)?,
            ) as usize;

            let final_l = 5 + l;

            let bytes = match field.get_kind() {
                FieldKind::Int4 => {
                    if l != 4 {
                        return Err(DecoderError::WrongFieldKind(FieldKind::Int4));
                    }
                    PgValue::Int4(u32::from_be_bytes(
                        data[l_end..l_end + 4]
                            .try_into()
                            .map_err(|_| DecoderError::TruncatedInput)?,
                    ))
                }
                FieldKind::Text => todo!(),
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

    use super::parse;

    #[test]
    fn empty_data() {
        let data = bytes::Bytes::from_static(&[0, 0]);

        let arena = Bump::new();

        let relation = get_example_rel(&arena);

        let tuple_data = parse(&data, &arena, &relation.fields);
        let tuple_data_manual = vec![in &arena;];

        assert_eq!(tuple_data, Ok((tuple_data_manual, 2)));
    }

    #[test]
    fn one_byte_data() {
        let data = [0, 1, b'b', 0, 0, 0, 4, 0, 0, 0, 1];

        let arena = Bump::new();

        let relation = get_example_rel(&arena);

        let tuple_data = parse(&data, &arena, &relation.fields);
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

        let relation = &get_example_rel(&arena);

        let tuple_data = parse(&data, &arena, &relation.fields);
        let tuple_data_manual = vec![in &arena; col_byte_id(), col_text_name()];

        assert_eq!(tuple_data, Ok((tuple_data_manual, 21)));
    }
}
