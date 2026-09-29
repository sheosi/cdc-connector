use crate::decoder::DecoderError;
use bumpalo::{Bump, collections::Vec};
use cdc_avro::{FieldAccess, FieldKind, PgValue};

pub fn parse<'a, F: FieldAccess>(
    data: &'a [u8],
    arena: &'a Bump,
    fields: &'a [F],
) -> Result<(Vec<'a, PgValue<'a>>, usize), DecoderError> {
    #[inline(always)]
    fn num_cols<'a, const N: usize, F: FieldAccess>(
        data: &'a [u8],
        fields: &'a [F],
        arena: &'a Bump,
    ) -> Result<(Vec<'a, PgValue<'a>>, usize), DecoderError> {
        let mut last_pos = 2;
        let mut cols = Vec::with_capacity_in(N, arena);

        for i in 0..N {
            let (value, len) = parse_value(&data[last_pos..], &fields[i])?;

            last_pos += len;

            cols.push(value);
        }

        Ok((cols, last_pos))
    }

    if data.len() < 2 {
        return Err(DecoderError::TruncatedInput);
    }

    // Network byte order is be
    let n_cols = u16::from_be_bytes(data[0..2].try_into().expect(""));

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
) -> Result<(Vec<'a, PgValue<'a>>, usize), DecoderError> {
    #[inline(always)]
    fn num_cols<'a, const N: usize, F: FieldAccess>(
        data: &'a [u8],
        fields: &'a [F],
        arena: &'a Bump,
    ) -> Result<(Vec<'a, PgValue<'a>>, usize), DecoderError> {
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
    let n_cols = u16::from_be_bytes(data[0..2].try_into().expect(""));

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

fn parse_value<'a, F: FieldAccess>(
    data: &'a [u8],
    field: &F,
) -> Result<(PgValue<'a>, usize), DecoderError> {
    if data.len() == 0 {
        return Err(DecoderError::TruncatedInput);
    }

    match data[0] {
        b'n' => todo!(), //Ok((TupleCol::Null, 1)),
        b'u' => todo!(), //Ok((TupleCol::Toasted, 1)),
        b't' => {
            if data.len() < 5 {
                return Err(DecoderError::TruncatedInput);
            }

            // Network endianness (always big endian). Length is guaranteed by the
            // `data.len() < 5` check above.
            let l = u32::from_be_bytes(data[1..5].try_into().expect("")) as usize;

            if data.len() < 5 + l {
                return Err(DecoderError::TruncatedInput);
            }

            let final_l = 5 + l;
            let text = PgValue::Text(simdutf8::basic::from_utf8(&data[5..final_l])?);
            Ok((text, final_l))
        }
        b'b' => {
            if data.len() < 5 {
                return Err(DecoderError::TruncatedInput);
            }

            // Length of the binary data
            let l = u32::from_be_bytes(data[1..5].try_into().expect("")) as usize;

            let final_l = 5 + l;

            let bytes = match field.get_kind() {
                FieldKind::Int4 => {
                    if l != 4 {
                        return Err(DecoderError::WrongFieldKind(FieldKind::Int4));
                    }

                    if data.len() < 9 {
                        return Err(DecoderError::TruncatedInput);
                    }

                    // Length is guaranteed by the `data.len() < 9` check above.
                    PgValue::Int4(u32::from_be_bytes(data[5..9].try_into().expect("")))
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
    use cdc_avro::{FieldKind::Int4, PgValue};

    use crate::decoder::{
        DecoderError,
        common::{
            col_byte_id, col_text_name, get_example_rel, get_new_tuple_data, get_old_tuple_data,
        },
        relation, tuple_data,
    };

    use super::parse;

    #[test]
    fn empty_data() {
        let data = [0, 0];

        let arena = Bump::new();

        let relation = get_example_rel();

        let tuple_data = parse(&data, &arena, &relation.fields);
        let tuple_data_manual = vec![in &arena;];

        assert_eq!(tuple_data, Ok((tuple_data_manual, 2)));
    }

    #[test]
    fn one_byte_data() {
        let data = [0, 1, b'b', 0, 0, 0, 4, 0, 0, 0, 1];

        let arena = Bump::new();

        let relation = get_example_rel();

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

        let relation = &get_example_rel();

        let tuple_data = parse(&data, &arena, &relation.fields);
        let tuple_data_manual = vec![in &arena; col_byte_id(), col_text_name()];

        assert_eq!(tuple_data, Ok((tuple_data_manual, 21)));
    }

    #[test]
    fn truncated_col() {
        let data = [0];

        let arena = Bump::new();

        let relation = &get_example_rel();

        let tuple_data = parse(&data, &arena, &relation.fields);

        assert_eq!(tuple_data, Err(DecoderError::TruncatedInput))
    }

    #[test]
    fn too_many_cols() {
        let data = [0, 1];

        let arena = Bump::new();

        let relation = &get_example_rel();

        let tuple_data = parse(&data, &arena, &relation.fields);

        assert_eq!(tuple_data, Err(DecoderError::TruncatedInput))
    }

    #[test]
    fn wrong_tuple_type() {
        let data = [0, 1, b'c'];

        let arena = Bump::new();

        let relation = &get_example_rel();

        let tuple_data = parse(&data, &arena, &relation.fields);

        assert_eq!(tuple_data, Err(DecoderError::WrongColTypeKey(b'c')));
    }

    #[test]
    fn truncated_t_size() {
        let data = [0, 1, b't'];

        let arena = Bump::new();

        let relation = &get_example_rel();

        let tuple_data = parse(&data, &arena, &relation.fields);

        assert_eq!(tuple_data, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn truncated_t_data() {
        let data = [0, 1, b't', 0, 0, 0, 1];

        let arena = Bump::new();

        let relation = &get_example_rel();

        let tuple_data = parse(&data, &arena, &relation.fields);

        assert_eq!(tuple_data, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn truncated_b_size() {
        let data = [0, 1, b'b'];

        let arena = Bump::new();

        let relation = &get_example_rel();

        let tuple_data = parse(&data, &arena, &relation.fields);

        assert_eq!(tuple_data, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn truncated_b_data() {
        // The example rel says it's, it checks first for the type and then
        // then whether the data is that size
        let data = [0, 1, b'b', 0, 0, 0, 4];

        let arena = Bump::new();

        let relation = &get_example_rel();

        let tuple_data = parse(&data, &arena, &relation.fields);

        assert_eq!(tuple_data, Err(DecoderError::TruncatedInput));
    }

    #[test]
    fn wrong_b_size() {
        let data = [0, 1, b'b', 0, 0, 0, 1];

        let arena = Bump::new();

        let relation = &get_example_rel();

        let tuple_data = parse(&data, &arena, &relation.fields);

        assert_eq!(tuple_data, Err(DecoderError::WrongFieldKind(Int4)))
    }
}
