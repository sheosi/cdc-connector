use crate::decoder::tuple_data::TupleData;

pub fn get_old_tuple_data(data: &[u8]) -> TupleData {
    match data[0] {
        b'K' => TupleData::parse(&data[1..]),
        b'O' => {
            todo!()
        }
        _ => panic!("Wrong key"),
    }
}

pub fn get_new_tuple_data(data: &[u8]) -> TupleData {
    assert!(data[0] == b'N');

    TupleData::parse(&data[1..])
}
