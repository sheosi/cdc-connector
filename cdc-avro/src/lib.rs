use std::{collections::HashMap, sync::LazyLock};

use apache_avro::{
    Reader, Writer, from_value,
    types::{Record, Value},
};

use apache_avro::{AvroSchema, Schema};
use serde::{Deserialize, Serialize};
#[derive(AvroSchema, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Op {
    Insert {
        row: HashMap<String, String>,
    },
    Update {
        key: String,
        row: HashMap<String, String>,
    },
    Delete {
        key: String,
    },
}

impl Op {
    pub fn from_avro(bytes: Vec<u8>) -> Self {
        let schema = &OP_SCHEMA;
        let reader = Reader::new(std::io::Cursor::new(bytes)).unwrap();
        for result in reader {
            let new_op: Op = from_value(&result.unwrap()).unwrap();
            return new_op;
        }
        panic!("Something should be returned");
    }

    pub fn into_avro(&self) -> Vec<u8> {
        let schema = &OP_SCHEMA;
        let mut writer = apache_avro::Writer::new(schema, Vec::new()).unwrap();

        writer.append_ser(self).unwrap();
        writer.flush().unwrap();

        writer.into_inner().unwrap()
    }
}

#[derive(AvroSchema, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ChangeEvent {
    pub op: Op,
    pub table: String,
}

const OP_SCHEMA: LazyLock<Schema> = LazyLock::new(|| Op::get_schema());

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn back_and_forth() {
        let op = Op::Insert {
            row: maplit::hashmap!("a".to_string()=>"b".to_string()),
        };

        let bytes = op.into_avro();

        let back = Op::from_avro(bytes);

        assert_eq!(op, back);

        /*let msg = ChangeEvent {
            op: Op::Insert {
                row: maplit::hashmap! {"Test".to_string()=> "B".to_string()},
            },
            table: "users".into(),
        };*/

        //assert_eq!(record, back_again);
        //let bytes = (&msg);
        //let back: ChangeEvent = decode(&bytes).unwrap();
        // assert_eq!(msg, back);
    }
}
