use std::{collections::HashMap, sync::LazyLock};

use apache_avro::{Reader, from_value};

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

#[derive(AvroSchema, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ChangeEvent {
    pub op: Op,
    pub table: String,
}

impl ChangeEvent {
    pub fn from_avro(bytes: &[u8]) -> Self {
        let reader = Reader::new(std::io::Cursor::new(bytes)).unwrap();
        for result in reader {
            let new_event: ChangeEvent = from_value(&result.unwrap()).unwrap();
            return new_event;
        }
        panic!("Something should be returned");
    }

    pub fn into_avro(&self) -> Vec<u8> {
        let schema = &CHANGE_EVENT_SCHEMA;
        let mut writer = apache_avro::Writer::new(schema, Vec::new()).unwrap();

        writer.append_ser(self).unwrap();
        writer.flush().unwrap();

        writer.into_inner().unwrap()
    }
}

const CHANGE_EVENT_SCHEMA: LazyLock<Schema> = LazyLock::new(|| ChangeEvent::get_schema());

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
    }
}
