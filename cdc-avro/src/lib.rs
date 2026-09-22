use bumpalo::collections::Vec;
use serde::Serialize;
use serde::{Serializer, ser::SerializeStruct};
use serde_avro_fast::Schema;
use serde_repr::Serialize_repr;
use std::sync::LazyLock;
use thiserror::Error;

#[derive(Serialize, Debug, Clone, PartialEq)]
pub enum Op<'a> {
    Insert {
        row: Vec<'a, PgValue<'a>>,
    },
    Update {
        old: Vec<'a, PgValue<'a>>,
        row: Vec<'a, PgValue<'a>>,
    },
    Delete {
        old: Vec<'a, PgValue<'a>>,
    },
}

#[derive(Debug, Error)]
pub enum FromAvroError {
    #[error("No events where found in the transmission")]
    NoEvents,

    #[error("While ng from Avro: {0}")]
    Avro(#[from] serde_avro_fast::de::DeError),
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct ChangeEvent<'a> {
    #[serde(borrow)]
    pub op: Op<'a>,
    pub rel: u32,
}

impl<'a> ChangeEvent<'a> {
    pub fn from_avro(slice: &'a [u8]) -> Result<Self, FromAvroError> {
        /*Ok(serde_avro_fast::from_datum_slice::<ChangeEvent<'_>>(
            slice,
            &CHANGE_EVENT_SCHEMA,
        )?)*/
        Err(FromAvroError::NoEvents)
    }

    pub fn into_avro(&self) -> Result<std::vec::Vec<u8>, serde_avro_fast::ser::SerError> {
        let schema = &CHANGE_EVENT_SCHEMA;

        let mut config = serde_avro_fast::ser::SerializerConfig::new(schema);
        serde_avro_fast::to_datum(&self, std::vec::Vec::with_capacity(256), &mut config)
    }
}

const CHANGE_EVENT_SCHEMA_STR: &str = r#"{"type":"record","name":"ChangeEvent","fields":[{"name":"op","type":[{"type":"record","name":"Insert","fields":[{"name":"row","type":{"type":"array","items":[{"type":"record","name":"Text","fields":[{"name":"Text","type":"string"}]},{"type":"record","name":"Int4","fields":[{"name":"Int4","type":"int"}]}]}}]},{"type":"record","name":"Update","fields":[{"name":"old_k","type":"int"},{"name":"old","type":{"type":"array","items":["Text","Int4"]}},{"name":"row","type":{"type":"array","items":["Text","Int4"]}}]},{"type":"record","name":"Delete","fields":[{"name":"old_k","type":"int"},{"name":"old","type":{"type":"array","items":["Text","Int4"]}}]}]},{"name":"rel","type":"int"}]}"#;

const RELATION_SCHEMA_STR: &str = r#"{"type":"record","name":"Relation","fields":[{"name":"oid","type":"int"},{"name":"namespace","type":"string"},{"name":"relname","type":"string"},{"name":"fields","type":{"type":"array","items":{"type":"record","name":"Field","fields":[{"name":"name","type":"string"},{"name":"kind","type":"string"},{"name":"is_key","type":"boolean"}]}}}]}"#;

const CHANGE_EVENT_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    CHANGE_EVENT_SCHEMA_STR
        .parse()
        .expect("Failed to parse Avro schema")
});

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct Relation<'a> {
    pub relation_oid: u32,
    pub name: String,
    pub namespace: String,
    pub fields: Vec<'a, Field>,
    pub replica_id: ReplicaKind,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct Field {
    pub is_key: bool,
    pub name: String,
    pub kind: FieldKind,
}

pub trait FieldAccess {
    fn get_name(&self) -> &str;
    fn get_kind(&self) -> FieldKind;
}

impl FieldAccess for Field {
    #[inline(always)]
    fn get_name(&self) -> &str {
        &self.name
    }

    #[inline(always)]
    fn get_kind(&self) -> FieldKind {
        self.kind
    }
}

#[derive(Serialize, Copy, Clone, Debug, PartialEq)]
pub enum FieldKind {
    Int4,
    Text,
}

impl<'a> Relation<'a> {
    pub fn from_avro(slice: &[u8]) -> Result<Self, FromAvroError> {
        /*Ok(serde_avro_fast::from_datum_slice::<ChangeEvent<'_>>(
            slice,
            &CHANGE_EVENT_SCHEMA,
        )?)*/
        Err(FromAvroError::NoEvents)
    }

    pub fn to_avro(&self) -> Result<std::vec::Vec<u8>, serde_avro_fast::ser::SerError> {
        let schema = &CHANGE_EVENT_SCHEMA;

        let mut config = serde_avro_fast::ser::SerializerConfig::new(schema);
        serde_avro_fast::to_datum(&self, std::vec::Vec::with_capacity(256), &mut config)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize_repr)]
#[repr(u8)]
pub enum ReplicaKind {
    Keys,
    Row,
}

const RELATION_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    RELATION_SCHEMA_STR
        .parse()
        .expect("Failed to parse Avro schema")
});

#[derive(Debug, Clone, PartialEq)]
pub enum PgValue<'a> {
    Text(&'a str),
    Int4(u32),
}

impl<'a> Serialize for PgValue<'a> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            PgValue::Text(s) => {
                let mut record = serializer.serialize_struct("Text", 1)?;
                record.serialize_field("Text", s)?;
                record.end()
            }
            PgValue::Int4(n) => {
                let mut record = serializer.serialize_struct("Int4", 1)?;
                record.serialize_field("Int4", n)?;
                record.end()
            }
        }
    }
}

impl<'a> From<&'a str> for PgValue<'a> {
    fn from(value: &'a str) -> Self {
        Self::Text(value)
    }
}

impl From<u32> for PgValue<'_> {
    fn from(value: u32) -> Self {
        Self::Int4(value)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use bumpalo::{Bump, vec};

    #[test]
    fn back_and_forth() {
        let arena = Bump::with_capacity(1024);

        let event = ChangeEvent {
            op: Op::Insert {
                row: vec![in &arena;
                    PgValue::Text("b"),
                ],
            },
            rel: 1024,
        };

        let bytes = event.into_avro().unwrap();

        //Reader::new(std::io::Cursor::new(bytes))
        //let back = ChangeEvent::from_avro(&bytes).unwrap();

        //assert_eq!(event, back);
    }
}
