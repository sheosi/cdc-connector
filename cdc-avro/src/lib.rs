use chrono::DateTime;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{MapAccess, Visitor},
    ser::SerializeStruct,
};
use serde_avro_fast::Schema;
use std::{fmt, sync::LazyLock};
use thiserror::Error;

/// Version compatible with bumpalo arenas. Serde is not compatible with custom
/// allocators, so, we cannot provide serialization here.
pub mod arena {
    use bumpalo::collections::Vec;
    use serde::Serialize;

    use super::PgValue;

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

    #[derive(Serialize, Debug, Clone, PartialEq)]
    pub struct ChangeEvent<'a> {
        #[serde(borrow)]
        pub op: Op<'a>,
        pub rel: u32,
    }

    impl<'a> ChangeEvent<'a> {
        pub fn into_avro(&self) -> Result<std::vec::Vec<u8>, serde_avro_fast::ser::SerError> {
            let schema = &super::CHANGE_EVENT_SCHEMA;

            let mut config = serde_avro_fast::ser::SerializerConfig::new(schema);
            serde_avro_fast::to_datum(&self, std::vec::Vec::with_capacity(256), &mut config)
        }
    }
}

/// Makes use of globally allocated vectors. We can provide deserialization here.
pub mod owned {
    use serde::{Deserialize, Serialize};

    use super::PgValue;

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    pub enum Op<'a> {
        Insert {
            #[serde(borrow)]
            row: Vec<PgValue<'a>>,
        },
        Update {
            old: Vec<PgValue<'a>>,
            row: Vec<PgValue<'a>>,
        },
        Delete {
            old: Vec<PgValue<'a>>,
        },
    }

    impl<'a> Op<'a> {
        /// Str for log and metrics purposes
        pub fn op_str(&self) -> &'static str {
            match self {
                Op::Insert { row: _ } => "insert",
                Op::Update { old: _, row: _ } => "update",
                Op::Delete { old: _ } => "delete",
            }
        }

        pub fn merge_toasted(&mut self) -> Result<(), ()> {
            if let Op::Update { old, row } = self {
                for (i, v) in row.iter_mut().enumerate() {
                    if matches!(v, PgValue::Toasted) {
                        if i < old.len() {
                            *v = old[i].clone();
                        } else {
                            return Err(());
                        }
                    }
                }
            }

            Ok(())
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    pub struct ChangeEvent<'a> {
        #[serde(borrow)]
        pub op: Op<'a>,
        pub rel: u32,
    }

    impl<'a> ChangeEvent<'a> {
        pub fn from_avro(slice: &'a [u8]) -> Result<Self, super::FromAvroError> {
            Ok(serde_avro_fast::from_datum_slice::<ChangeEvent<'_>>(
                slice,
                &super::CHANGE_EVENT_SCHEMA,
            )?)
        }

        pub fn into_avro(&self) -> Result<std::vec::Vec<u8>, serde_avro_fast::ser::SerError> {
            let schema = &super::CHANGE_EVENT_SCHEMA;

            let mut config = serde_avro_fast::ser::SerializerConfig::new(schema);
            serde_avro_fast::to_datum(&self, std::vec::Vec::with_capacity(256), &mut config)
        }
    }
}

const CHANGE_EVENT_SCHEMA_STR: &str = r#"{"type":"record","name":"ChangeEvent","fields":[{"name":"op","type":[{"type":"record","name":"Insert","fields":[{"name":"row","type":{"type":"array","items":[{"type":"record","name":"Text","fields":[{"name":"Text","type":"string"}]},{"type":"record","name":"Int4","fields":[{"name":"Int4","type":"long"}]},{"type":"record","name":"Toasted","fields":[{"name":"Toasted","type":"null"}]},{"type":"record","name":"Null","fields":[{"name":"Null","type":"null"}]},{"type":"record","name":"Int8","fields":[{"name":"Int8","type":"long"}]},{"type":"record","name":"Bool","fields":[{"name":"Bool","type":"boolean"}]},{"type":"record","name":"Timestamp","fields":[{"name":"Timestamp","type":"long"}]}]}}]},{"type":"record","name":"Update","fields":[{"name":"old","type":{"type":"array","items":["Text","Int4","Null","Toasted","Int8","Bool","Timestamp"]}},{"name":"row","type":{"type":"array","items":["Text","Int4","Null","Toasted","Int8","Bool","Timestamp"]}}]},{"type":"record","name":"Delete","fields":[{"name":"old","type":{"type":"array","items":["Text","Int4","Null","Toasted","Int8","Bool","Timestamp"]}}]}]},{"name":"rel","type":"long"}]}"#;

const RELATION_SCHEMA_STR: &str = r#"{"type":"record","name":"Relation","fields":[{"name":"relation_oid","type":"long"},{"name":"namespace","type":"string"},{"name":"name","type":"string"},{"name":"fields","type":{"type":"array","items":{"type":"record","name":"Field","fields":[{"name":"is_key","type":"boolean"},{"name":"name","type":"string"},{"name":"kind","type":"int"}]}}},{"name":"replica_id","type":"int"}]}"#;

const CHANGE_EVENT_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    CHANGE_EVENT_SCHEMA_STR
        .parse()
        .expect("Failed to parse Avro schema")
});

const RELATION_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    RELATION_SCHEMA_STR
        .parse()
        .expect("Failed to parse Avro schema")
});

pub trait FieldAccess {
    fn get_name(&self) -> &str;
    fn get_kind(&self) -> FieldKind;
}

impl<'a> FieldAccess for Field {
    #[inline(always)]
    fn get_name(&self) -> &str {
        &self.name
    }

    #[inline(always)]
    fn get_kind(&self) -> FieldKind {
        self.kind
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Relation {
    pub relation_oid: u32,
    pub name: String,
    pub namespace: String,
    pub fields: Vec<Field>,
    pub replica_id: ReplicaKind,
}

impl Relation {
    pub fn from_avro(slice: &[u8]) -> Result<Self, FromAvroError> {
        Ok(serde_avro_fast::from_datum_slice::<Relation>(
            slice,
            &RELATION_SCHEMA,
        )?)
    }

    pub fn into_avro(&self) -> Result<std::vec::Vec<u8>, serde_avro_fast::ser::SerError> {
        let schema = &RELATION_SCHEMA;

        let mut config = serde_avro_fast::ser::SerializerConfig::new(schema);
        serde_avro_fast::to_datum(&self, std::vec::Vec::with_capacity(256), &mut config)
    }
}
#[derive(Debug, Error)]
pub enum FromAvroError {
    #[error("No events where found in the transmission")]
    NoEvents,

    #[error("While ng from Avro: {0}")]
    Avro(#[from] serde_avro_fast::de::DeError),
}

impl PartialEq for FromAvroError {
    fn eq(&self, other: &Self) -> bool {
        use FromAvroError::*;

        match (self, other) {
            (NoEvents, NoEvents) => true,
            (Avro(_), Avro(_)) => true,
            _ => false,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Field {
    pub is_key: bool,
    pub name: String,
    pub kind: FieldKind,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum FieldKind {
    Int4,
    Text,
    Int8,
    Bool,
    Timestamp,
}

impl Serialize for FieldKind {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(*self as u8)
    }
}

impl<'de> Deserialize<'de> for FieldKind {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error as DeError;
        match u8::deserialize(deserializer)? {
            0 => Ok(FieldKind::Int4),
            1 => Ok(FieldKind::Text),
            2 => Ok(FieldKind::Int8),
            3 => Ok(FieldKind::Bool),
            4 => Ok(FieldKind::Timestamp),
            other => Err(DeError::custom(format!("unknown FieldKind: {}", other))),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ReplicaKind {
    Keys,
    Row,
}

impl Serialize for ReplicaKind {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(*self as u8)
    }
}

impl<'de> Deserialize<'de> for ReplicaKind {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error as DeError;
        match u8::deserialize(deserializer)? {
            0 => Ok(ReplicaKind::Keys),
            1 => Ok(ReplicaKind::Row),
            other => Err(DeError::custom(format!("unknown FieldKind: {}", other))),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PgValue<'a> {
    Null,
    Text(&'a str),
    Int4(i32),
    Int8(i64),
    Bool(bool),
    Timestamp(i64),
    Toasted,
}

impl<'a> Serialize for PgValue<'a> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            PgValue::Null => {
                let mut record = serializer.serialize_struct("Null", 1)?;
                record.serialize_field("Null", &())?;
                record.end()
            }
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
            PgValue::Int8(n) => {
                let mut record = serializer.serialize_struct("Int8", 1)?;
                record.serialize_field("Int8", n)?;
                record.end()
            }
            PgValue::Bool(b) => {
                let mut record = serializer.serialize_struct("Bool", 1)?;
                record.serialize_field("Bool", b)?;
                record.end()
            }
            PgValue::Timestamp(t) => {
                let mut record = serializer.serialize_struct("Timestamp", 1)?;
                record.serialize_field("Timestamp", t)?;
                record.end()
            }
            PgValue::Toasted => {
                let mut record = serializer.serialize_struct("Toasted", 1)?;
                record.serialize_field("Toasted", &())?;
                record.end()
            }
        }
    }
}

impl<'de: 'a, 'a> Deserialize<'de> for PgValue<'a> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct PgValueVisitor<'a>(std::marker::PhantomData<&'a ()>);

        impl<'de: 'a, 'a> Visitor<'de> for PgValueVisitor<'a> {
            type Value = PgValue<'a>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a Text, Int4, Int8, Bool, Timestamp, Toasted or Null record")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let key: String = map
                    .next_key()?
                    .ok_or_else(|| serde::de::Error::custom("empty PgValue record"))?;

                match key.as_str() {
                    "Text" => {
                        let value: &'de str = map.next_value()?;
                        Ok(PgValue::Text(value))
                    }
                    "Int4" => {
                        let value: i32 = map.next_value()?;
                        Ok(PgValue::Int4(value))
                    }
                    "Int8" => {
                        let value: i64 = map.next_value()?;
                        Ok(PgValue::Int8(value))
                    }
                    "Bool" => {
                        let value: bool = map.next_value()?;
                        Ok(PgValue::Bool(value))
                    }
                    "Timestamp" => {
                        let value: i64 = map.next_value()?;
                        Ok(PgValue::Timestamp(value))
                    }
                    "Null" => {
                        let _ = map.next_value::<()>()?;
                        Ok(PgValue::Null)
                    }
                    "Toasted" => {
                        let _ = map.next_value::<()>()?;
                        Ok(PgValue::Toasted)
                    }
                    other => Err(serde::de::Error::custom(format!(
                        "unknown PgValue variant: {}",
                        other
                    ))),
                }
            }
        }

        deserializer.deserialize_map(PgValueVisitor(std::marker::PhantomData))
    }
}

impl<'a> From<&'a str> for PgValue<'a> {
    fn from(value: &'a str) -> Self {
        Self::Text(value)
    }
}

impl From<i32> for PgValue<'_> {
    fn from(value: i32) -> Self {
        Self::Int4(value)
    }
}

impl<'a> PgValue<'a> {
    pub fn to_json_value(&self) -> serde_json::Value {
        match self {
            PgValue::Null => serde_json::Value::Null,
            PgValue::Text(s) => serde_json::Value::String(s.to_string()),
            PgValue::Int4(n) => serde_json::Value::Number((*n).into()),
            PgValue::Int8(n) => serde_json::Value::Number((*n).into()),
            PgValue::Bool(b) => serde_json::Value::Bool(*b),
            PgValue::Timestamp(t) => {
                let pg_epoch_to_unix_micros = 946_684_800_000_000i64;
                match DateTime::from_timestamp_micros(*t + pg_epoch_to_unix_micros) {
                    Some(dt) => {
                        serde_json::Value::String(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
                    }
                    None => serde_json::Value::Null,
                }
            }
            PgValue::Toasted => serde_json::Value::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    // Note: The thing we are testing right now is roundtrip, which is not
    // possible in the arena versions, we'll just rely on the owned version for
    // the testing

    mod owned {
        use std::assert_matches;

        use super::super::{Field, FieldKind, PgValue, Relation, ReplicaKind, owned::*};

        fn roundtrip(event: ChangeEvent<'_>) {
            let bytes = event.into_avro().unwrap();

            let back = ChangeEvent::from_avro(&bytes);

            assert_eq!(Ok(event), back);
        }

        fn roundtrip_bugged(event: ChangeEvent<'_>) {
            let mut bytes = event.into_avro().unwrap();

            bytes[3] = 124;
            bytes.pop();

            let back = ChangeEvent::from_avro(&bytes);

            assert_matches!(back, Err(_));
        }

        fn roundtrip_rel(relation: Relation) {
            let bytes = relation.into_avro().unwrap();

            let back = Relation::from_avro(&bytes);

            assert_eq!(Ok(relation), back);
        }

        fn roundtrip_rel_bugged(relation: Relation) {
            let mut bytes = relation.into_avro().unwrap();

            bytes[3] = 124;
            bytes.pop();

            let back = Relation::from_avro(&bytes);

            assert_matches!(back, Err(_));
        }

        fn text_field(field: &str) -> Field {
            Field {
                is_key: false,
                name: field.to_string(),
                kind: FieldKind::Text,
            }
        }

        fn int4_field_key(field: &str) -> Field {
            Field {
                is_key: true,
                name: field.to_string(),
                kind: FieldKind::Int4,
            }
        }

        #[test]
        fn roundtrip_event_insert() {
            let event = ChangeEvent {
                op: Op::Insert {
                    row: vec![PgValue::Text("b")],
                },
                rel: 1024,
            };

            roundtrip(event);
        }

        #[test]
        fn roundtrip_event_insert_empty() {
            let event = ChangeEvent {
                op: Op::Insert { row: vec![] },
                rel: 1024,
            };

            roundtrip(event);
        }

        #[test]
        fn roundtrip_event_insert_int8() {
            let event = ChangeEvent {
                op: Op::Insert {
                    row: vec![PgValue::Int8(0)],
                },
                rel: 1024,
            };

            roundtrip(event);
        }

        #[test]
        fn roundtrip_event_insert_bool() {
            let event = ChangeEvent {
                op: Op::Insert {
                    row: vec![PgValue::Bool(true)],
                },
                rel: 1024,
            };

            roundtrip(event);
        }

        #[test]
        fn roundtrip_event_insert_timestamp() {
            let event = ChangeEvent {
                op: Op::Insert {
                    row: vec![PgValue::Timestamp(999999)],
                },
                rel: 1024,
            };

            roundtrip(event);
        }

        #[test]
        fn roundtrip_event_update() {
            let event = ChangeEvent {
                op: Op::Update {
                    old: vec![PgValue::Int4(1)],

                    row: vec![PgValue::Text("hola")],
                },

                rel: 1024,
            };

            roundtrip(event);
        }

        #[test]
        fn roundtrip_event_update_empty() {
            let event = ChangeEvent {
                op: Op::Update {
                    old: vec![],

                    row: vec![PgValue::Text("hola")],
                },

                rel: 1024,
            };

            roundtrip(event);
        }

        #[test]
        fn roundtrip_event_update_toasted() {
            let event = ChangeEvent {
                op: Op::Update {
                    old: vec![PgValue::Int4(1), PgValue::Text("old@x.com")],
                    row: vec![PgValue::Int4(1), PgValue::Text("Alice"), PgValue::Toasted],
                },
                rel: 1024,
            };

            roundtrip(event);
        }

        #[test]
        fn roundtrip_event_delete() {
            let event = ChangeEvent {
                op: Op::Delete {
                    old: vec![PgValue::Int4(1), PgValue::Text("HOLA")],
                },

                rel: 1024,
            };

            roundtrip(event);
        }

        #[test]
        fn roundtrip_event_delete_empty() {
            let event = ChangeEvent {
                op: Op::Delete { old: vec![] },

                rel: 1024,
            };

            roundtrip(event);
        }

        #[test]
        fn roundtrip_event_bugged() {
            let event = ChangeEvent {
                op: Op::Delete {
                    old: vec![PgValue::Int4(1), PgValue::Text("HOLA")],
                },

                rel: 1024,
            };

            roundtrip_bugged(event);
        }

        #[test]
        fn roundtrip_relation_simple() {
            let relation = Relation {
                relation_oid: 1,
                name: "Simple".to_string(),
                namespace: "Public".to_string(),
                fields: vec![text_field("Hola")],
                replica_id: ReplicaKind::Row,
            };

            roundtrip_rel(relation);
        }

        #[test]
        fn roundtrip_relation_multiple() {
            let relation = Relation {
                relation_oid: 1,
                name: "Simple".to_string(),
                namespace: "Public".to_string(),
                fields: vec![int4_field_key("id"), text_field("Hola")],
                replica_id: ReplicaKind::Row,
            };

            roundtrip_rel(relation);
        }

        #[test]
        fn roundtrip_relation_bugged() {
            let relation = Relation {
                relation_oid: 1,
                name: "Simple".to_string(),
                namespace: "Public".to_string(),
                fields: vec![text_field("Hola")],
                replica_id: ReplicaKind::Row,
            };

            roundtrip_rel_bugged(relation);
        }

        #[test]
        fn merge_toasted() {
            let mut op = Op::Update {
                old: vec![PgValue::Int4(0), PgValue::Text("hello")],
                row: vec![PgValue::Int4(0), PgValue::Toasted],
            };

            let op_merged = Op::Update {
                old: vec![PgValue::Int4(0), PgValue::Text("hello")],
                row: vec![PgValue::Int4(0), PgValue::Text("hello")],
            };

            op.merge_toasted().unwrap();

            assert_eq!(op, op_merged);
        }
    }
}
