use serde::{Deserialize, Serialize, Serializer, ser::SerializeStruct};
use serde_avro_fast::Schema;
use serde_repr::Serialize_repr;
use std::sync::LazyLock;

/// Version compatible with bumpalo arenas. Serde is not compatible with custom
/// allocators, so, we cannot provide serialization here.
pub mod arena {
    use bumpalo::collections::Vec;
    use serde::Serialize;

    use super::{Field, PgValue, ReplicaKind};

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

    #[derive(Serialize, Debug, Clone, PartialEq)]
    pub struct Relation<'a> {
        pub relation_oid: u32,
        pub name: String,
        pub namespace: String,
        pub fields: Vec<'a, Field>,
        pub replica_id: ReplicaKind,
    }

    impl<'a> Relation<'a> {
        pub fn into_avro(&self) -> Result<std::vec::Vec<u8>, serde_avro_fast::ser::SerError> {
            let schema = &super::RELATION_SCHEMA;

            let mut config = serde_avro_fast::ser::SerializerConfig::new(schema);
            serde_avro_fast::to_datum(&self, std::vec::Vec::with_capacity(256), &mut config)
        }
    }
}

pub mod owned {
    use crate::Field;
    use serde::{Deserialize, Serialize};
    use thiserror::Error;

    use super::{PgValue, ReplicaKind};

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
        pub fn op_str(&self) -> &'static str {
            match self {
                Op::Insert { row: _ } => "insert",
                Op::Update { old: _, row: _ } => "update",
                Op::Delete { old: _ } => "delete",
            }
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    pub struct ChangeEvent<'a> {
        #[serde(borrow)]
        pub op: Op<'a>,
        pub rel: u32,
    }

    impl<'a> ChangeEvent<'a> {
        pub fn from_avro(slice: &'a [u8]) -> Result<Self, FromAvroError> {
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
                &super::CHANGE_EVENT_SCHEMA,
            )?)
        }

        pub fn into_avro(&self) -> Result<std::vec::Vec<u8>, serde_avro_fast::ser::SerError> {
            let schema = &super::RELATION_SCHEMA;

            let mut config = serde_avro_fast::ser::SerializerConfig::new(schema);
            serde_avro_fast::to_datum(&self, std::vec::Vec::with_capacity(256), &mut config)
        }
    }
}

const CHANGE_EVENT_SCHEMA_STR: &str = r#"{"type":"record","name":"ChangeEvent","fields":[{"name":"op","type":[{"type":"record","name":"Insert","fields":[{"name":"row","type":{"type":"array","items":[{"type":"record","name":"Text","fields":[{"name":"Text","type":"string"}]},{"type":"record","name":"Int4","fields":[{"name":"Int4","type":"int"}]}]}}]},{"type":"record","name":"Update","fields":[{"name":"old","type":{"type":"array","items":["Text","Int4"]}},{"name":"row","type":{"type":"array","items":["Text","Int4"]}}]},{"type":"record","name":"Delete","fields":[{"name":"old","type":{"type":"array","items":["Text","Int4"]}}]}]},{"name":"rel","type":"int"}]}"#;

const RELATION_SCHEMA_STR: &str = r#"{"type":"record","name":"Relation","fields":[{"name":"relation_oid","type":"int"},{"name":"namespace","type":"string"},{"name":"name","type":"string"},{"name":"fields","type":{"type":"array","items":{"type":"record","name":"Field","fields":[{"name":"name","type":"string"},{"name":"kind","type":"string"},{"name":"is_key","type":"boolean"}]}}}]}"#;

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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
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

#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq)]
pub enum FieldKind {
    Int4,
    Text,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize_repr)]
#[repr(u8)]
pub enum ReplicaKind {
    Keys,
    Row,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
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
    // Note: The thing we are testing right now is roundtrip, which is not
    // possible in the arena versions, we'll just rely on the owned version for
    // the testing

    mod owned {
        use std::assert_matches;

        use super::super::{Field, FieldAccess, FieldKind, PgValue, ReplicaKind, owned::*};

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

            roundtrip(event);
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
    }
}
