use std::str::Utf8Error;

use cdc_avro::{FieldKind, ReplicaKind};
use thiserror::Error;

pub mod delete;
pub mod insert;
pub mod relation;
pub mod transactions;
pub mod tuple_data;
pub mod update;

mod common;

#[derive(Debug, Error, PartialEq)]
pub enum DecoderError {
    #[error("Found wrong old tuple key '{0}'")]
    WrongOldTupleKey(u8),

    #[error("Found wrong column type key '{0}'")]
    WrongColTypeKey(u8),

    #[error("Found wrong new tuple key '{0}'")]
    WrongNewTupleKey(u8),

    #[error("Found wrong field data flag {0}, allowed values are 0-3")]
    WrongFieldDataFlag(u8),

    #[error("Invalid OID {0}")]
    InvalidOid(u32),

    #[error("The input is incomplete")]
    TruncatedInput,

    #[error("Wrong UTF-8 characters: {0}")]
    NonUtf8(#[from] Utf8Error),

    #[error("Wrong UTF-8 characters: {0}")]
    NonUtf8Simd(#[from] simdutf8::basic::Utf8Error),

    #[error("Unknown relation of event: {0}")]
    UnknownRelation(u32),

    #[error("Wrong field kind {0:?}")]
    WrongFieldKind(FieldKind),

    #[error("Wrong replica id value {0}")]
    WrongReplicaId(u8),

    #[error("Got a different kind of old tuple data in a message compared to the relation {0:?}")]
    WrongOldTupleKind(ReplicaKind),
}
