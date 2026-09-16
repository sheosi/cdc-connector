use crate::decoder::DecoderError;

pub struct Begin {
    final_lsn: i64,
}

impl Begin {
    pub fn parse(data: bytes::Bytes) -> Result<Self, DecoderError> {
        let final_lsn = i64::from_be_bytes(
            data[1..9]
                .try_into()
                .map_err(|_| DecoderError::TruncatedInput)?,
        );

        Ok(Begin { final_lsn })
    }
}

pub struct Commit {}

impl Commit {
    pub fn parse(data: bytes::Bytes) -> Result<Self, DecoderError> {
        Ok(Self {})
    }
}
