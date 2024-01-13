use crate::pb::abi::Value;
use thiserror::Error;
use tokio_rustls::{rustls::TLSError, webpki::InvalidDNSNameError};

#[derive(Error, Debug)]
pub enum KvError {
    #[error("Not found for key: {0}")]
    NotFound(String),

    #[error("Cannot parse command: `{0}`")]
    InvalidCommand(String),
    #[error("Cannot convert value {:0} to {1}")]
    ConvertError(Value, &'static str),
    #[error("Cannot process command {0} with table: {1}, key: {2}. Error: {}")]
    StorageError(&'static str, String, String, String),

    #[error("Failed to encode protobuf message")]
    EncodeError(#[from] prost::EncodeError),
    #[error("Failed to decode protobuf message")]
    DecodeError(#[from] prost::DecodeError),
    #[error("std::io error: {0}")]
    IOError(String),
    #[error("I/O error")]
    IoError(#[from] std::io::Error),
    #[error("Internal error: {0}")]
    Internal(String),

    #[error("unsupported {0} {1}")]
    CertificateParseError(String, String),

    #[error("TLSError")]
    TLSError(#[from] TLSError),

    #[error("InvalidDNSNameError")]
    InvalidDNSNameError(#[from] InvalidDNSNameError),

    #[error("TomlError")]
    TomlError(#[from] toml::de::Error),
}

pub(crate) trait IOError<T> {
    fn to_error(self) -> Result<T, KvError>;
}

impl<T> IOError<T> for Result<T, std::io::Error> {
    fn to_error(self) -> Result<T, KvError> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(KvError::IOError(format!("{:?}", e))),
        }
    }
}

pub(crate) trait CertError<T> {
    fn to_error(self, key: impl Into<String>, value: impl Into<String>) -> Result<T, KvError>;
}

impl<T> CertError<T> for Result<T, ()> {
    fn to_error(self, key: impl Into<String>, value: impl Into<String>) -> Result<T, KvError> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(KvError::CertificateParseError(key.into(), value.into())),
        }
    }
}
