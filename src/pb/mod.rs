pub mod abi;

use abi::{command_request::RequestData, *};
use bytes::Bytes;
use hyper::StatusCode;

use crate::error::KvError;

impl CommandRequest {
    pub fn new_hset(
        table: impl Into<String>,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> Self {
        RequestData::Hset(Hset {
            table: table.into(),
            pair: Some(Kvpair::new(key, value.into())),
        })
        .into()
    }

    pub fn new_hget(table: impl Into<String>, key: impl Into<String>) -> Self {
        RequestData::Hget(Hget {
            table: table.into(),
            key: key.into(),
        })
        .into()
    }

    pub fn new_hdel(table: impl Into<String>, key: impl Into<String>) -> Self {
        RequestData::Hdel(Hdel {
            table: table.into(),
            key: key.into(),
        })
        .into()
    }

    pub fn new_hgetall(table: impl Into<String>) -> Self {
        RequestData::Hgetall(Hgetall {
            table: table.into(),
        })
        .into()
    }

    pub fn new_hmget(table: impl Into<String>, key: impl Into<String>) -> Self {
        RequestData::Hmget(Hmget {
            table: table.into(),
            keys: vec![key.into()],
        })
        .into()
    }

    pub fn new_publish(topic: &str, data: Vec<Value>) -> Self {
        RequestData::Publish(Publish {
            topic: topic.into(),
            data,
        })
        .into()
    }

    pub fn new_subscribe(topic: &str) -> Self {
        RequestData::Subscribe(Subscribe {
            topic: topic.into(),
        })
        .into()
    }

    pub fn new_unsubscribe(topic: &str, id: u32) -> Self {
        RequestData::Unsubscribe(Unsubscribe {
            topic: topic.into(),
            id,
        })
        .into()
    }
}

impl CommandResponse {
    pub fn ok() -> Self {
        let mut cmd = CommandResponse::default();
        cmd.status = StatusCode::OK.as_u16() as _;
        cmd
    }
}

impl Kvpair {
    pub fn new(key: impl Into<String>, value: Value) -> Self {
        Self {
            key: key.into(),
            value: Some(value),
        }
    }
}

impl From<RequestData> for CommandRequest {
    fn from(value: RequestData) -> Self {
        Self {
            request_data: Some(value),
        }
    }
}

impl From<Hset> for RequestData {
    fn from(value: Hset) -> Self {
        RequestData::Hset(value)
    }
}

impl From<Hget> for RequestData {
    fn from(value: Hget) -> Self {
        RequestData::Hget(value)
    }
}

impl From<Hdel> for RequestData {
    fn from(value: Hdel) -> Self {
        RequestData::Hdel(value)
    }
}

impl From<Value> for CommandResponse {
    fn from(value: Value) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as _,
            values: vec![value],
            ..Default::default()
        }
    }
}

impl From<Vec<Value>> for CommandResponse {
    fn from(values: Vec<Value>) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as _,
            values,
            ..Default::default()
        }
    }
}

impl From<KvError> for CommandResponse {
    fn from(value: KvError) -> Self {
        let mut result = Self {
            status: StatusCode::INTERNAL_SERVER_ERROR.as_u16() as _,
            message: value.to_string(),
            values: vec![],
            pairs: vec![],
        };

        match value {
            KvError::NotFound(_) => result.status = StatusCode::NOT_FOUND.as_u16() as _,
            KvError::InvalidCommand(_) => result.status = StatusCode::BAD_REQUEST.as_u16() as _,
            _ => {}
        }

        result
    }
}

impl From<(String, Value)> for Kvpair {
    fn from(kv: (String, Value)) -> Self {
        Kvpair {
            key: kv.0,
            value: Some(kv.1),
        }
    }
}

impl From<Vec<Kvpair>> for CommandResponse {
    fn from(value: Vec<Kvpair>) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as _,
            pairs: value,
            ..Default::default()
        }
    }
}

impl From<i64> for CommandResponse {
    fn from(value: i64) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as _,
            values: vec![value.into()],
            ..Default::default()
        }
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self {
            value: Some(value::Value::Integer(value)),
        }
    }
}

impl TryFrom<&Value> for i64 {
    type Error = KvError;

    fn try_from(v: &Value) -> Result<Self, Self::Error> {
        match v.value {
            Some(value::Value::Integer(i)) => Ok(i),
            _ => Err(KvError::ConvertError(v.clone(), "Integer")),
        }
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self {
            value: Some(value::Value::String(value)),
        }
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self {
            value: Some(value::Value::String(value.into())),
        }
    }
}

impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Self {
        let str = String::from_utf8_lossy(value);
        Self {
            value: Some(value::Value::String(str.into())),
        }
    }
}

impl From<Bytes> for Value {
    fn from(value: Bytes) -> Self {
        Self {
            value: Some(value::Value::Binary(value)),
        }
    }
}

impl<const N: usize> From<&[u8; N]> for Value {
    fn from(buf: &[u8; N]) -> Self {
        Bytes::copy_from_slice(&buf[..]).into()
    }
}

impl TryFrom<&CommandResponse> for i64 {
    type Error = KvError;

    fn try_from(value: &CommandResponse) -> Result<Self, Self::Error> {
        if value.status != StatusCode::OK.as_u16() as u32 {
            return Err(KvError::ConvertError(
                value.values[0].clone(),
                "CommandResponse",
            ));
        }
        match value.values.get(0) {
            Some(v) => v.try_into(),
            None => Err(KvError::ConvertError(
                value.values[0].clone(),
                "CommandResponse",
            )),
        }
    }
}
