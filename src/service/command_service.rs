use crate::{
    error::KvError,
    pb::abi::{CommandResponse, Hget, Hgetall, Hset, Value},
    Storage,
};

pub trait CommandService {
    fn execute(self, store: &impl Storage) -> CommandResponse;
}

impl CommandService for Hget {
    fn execute(self, store: &impl crate::Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(format!("{}:{}", self.table, self.key)).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hgetall {
    fn execute(self, store: &impl crate::Storage) -> CommandResponse {
        match store.get_all(&self.table) {
            Ok(v) => v.into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hset {
    fn execute(self, store: &impl crate::Storage) -> CommandResponse {
        match self.pair {
            Some(pair) => match store.set(&self.table, pair.key, pair.value.unwrap_or_default()) {
                Ok(Some(v)) => v.into(),
                Ok(None) => Value::default().into(),
                Err(e) => e.into(),
            },
            None => todo!(),
        }
    }
}
