use super::U8toString;
use crate::{
    error::KvError,
    pb::abi::{value, Kvpair, Value},
    Storage, StorageIter,
};
use sled::{Db, Error, IVec};
use std::{fmt::Debug, ops::Deref, path::Path};

pub struct SledDB(Db);

impl SledDB {
    /// 读取本都数据库文件
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(sled::open(path).unwrap())
    }

    /// 在 sleddb 里，因为它可以 scan_prefix，我们用 prefix
    /// 来模拟一个 table。当然，还可以用其它方案。
    fn get_full_key(table: &str, key: &str) -> String {
        // format!("{}:{}", table, key)
        format!("{}:{}", table, key)
    }

    /// 遍历 table 的 key 时，我们直接把 prefix: 当成 table
    fn get_table_prefix(table: &str) -> String {
        format!("{}:", table)
    }
}

impl Storage for SledDB {
    fn get(
        &self,
        table: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Option<Value>, KvError> {
        let key = SledDB::get_full_key(&table.into(), &key.into());
        self.deref().get(key).flip()
    }

    fn set(
        &self,
        table: impl Into<String>,
        key: impl Into<String>,
        value: Value,
    ) -> Result<Option<Value>, KvError> {
        let key = SledDB::get_full_key(&table.into(), &key.into());
        self.insert(key, value).flip()
    }

    fn contains(&self, table: impl Into<String>, key: impl Into<String>) -> Result<bool, KvError> {
        let key = SledDB::get_full_key(&table.into(), &key.into());
        self.contains_key(key).sled_error()
    }

    fn del(
        &self,
        table: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Option<Value>, KvError> {
        let key = SledDB::get_full_key(&table.into(), &key.into());
        self.remove(key).flip()
    }

    fn get_all(&self, table: impl Into<String>) -> Result<Vec<Kvpair>, KvError> {
        let data = self
            .scan_prefix(table.into())
            .map(|v: Result<(IVec, IVec), Error>| v.into())
            .collect();
        Ok(data)
    }

    fn get_iter(&self, table: impl Into<String>) -> Result<impl Iterator<Item = Kvpair>, KvError> {
        let iter = self.scan_prefix(table.into()).into_iter();
        let iter = StorageIter::new(iter);
        Ok(iter)
    }
}

impl From<Value> for IVec {
    fn from(value: Value) -> Self {
        match value.value {
            Some(v) => match v {
                value::Value::String(s) => s.as_str().into(),
                _ => todo!(),
            },
            None => todo!(),
        }
    }
}

impl From<Result<(IVec, IVec), Error>> for Kvpair {
    fn from(value: Result<(IVec, IVec), Error>) -> Self {
        match value {
            Ok(v) => Kvpair::new(v.0.u8_to_string(), v.1.as_ref().into()),
            Err(_) => Kvpair::default(),
        }
    }
}

impl std::ops::Deref for SledDB {
    type Target = Db;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

trait SledResult<T, E> {
    fn flip(self) -> Result<Option<Value>, KvError>;
}

impl<T, E> SledResult<T, E> for Result<Option<T>, E>
where
    T: Deref<Target = [u8]>,
    E: Debug,
{
    fn flip(self) -> Result<Option<Value>, KvError> {
        match self {
            Ok(value) => Ok(value.map(|m| m.deref().into())),
            Err(e) => Err(KvError::Internal(
                format!("error flipr: {:?}", e).to_owned(),
            )),
        }
    }
}

trait SledResultError<T, E> {
    fn sled_error(self) -> Result<T, E>;
}

impl<T, E> SledResultError<T, KvError> for Result<T, E>
where
    E: Debug,
{
    fn sled_error(self) -> Result<T, KvError> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(KvError::Internal(format!("error flips: {:?}", e))),
        }
    }
}

#[cfg(test)]
mod sled_test {
    use std::env::temp_dir;

    use tempfile::tempdir;

    use crate::storage::tests::{test_basic_interface, test_get_all, test_get_iter};

    use super::SledDB;

    #[test]
    fn sleddb_basic_interface_should_work() {
        let dir = temp_dir();
        let store = SledDB::new(dir);
        test_basic_interface(store);
    }

    #[test]
    fn sleddb_get_all_should_work() {
        let dir = tempdir().unwrap();
        let store = SledDB::new(dir);
        test_get_all(store);
    }
    #[test]
    fn sleddb_iter_should_work() {
        let dir = tempdir().unwrap();
        let store = SledDB::new(dir);
        test_get_iter(store);
    }
}
