use super::Storage;
use crate::{
    error::KvError,
    pb::abi::{Kvpair, Value},
    StorageIter,
};
use dashmap::{mapref::one::Ref, DashMap};

/// Memory DB
#[derive(Debug, Default, Clone)]
pub struct MemTable {
    tables: DashMap<String, DashMap<String, Value>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self::default()
    }

    fn get_or_create_table(&self, name: impl Into<String>) -> Ref<String, DashMap<String, Value>> {
        let names: String = name.into();
        match self.tables.get(&names) {
            Some(table) => table,
            None => {
                let entry = self.tables.entry(names).or_default();
                entry.downgrade()
            }
        }
    }
}

impl Storage for MemTable {
    fn get(
        &self,
        table: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.get(&key.into()).map(|v| v.value().clone()))
    }

    fn set(
        &self,
        table: impl Into<String>,
        key: impl Into<String>,
        value: Value,
    ) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.insert(key.into(), value))
    }

    fn contains(&self, table: impl Into<String>, key: impl Into<String>) -> Result<bool, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.contains_key(&key.into()))
    }

    fn del(
        &self,
        table: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.remove(&key.into()).map(|(_, v)| v))
    }

    fn get_all(&self, table: impl Into<String>) -> Result<Vec<Kvpair>, KvError> {
        let table = self.get_or_create_table(table);
        let result: Vec<Kvpair> = table
            .iter()
            .map(|m| Kvpair::new(m.key(), m.value().clone()))
            .collect();
        Ok(result)
    }

    fn get_iter(&self, table: impl Into<String>) -> Result<impl Iterator<Item = Kvpair>, KvError> {
        let table = self.get_or_create_table(table).clone();
        let iter = StorageIter::new(table.into_iter());
        Ok(iter)
    }
}
