pub mod memory;
pub mod sled_db;

use crate::{
    error::KvError,
    pb::abi::{Kvpair, Value},
};

/// 对存储的抽象，我们不关心数据存在哪儿，但需要定义外界如何和存储打交道
pub trait Storage: Send + Sync + 'static {
    /// 从一个 HashTable 里获取一个 key 的 value
    fn get(
        &self,
        table: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Option<Value>, KvError>;

    /// 从一个 HashTable 里设置一个 key 的 value，返回旧的 value
    fn set(
        &self,
        table: impl Into<String>,
        key: impl Into<String>,
        value: Value,
    ) -> Result<Option<Value>, KvError>;

    /// 查看 HashTable 中是否有 key
    fn contains(&self, table: impl Into<String>, key: impl Into<String>) -> Result<bool, KvError>;

    /// 从 HashTable 中删除一个 key
    fn del(
        &self,
        table: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Option<Value>, KvError>;

    /// 遍历 HashTable，返回所有 kv pair（这个接口不好）
    fn get_all(&self, table: impl Into<String>) -> Result<Vec<Kvpair>, KvError>;

    /// 遍历 HashTable，返回 kv pair 的 Iterator
    fn get_iter(&self, table: impl Into<String>) -> Result<impl Iterator<Item = Kvpair>, KvError>;
}

/// 提供 Storage iterator，这样 trait 的实现者只需要
/// 把它们的 iterator 提供给 StorageIter，然后它们保证
/// next() 传出的类型实现了 Into<Kvpair> 即可
pub struct StorageIter<T> {
    data: T,
}

impl<T> StorageIter<T> {
    pub fn new(data: T) -> Self {
        Self { data }
    }
}

impl<T> Iterator for StorageIter<T>
where
    T: Iterator,
    T::Item: Into<Kvpair>,
{
    type Item = Kvpair;

    fn next(&mut self) -> Option<Self::Item> {
        self.data.next().map(|v| v.into())
    }
}

trait U8toString<T> {
    fn u8_to_string(self) -> String;
}

impl<T> U8toString<T> for T
where
    T: AsRef<[u8]>,
{
    fn u8_to_string(self) -> String {
        String::from_utf8_lossy(self.as_ref()).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{memory::MemTable, *};
    use pretty_assertions::assert_eq;

    #[test]
    pub fn memtable_basic_interface_should_work() {
        let store = MemTable::new();
        test_basic_interface(store);
    }

    #[test]
    pub fn memtable_get_all_should_work() {
        let store = MemTable::new();
        test_get_all(store);
    }

    #[test]
    pub fn memtable_iter_should_work() {
        let store = MemTable::new();
        test_get_iter(store);
    }

    pub fn test_get_all(store: impl Storage) {
        store.set("t2", "k1", "v1".into()).unwrap();
        store.set("t2", "k2", "v2".into()).unwrap();
        let mut data = store.get_all("t2").unwrap();
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(
            data,
            vec![
                Kvpair::new("k1", "v1".into()),
                Kvpair::new("k2", "v2".into())
            ]
        )
    }

    pub fn test_get_iter(store: impl Storage) {
        store.set("t2", "k1", "v1".into()).unwrap();
        store.set("t2", "k2", "v2".into()).unwrap();
        let mut data: Vec<_> = store.get_iter("t2").unwrap().collect();
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(
            data,
            vec![
                Kvpair::new("k1", "v1".into()),
                Kvpair::new("k2", "v2".into())
            ]
        )
    }

    pub fn test_basic_interface(store: impl Storage) {
        // 第一次 set 会创建 table，插入 key 并返回 None（之前没值）
        let v = store.set("t1", "hello", "world".into());
        assert!(v.unwrap().is_none());
        // 再次 set 同样的 key 会更新，并返回之前的值
        let v1 = store.set("t1", "hello", "world1".into());
        assert_eq!(v1.unwrap(), Some("world".into()));

        // get 存在的 key 会得到最新的值
        let v = store.get("t1", "hello");
        assert_eq!(v.unwrap(), Some("world1".into()));

        // get 不存在的 key 或者 table 会得到 None
        assert_eq!(None, store.get("t1", "hello1").unwrap());
        assert!(store.get("t2", "hello1").unwrap().is_none());

        // contains 纯在的 key 返回 true，否则 false
        assert!(store.contains("t1", "hello").unwrap());
        assert!(!store.contains("t1", "hello1").unwrap());
        assert!(!store.contains("t2", "hello").unwrap());

        // del 存在的 key 返回之前的值
        let v = store.del("t1", "hello");
        assert_eq!(v.unwrap(), Some("world1".into()));

        // del 不存在的 key 或 table 返回 None
        assert_eq!(None, store.del("t1", "hello1").unwrap());
        assert_eq!(None, store.del("t2", "hello").unwrap());
    }
}
