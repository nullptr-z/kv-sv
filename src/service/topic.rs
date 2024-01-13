use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use dashmap::{DashMap, DashSet};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::{
    error::KvError,
    pb::abi::{CommandResponse, Value},
};

/// topic存放数据的上限
const BROADCAST_CAPACITY: usize = 128;

/// 下一个 subscription id
static NEXT_ID: AtomicU32 = AtomicU32::new(1);

/// 获取下一个 subscript id
fn new_subscript_id() -> u32 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

pub trait Topic: Send + Sync + 'static {
    /// 订阅
    fn subscript(self, name: impl Into<String>) -> mpsc::Receiver<Arc<CommandResponse>>;
    /// 取消订阅
    fn unsubscribe(self, name: impl Into<String>, id: u32) -> Result<u32, KvError>;
    /// 往主题里发布一个数据
    fn publish(self, name: impl Into<String>, value: Arc<CommandResponse>);
}

/// 用于主题发布订阅的数据结构
#[derive(Default)]
pub struct BroadCaster {
    /// 所有主题的列表，<主题名字，订阅ID>
    topics: DashMap<String, DashSet<u32>>,
    /// 订阅 ID 和 channel 的发送端，<订阅ID，rx>
    subscriptions: DashMap<u32, mpsc::Sender<Arc<CommandResponse>>>,
}

impl BroadCaster {
    pub fn remove_subscription(&self, name: String, id: u32) -> Option<u32> {
        if let Some(v) = self.topics.get_mut(&name) {
            // 在 topics 表里找到 topic 的 subscription id，删除
            v.remove(&id);

            // 如果这个 topic 为空，则也删除 topic
            if v.is_empty() {
                info!("Topic: {:?} is deleted", &name);
                drop(v);
                self.topics.remove(&name);
            }
        }

        debug!("Subscription {} is removed!", id);
        // 在 subscription 表中同样删除
        self.subscriptions.remove(&id).map(|(id, _)| id)
    }
}

impl Topic for Arc<BroadCaster> {
    fn subscript(self, name: impl Into<String>) -> mpsc::Receiver<Arc<CommandResponse>> {
        let id = {
            let entry = self.topics.entry(name.into()).or_default();
            let id = new_subscript_id();
            entry.value().insert(id);
            id
        };
        // 生成一个 mpsc channel
        let (tx, rx) = mpsc::channel(BROADCAST_CAPACITY);
        let v: Value = (id as i64).into();

        let tx_cloned = tx.clone();
        // 立刻发送subscription id 到 rx
        tokio::spawn(async move {
            if let Err(e) = tx_cloned.send(Arc::new(v.into())).await {
                warn!("Failed to send subscription id: {}. Error: {:?}", id, e)
            }
        });

        self.subscriptions.insert(id, tx);
        debug!("Subscription {} is added", id);
        // 返回 rx 给网络处理的上下文
        rx
    }

    fn unsubscribe(self, name: impl Into<String>, id: u32) -> Result<u32, KvError> {
        match self.remove_subscription(name.into(), id) {
            Some(id) => Ok(id),
            None => Err(KvError::NotFound(format!("subscription {}", id))),
        }
    }

    fn publish(self, name: impl Into<String>, value: Arc<CommandResponse>) {
        let name = name.into();
        tokio::spawn(async move {
            let mut ids = vec![];
            if let Some(topic) = self.topics.get(&name) {
                // 复制整个 topic 下所有的 subscription id
                // 这里我们每个 id 是 u32，如果一个 topic 下有 10k 订阅，复制的成本
                // 也就是 40k 堆内存（外加一些控制结构），所以效率不算差
                // 这也是为什么我们用 NEXT_ID 来控制 subscription id 的生成

                let subscriptions = topic.value().clone();
                // 尽快释放锁
                drop(topic);

                // 循环发送
                for id in subscriptions.into_iter() {
                    if let Some(tx) = self.subscriptions.get(&id) {
                        if let Err(e) = tx.send(value.clone()).await {
                            warn!("Publish to {} failed! error: {:?}", id, e);
                            // client 中断连接
                            ids.push(id);
                        }
                    }
                }
            }

            for id in ids {
                self.remove_subscription(name.clone(), id);
            }
        });
    }
}

#[cfg(test)]
mod topic_test {
    use std::sync::Arc;

    use crate::{
        assert_res_ok,
        pb::abi::Value,
        topic::{BroadCaster, Topic},
    };

    #[tokio::test]
    async fn pub_sub_should_work() {
        let broad = Arc::new(BroadCaster::default());
        let lobby = "lobby";

        // 订阅
        let mut stream1 = broad.clone().subscript(lobby);
        let mut stream2 = broad.clone().subscript(lobby);

        // 发布
        let value: Value = "hello".into();
        broad.clone().publish(lobby, Arc::new(value.clone().into()));

        // subscription 应该能收到 publish 的数据
        let id1: i64 = stream1.recv().await.unwrap().as_ref().try_into().unwrap();
        let id2: i64 = stream2.recv().await.unwrap().as_ref().try_into().unwrap();
        assert!(id1 != id2);

        let res1 = stream1.recv().await.unwrap();
        let res2 = stream2.recv().await.unwrap();
        assert_eq!(res1, res2);
        assert_res_ok(&res1, &[value.clone()], &[]);

        // 如果 subscription 取消订阅，则收不到新数据
        broad.clone().unsubscribe(lobby, id1 as _);

        // // 再一次发布
        let value: Value = "world".into();
        broad.clone().publish(lobby, Arc::new(value.clone().into()));

        assert!(stream1.recv().await.is_none());
        let res2 = stream2.recv().await.unwrap();
        assert_res_ok(&res2, &[value], &[]);
    }
}
