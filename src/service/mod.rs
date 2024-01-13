mod command_service;
pub mod notify;
pub mod service_builder;
pub mod topic;
pub mod topic_service;

use crate::{
    error::KvError,
    memory::MemTable,
    pb::abi::{command_request::RequestData, CommandRequest, CommandResponse},
    Storage,
};
use command_service::*;
use futures::{stream, Stream};
use std::{ops::Deref, sync::Arc};
use topic_service::*;
use tracing::info;

/// 可以跨线程，可以调用 execute 来执行某个 CommandRequest 命令，返回 CommandResponse。
pub struct Service<Store = MemTable> {
    inner: Arc<ServiceBuilder<Store>>,
    broadcaster: Arc<BroadCaster>,
}

impl<Store: Storage> Service<Store> {
    pub fn execute(&self, cmd: CommandRequest) -> impl Stream<Item = Arc<CommandResponse>> + Send {
        info!("God request: {:?}", &cmd);
        self.on_received.notify(&cmd);
        let mut resp = dispatch(cmd.clone(), &self.store);
        if resp == CommandResponse::default() {
            dispatch_stream(cmd, self.broadcaster.clone())
        } else {
            info!("Executed response: {:?}", resp);
            self.on_executed.notify(&resp);
            self.on_before_send.notify(&mut resp);

            if !self.on_before_send.is_empty() {
                info!("Modified response: {:?}", resp);
            }
            Box::pin(stream::once(async { Arc::new(resp) }))
        }
    }
}

impl<Store> Clone for Service<Store> {
    fn clone(&self) -> Self {
        Service {
            inner: self.inner.clone(),
            broadcaster: self.broadcaster.clone(),
        }
    }
}

impl<Store> Deref for Service<Store> {
    type Target = Arc<ServiceBuilder<Store>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// 从 Request 中得到 Response，目前处理所有 HGET/HSET/HGETALL
fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
    match cmd.request_data {
        Some(RequestData::Hget(cmd)) => cmd.execute(store),
        Some(RequestData::Hgetall(cmd)) => cmd.execute(store),
        Some(RequestData::Hset(cmd)) => cmd.execute(store),
        None => KvError::InvalidCommand("Request has no data".into()).into(),
        // 没有做任何处理，尝试让之后的 dispatch_stream 处理
        _ => CommandResponse::default(),
        // _ => KvError::Internal("Not implemented yet".into()).into(),
    }
}

// fn dispatch_stream(
//     cmd: CommandRequest,
//     topic: impl Topic,
// ) -> impl Stream<Item = Arc<CommandResponse>> + Send {
//     match cmd.request_data {
//         Some(RequestData::Subscribe(cmd)) => cmd.execute(topic),
//         Some(RequestData::Unsubscribe(cmd)) => cmd.execute(topic),
//         Some(RequestData::Publish(cmd)) => cmd.execute(topic),
//         _ => unreachable!(), // 走到这里说明代码逻辑有问题，尽早改了才是
//     }
// }

fn dispatch_stream(cmd: CommandRequest, topic: impl Topic) -> StreamingResponse {
    match cmd.request_data {
        Some(RequestData::Subscribe(cmd)) => Box::pin(cmd.execute(topic)),
        Some(RequestData::Unsubscribe(cmd)) => Box::pin(cmd.execute(topic)),
        Some(RequestData::Publish(cmd)) => Box::pin(cmd.execute(topic)),
        _ => unreachable!(), // 走到这里说明代码逻辑有问题，尽早改了才是
    }
}

#[cfg(test)]
mod service_tests {
    use super::*;
    use crate::{
        memory::MemTable,
        pb::abi::{command_request::RequestData, CommandRequest, CommandResponse, Kvpair, Value},
        Storage,
    };

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("t1", "hello", "world");
        let res = dispatch(cmd.clone(), &store);
        assert_res_ok(res, &[Value::default()], &[]);

        let res = dispatch(cmd, &store);
        assert_res_ok(res, &["world".into()], &[]);
    }

    #[test]
    fn hget_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("score", "u1", 10);
        dispatch(cmd, &store);
        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[10.into()], &[]);
    }

    #[test]
    fn hgetall_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("score", "u1", 10),
            CommandRequest::new_hset("score", "u2", 8),
            CommandRequest::new_hset("score", "u3", 11),
            CommandRequest::new_hset("score", "u1", 6),
        ];
        for cmd in cmds {
            dispatch(cmd, &store);
        }
    }

    // 测试成功返回的结果
    fn assert_res_ok(mut res: CommandResponse, values: &[Value], pairs: &[Kvpair]) {
        res.pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(res.status, 200);
        assert_eq!(res.message, "");
        assert_eq!(res.values, values);
        assert_eq!(res.pairs, pairs);
    }

    // 测试失败返回的结果
    fn assert_res_error(res: CommandResponse, code: u32, msg: &str) {
        assert_eq!(res.status, code);
        assert!(res.message.contains(msg));
        assert_eq!(res.values, &[]);
        assert_eq!(res.pairs, &[]);
    }

    fn dispatch(mut cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
        match cmd.request_data.unwrap() {
            RequestData::Hget(v) => v.execute(store),
            RequestData::Hgetall(v) => v.execute(store),
            RequestData::Hset(v) => v.execute(store),
            _ => todo!(),
        }
    }
}

#[cfg(test)]
mod service_tests_2 {
    use std::thread;

    use futures::StreamExt;

    use crate::{
        assert_res_ok,
        pb::abi::{CommandRequest, Value},
        Service,
    };

    use super::service_builder::ServiceBuilder;

    #[tokio::test]
    async fn service_should_works() {
        // 我们需要一个 service 结构至少包含 Storage
        let service: Service = ServiceBuilder::default().finish();

        // service 可以运行在多线程环境下，它的 clone 应该是轻量级的
        let cloned = service.clone();

        // 创建一个线程，在 table t1 中写入 k1, v1
        let handle = thread::spawn(move || async move {
            let mut res = cloned.execute(CommandRequest::new_hset("t1", "k1", "v1"));
            let res = res.next().await.unwrap();
            assert_res_ok(&res, &[Value::default()], &[]);
        });
        handle.join().unwrap().await;

        // 在当前线程下读取 table t1 的 k1，应该返回 v1
        let mut res = service.execute(CommandRequest::new_hget("t1", "k1"));
        let res = res.next().await.unwrap();
        assert_res_ok(&res, &["v1".into()], &[]);
    }
}

#[cfg(test)]
use crate::pb::abi::{Kvpair, Value};

use self::{
    notify::{Notify, NotifyMut},
    service_builder::ServiceBuilder,
    topic::{BroadCaster, Topic},
};
// 测试成功返回的结果
#[cfg(test)]
pub fn assert_res_ok(res: &CommandResponse, values: &[Value], pairs: &[Kvpair]) {
    let mut sorted_pairs = res.pairs.clone();
    sorted_pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(res.status, 200);
    assert_eq!(res.message, "");
    assert_eq!(res.values, values);
    assert_eq!(sorted_pairs, pairs);
}

// 测试失败返回的结果
#[cfg(test)]
pub fn assert_res_error(res: &CommandResponse, code: u32, msg: &str) {
    println!("【 res.message 】==> {:?}", res.message);
    assert_eq!(res.status, code);
    assert!(res.message.contains(msg));
    assert_eq!(res.values, &[]);
    assert_eq!(res.pairs, &[]);
}
