use std::sync::Arc;

use crate::{
    memory::MemTable,
    pb::abi::{CommandRequest, CommandResponse},
    Service, Storage,
};

pub struct ServiceBuilder<Store> {
    pub store: Store,
    /// 当服务器收到 CommandRequest 时触发
    pub on_received: Vec<fn(&CommandRequest)>,
    /// 当服务器处理完 CommandRequest 得到 CommandResponse 时触发
    pub on_executed: Vec<fn(&CommandResponse)>,
    /// 在服务器发送 CommandResponse 之前触发。注意这个接口提供的是 &mut CommandResponse，这样事件的处理者可以根据需要，在发送前，修改 CommandResponse。
    pub on_before_send: Vec<fn(&mut CommandResponse)>,
    /// 在服务器发送完 CommandResponse 后触发
    pub on_after_send: Vec<fn()>,
}

impl<Store: Storage> ServiceBuilder<Store> {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            on_received: Vec::new(),
            on_executed: Vec::new(),
            on_before_send: Vec::new(),
            on_after_send: Vec::new(),
        }
    }

    pub fn fn_received(mut self, f: fn(&CommandRequest)) -> Self {
        self.on_received.push(f);
        self
    }
    pub fn fn_executed(mut self, f: fn(&CommandResponse)) -> Self {
        self.on_executed.push(f);
        self
    }
    pub fn fn_before_send(mut self, f: fn(&mut CommandResponse)) -> Self {
        self.on_before_send.push(f);
        self
    }
    pub fn fn_after_send(mut self, f: fn()) -> Self {
        self.on_after_send.push(f);
        self
    }

    pub fn finish(self) -> Service<Store> {
        Service {
            inner: Arc::new(self),
            broadcaster: Default::default(),
        }
    }
}

// impl<Store> From<ServiceBuilder<Store>> for Service<Store> {
//     fn from(inner: ServiceBuilder<Store>) -> Self {
//         Service {
//             inner: Arc::new(inner),
//         }
//     }
// }

impl Default for ServiceBuilder<MemTable> {
    fn default() -> Self {
        Self {
            store: MemTable::default(),
            on_received: Default::default(),
            on_executed: Default::default(),
            on_before_send: Default::default(),
            on_after_send: Default::default(),
        }
    }
}

#[cfg(test)]
mod builder_tests {
    use hyper::StatusCode;
    use tokio_stream::StreamExt;
    use tracing::info;

    use crate::{
        pb::abi::{CommandRequest, CommandResponse, Value},
        service::service_builder::ServiceBuilder,
        Service,
    };

    #[tokio::test]
    async fn event_registration_should_work() {
        fn b(cmd: &CommandRequest) {
            info!("Got {:?}", cmd);
        }
        fn c(res: &CommandResponse) {
            info!("{:?}", res);
        }
        fn d(res: &mut CommandResponse) {
            res.status = StatusCode::CREATED.as_u16() as _;
        }
        fn e() {
            info!("Data is sent");
        }

        let service: Service = ServiceBuilder::default()
            .fn_received(|_: &CommandRequest| {})
            .fn_received(b)
            .fn_executed(c)
            .fn_before_send(d)
            .fn_after_send(e)
            .finish();

        let mut res = service.execute(CommandRequest::new_hset("t1", "k1", "v1"));
        let res = res.next().await.unwrap().as_ref().to_owned();
        assert_eq!(res.status, StatusCode::CREATED.as_u16() as _);
        assert_eq!(res.message, "");
        assert_eq!(res.values, vec![Value::default()]);
    }
}
