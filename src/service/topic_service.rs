use futures::{stream, Stream};
use std::{pin::Pin, sync::Arc};
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    pb::abi::{CommandResponse, Publish, Subscribe, Unsubscribe},
    topic::Topic,
};

pub type StreamingResponse = Pin<Box<dyn Stream<Item = Arc<CommandResponse>> + Send>>;
pub trait TopicService {
    fn execute(self, topic: impl Topic) -> impl Stream<Item = Arc<CommandResponse>> + Send;
}

impl TopicService for Subscribe {
    fn execute(self, topic: impl Topic) -> impl Stream<Item = Arc<CommandResponse>> + Send {
        let rx = topic.subscript(self.topic);
        ReceiverStream::new(rx)
    }
}

impl TopicService for Unsubscribe {
    fn execute(self, topic: impl Topic) -> impl Stream<Item = Arc<CommandResponse>> + Send {
        let resp = match topic.unsubscribe(self.topic, self.id) {
            Ok(_) => CommandResponse::ok(),
            Err(e) => e.into(),
        };
        stream::once(async { Arc::new(resp) })
    }
}

impl TopicService for Publish {
    fn execute(self, topic: impl Topic) -> impl Stream<Item = Arc<CommandResponse>> + Send {
        topic.publish(self.topic, Arc::new(self.data.into()));
        stream::once(async { Arc::new(CommandResponse::ok()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        assert_res_error, assert_res_ok, pb::abi::CommandRequest, service::dispatch_stream,
        topic::BroadCaster,
    };
    use futures::StreamExt;
    use std::{convert::TryInto, time::Duration};
    use tokio::time;

    #[tokio::test]
    async fn dispatch_publish_should_work() {
        let topic = Arc::new(BroadCaster::default());
        let cmd = CommandRequest::new_publish("lobby", vec!["hello".into()]);
        let mut res = dispatch_stream(cmd, topic);
        let data = res.next().await.unwrap();
        assert_res_ok(&data, &[], &[]);
    }

    #[tokio::test]
    async fn dispatch_subscribe_should_work() {
        let topic = Arc::new(BroadCaster::default());
        let cmd = CommandRequest::new_subscribe("lobby");
        let mut res = dispatch_stream(cmd, topic);
        let id: i64 = res.next().await.unwrap().as_ref().try_into().unwrap();
        assert!(id > 0);
    }

    #[tokio::test]
    async fn dispatch_subscribe_abnormal_quit_should_be_removed_on_next_publish() {
        let topic = Arc::new(BroadCaster::default());
        let id = {
            let cmd = CommandRequest::new_subscribe("lobby");
            let mut res = dispatch_stream(cmd, topic.clone());
            let id: i64 = res.next().await.unwrap().as_ref().try_into().unwrap();
            drop(res);
            id as u32
        };

        // publish 时，这个 subscription 已经失效，所以会被删除
        let cmd = CommandRequest::new_publish("lobby", vec!["hello".into()]);
        dispatch_stream(cmd, topic.clone());
        time::sleep(Duration::from_millis(10)).await;

        // 如果再尝试删除，应该返回 KvError
        let result = topic.unsubscribe("lobby", id);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn dispatch_unsubscribe_should_work() {
        let topic = Arc::new(BroadCaster::default());
        let cmd = CommandRequest::new_subscribe("lobby");
        let mut res = dispatch_stream(cmd, topic.clone());
        let id: i64 = res.next().await.unwrap().as_ref().try_into().unwrap();

        let cmd = CommandRequest::new_unsubscribe("lobby", id as _);
        let mut res = dispatch_stream(cmd, topic);
        let data = res.next().await.unwrap();

        assert_res_ok(&data, &[], &[]);
    }

    #[tokio::test]
    async fn dispatch_unsubscribe_random_id_should_error() {
        let topic = Arc::new(BroadCaster::default());

        let cmd = CommandRequest::new_unsubscribe("lobby", 9527);
        let mut res = dispatch_stream(cmd, topic);
        let data = res.next().await.unwrap();

        assert_res_error(&data, 404, "Not found for key: subscription 9527");
    }
}
