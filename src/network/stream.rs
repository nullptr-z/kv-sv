use super::frame::{read_fame, FrameCoder};
use crate::error::{IOError, KvError};
use bytes::BytesMut;
use futures::{ready, FutureExt, Sink, Stream};
use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite};

pub struct ProstStream<S, In, Out> {
    // inner stream
    stream: S,
    // write buffer
    wbuf: BytesMut,
    // 写入了多少字节
    written: usize,
    // read buffer
    rbuf: BytesMut,

    // 幽灵类型
    _in: PhantomData<In>,
    _out: PhantomData<Out>,
}

impl<S, In, Out> ProstStream<S, In, Out>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            wbuf: BytesMut::new(),
            written: 0,
            rbuf: BytesMut::new(),
            _in: PhantomData::default(),
            _out: PhantomData::default(),
        }
    }
}

// 读取时，返回In
impl<S, In, Out> Stream for ProstStream<S, In, Out>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    In: Unpin + Send + FrameCoder,
    Out: Unpin + Send,
{
    type Item = Result<In, KvError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // 上一次调用结束后，应该情况 rbuf
        assert!(self.rbuf.len() == 0);
        // 把 rbuf 分离出来,摆脱 self 的引用
        let mut rbuf = self.rbuf.split_off(0);
        // 使用 read_frame 来获取数据 Frame
        let fut = read_fame(&mut self.stream, &mut rbuf);
        // 因为 poll_xxx() 方法已经是 async/await 的底层 API 实现，
        // 所以我们在 poll_xxx() 方法中，是不能直接使用异步函数的；
        // 需要把它看作一个 future，然后调用 future 的 poll 函数。
        // 因为 future 是一个 trait，所以需要 Box 将其处理成一个在堆上的 trait object，
        // 这样就可以调用 FutureExt 的 poll_unpin() 方法了。Box::pin 会生成 Pin<Box>。
        ready!(Box::pin(fut).poll_unpin(cx))?;
        // 把拿到的 Frame 合并回 rbuf
        self.rbuf.unsplit(rbuf);
        // 解析这个新拿到的这个Frame
        Poll::Ready(Some(In::decode_frame(&mut self.rbuf)))
    }
}

// 调用Send()时会把 Out 发送出去
impl<S, In, Out> Sink<&Out> for ProstStream<S, In, Out>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    In: Unpin + Send,
    Out: Unpin + Send + FrameCoder,
{
    type Error = KvError;

    /// 不做背压，直接返回 Poll::Read
    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: &Out) -> Result<(), Self::Error> {
        let this = self.get_mut();
        item.encode_frame(&mut this.wbuf)?;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        // Write to stream in a loop
        while this.written != this.wbuf.len() {
            let n = ready!(Pin::new(&mut this.stream).poll_write(cx, &this.wbuf[this.written..]))
                .to_error()?;
            this.written += n;
        }

        // 使用完了，重置buf，为下次使用做准备
        this.wbuf.clear();
        this.written = 0;
        // 调用stream的 poll_flush确保写入
        ready!(Pin::new(&mut this.stream).poll_flush(cx)).to_error()?;

        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // 调用 stream的 poll_flush确保写入
        ready!(self.as_mut().poll_flush(cx))?;
        // 调用 stream的 poll_shutdown,确保 stream关闭
        ready!(Pin::new(&mut self.stream).poll_shutdown(cx)).to_error()?;
        Poll::Ready(Ok(()))
    }
}

/// 一般来说，为异步操作而创建的数据结构，如果使用了泛型参数，那么只要内部没有自引用数据，就应该实现 Unpin。
/// 这会给别人在使用这些的代码时带来很多方便
impl<S, In, Out> Unpin for ProstStream<S, In, Out> where S: Unpin {}

// 自己去实现Stream、Sink感受下

#[cfg(test)]
mod stream_tests {
    use anyhow::Result;
    use bytes::BytesMut;
    use futures::{SinkExt, StreamExt};

    use crate::{
        pb::abi::{CommandRequest, CommandResponse},
        stream::ProstStream,
        test_utils::DummyStream,
    };

    #[tokio::test]
    async fn prost_stream_should_work() -> Result<()> {
        let buf = BytesMut::new();
        let stream = DummyStream { buf };
        let mut stream = ProstStream::<_, CommandRequest, CommandRequest>::new(stream);
        let cmd = CommandRequest::new_hdel("t1", "k1");
        stream.send(&cmd.to_owned()).await?;
        if let Some(Ok(s)) = stream.next().await {
            assert_eq!(s, cmd);
        } else {
            assert!(false);
        }
        Ok(())
    }
}
