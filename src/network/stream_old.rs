pub mod stream;
pub mod tls;

mod frame;
use self::frame::{read_fame, FrameCoder};
use crate::{
    error::{IOError, KvError},
    pb::abi::{CommandRequest, CommandResponse},
    Service, Storage,
};
use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::info;

/// S: 各种协议。protocol: TPC UDP WS HTTP TLS and Customize
pub struct ProstServerStream<S, D> {
    stream: S,
    service: Service<D>,
}

pub struct ProstClientStream<S> {
    stream: S,
}

impl<S, F, D> ProstStreams<S, F> for ProstServerStream<S, D>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    F: FrameCoder,
{
    fn get_stream(&mut self) -> &mut S {
        &mut self.stream
    }
}

impl<S, F> ProstStreams<S, F> for ProstClientStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    F: FrameCoder,
{
    fn get_stream(&mut self) -> &mut S {
        &mut self.stream
    }
}

trait ProstStreams<S, F>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    F: FrameCoder,
{
    // send message package
    async fn send(&mut self, msg: F) -> Result<(), KvError> {
        let mut buf = BytesMut::new();
        msg.encode_frame(&mut buf)?;
        let encode = buf.freeze();
        self.get_stream()
            .write_all(encode.as_ref())
            .await
            .to_error()?;
        Ok(())
    }

    // receiver response
    async fn recv(&mut self) -> Result<F, KvError> {
        let mut buf = BytesMut::new();
        let stream = &mut self.get_stream();
        read_fame(stream, &mut buf).await?;
        F::decode_frame(&mut buf)
    }

    fn get_stream(&mut self) -> &mut S;
}

impl<S, D> ProstServerStream<S, D>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    D: Storage,
{
    pub fn new(stream: S, service: Service<D>) -> Self {
        Self { stream, service }
    }

    pub async fn process(mut self) -> Result<(), KvError> {
        while let Ok(cmd) = self.recv().await {
            info!("Got a new command: {:?}", cmd);
            let res = self.service.execute(cmd);
            self.send(res).await?;
        }

        Ok(())
    }
}

impl<S> ProstClientStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(stream: S) -> Self {
        Self { stream }
    }

    pub async fn execute(&mut self, cmd: CommandRequest) -> Result<CommandResponse, KvError> {
        self.send(cmd).await?;

        Ok(self.recv().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{assert_res_ok, pb::abi::Value, service_builder::ServiceBuilder};
    use anyhow::Result;
    use bytes::Bytes;
    use std::net::SocketAddr;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn client_server_basic_communication_should_work() -> anyhow::Result<()> {
        let addr = start_server().await?;
        let stream = TcpStream::connect(addr).await?;
        let mut client = ProstClientStream::new(stream);

        // 发送 HSET，等待回应
        let cmd = CommandRequest::new_hset("t1", "k1", "v1");
        let res = client.execute(cmd).await.unwrap();

        // 第一次 HSET 服务器应该返回 None
        assert_res_ok(res, &[Value::default()], &[]);

        // 再发一个 HSET
        let cmd = CommandRequest::new_hget("t1", "k1");
        let res = client.execute(cmd).await?;

        // 服务器应该返回上一次的结果
        assert_res_ok(res, &["v1".into()], &[]);

        Ok(())
    }

    #[tokio::test]
    async fn client_server_compression_should_work() -> anyhow::Result<()> {
        let addr = start_server().await?;

        let stream = TcpStream::connect(addr).await?;
        let mut client = ProstClientStream::new(stream);

        let v: Value = Bytes::from(vec![0u8; 16384]).into();
        let cmd = CommandRequest::new_hset("t2", "k2", v.clone());
        let res = client.execute(cmd).await?;

        assert_res_ok(res, &[Value::default()], &[]);

        let cmd = CommandRequest::new_hget("t2", "k2");
        let res = client.execute(cmd).await?;

        assert_res_ok(res, &[v], &[]);

        Ok(())
    }

    async fn start_server() -> Result<SocketAddr> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let service: Service = ServiceBuilder::default().finish();
                let server = ProstServerStream::new(stream, service);
                tokio::spawn(server.process());
            }
        });

        Ok(addr)
    }
}

#[cfg(test)]
pub mod test_utils {
    use std::task::{Context, Poll};

    use bytes::{BufMut, BytesMut};
    use tokio::io::{AsyncRead, AsyncWrite};

    pub struct DummyStream {
        pub buf: BytesMut,
    }

    impl AsyncRead for DummyStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let len = buf.capacity();
            let data = self.get_mut().buf.split_to(len);
            buf.put_slice(&data);
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for DummyStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
            self.get_mut().buf.put_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }
    }
}
