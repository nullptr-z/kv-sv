use futures::{future, Future, TryStreamExt};
use std::marker::PhantomData;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use yamux::{Config, Connection, ConnectionError, Control, Mode};

use crate::ProstClientStream;

pub struct YamuxCtrl<S> {
    ctrl: Control,
    _conn: PhantomData<S>,
}

impl<S> YamuxCtrl<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn new<F, Fut>(stream: S, config: Option<Config>, is_client: bool, f: F) -> Self
    where
        F: FnMut(yamux::Stream) -> Fut,
        F: Send + 'static,
        Fut: Future<Output = Result<(), ConnectionError>> + Send + 'static,
    {
        let mode = if is_client {
            Mode::Client
        } else {
            Mode::Server
        };

        // Create config；创建 config
        let mut config = config.unwrap_or_default();
        config.set_window_update_mode(yamux::WindowUpdateMode::OnRead);
        // yamux::Stream were used trait of futures,So needs convert to trait of tokio by compat()
        // 创建 config，yamux::Stream 使用的是 futures 的 trait 所以需要 compat() 到 tokio 的 trait
        let conn = Connection::new(stream.compat(), config, mode);
        // Create yamux ctrl；创建 yamux ctrl
        let ctrl = conn.control();
        // pull all stream data；pull 所有 stream 下的数据
        tokio::spawn(yamux::into_stream(conn).try_for_each_concurrent(None, f));

        Self {
            ctrl,
            _conn: PhantomData::default(),
        }
    }

    pub fn new_client(stream: S, config: Option<Config>) -> Self {
        Self::new(stream, config, true, |_s| future::ready(Ok(())))
    }

    pub fn new_server<F, Fut>(stream: S, config: Option<Config>, f: F) -> Self
    where
        F: FnMut(yamux::Stream) -> Fut,
        F: Send + 'static,
        Fut: Future<Output = Result<(), ConnectionError>> + Send + 'static,
    {
        Self::new(stream, config, false, f)
    }

    pub async fn open_stream(
        &mut self,
    ) -> Result<ProstClientStream<Compat<yamux::Stream>>, ConnectionError> {
        let stream = self.ctrl.open_stream().await?;
        Ok(ProstClientStream::new(stream.compat()))
    }
}

#[cfg(test)]
mod multiplex_tests {
    use anyhow::Result;

    #[test]
    fn yamux_ctrl_client_server_should_work() -> Result<()> {
        // 创建使用了TLS的yamux server
        // let acceptor=tls_acceptor

        Ok(())
    }
}
