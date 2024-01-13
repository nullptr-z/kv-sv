use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::{SinkExt, StreamExt};
use hyper::StatusCode;
use kv_db::pb::abi::{CommandRequest, CommandResponse};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9876";
    let listner = TcpListener::bind(addr).await?;
    info!("Listener on {}", addr);

    loop {
        let (stream, addr) = listner.accept().await?;
        info!("Client {:?} question connected", addr);
        tokio::spawn(async move {
            // 使用prost协议来封包
            let mut service =
                AsyncProstStream::<_, CommandRequest, CommandResponse, _>::from(stream).for_async();
            while let Some(Ok(msg)) = service.next().await {
                info!("Got a new command {:?}", msg);
                // 创建一个404响应给客户端
                let mut resp = CommandResponse::default();
                resp.status = StatusCode::NOT_FOUND.as_u16() as _;
                resp.message = "Not Found".into();
                service.send(resp).await.unwrap();
            }
            info!("Client {:?} disconnected", addr);
        });
    }
}
