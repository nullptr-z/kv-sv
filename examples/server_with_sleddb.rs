use std::ops::Deref;

use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::{SinkExt, StreamExt};
use kv_db::{
    pb::abi::{CommandRequest, CommandResponse},
    service_builder::ServiceBuilder,
    sled_db::SledDB,
    Service,
};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9876";
    let listner = TcpListener::bind(addr).await?;
    info!("Listener on {}", addr);

    let server: Service<SledDB> = ServiceBuilder::new(SledDB::new("kv-sv/db_file"))
        .fn_before_send(|cmd| {
            if cmd.message.is_empty() {
                cmd.message = "altered. Origin message is empty".into()
            } else {
                cmd.message = format!("altered.{}", cmd.message)
            }
        })
        .finish();

    loop {
        let (stream, addr) = listner.accept().await?;
        info!("Client {:?} question connected", addr);

        let svc = server.clone();
        tokio::spawn(async move {
            // 使用prost协议来封包
            let mut service =
                AsyncProstStream::<_, CommandRequest, CommandResponse, _>::from(stream).for_async();
            while let Some(Ok(msg)) = service.next().await {
                info!("Got a new command {:?}", msg);
                let mut resp = svc.clone().execute(msg);
                while let Some(data) = resp.next().await {
                    info!("execute command response {:?}", data);
                    service.send(data.as_ref().clone()).await.unwrap();
                }
            }
            info!("Client {:?} disconnected", addr);
        });
    }
}
