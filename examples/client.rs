use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::{SinkExt, StreamExt};
use kv_db::pb::abi::{CommandRequest, CommandResponse};
use rand::Rng;
use tokio::net::TcpStream;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mut rng = rand::thread_rng();
    let value: u32 = rng.gen_range(1..=100);

    // 创建一个Hset命令
    let set_cmd = CommandRequest::new_hset("table1", "k1", format!("value: {}", value));
    // let set_cmd2 = CommandRequest::new_hset("table1", "k2", format!("v2: {}", value).into());
    // let get_cmd = CommandRequest::new_hget("table1", "k1");
    // let get_cmd2 = CommandRequest::new_hget("table1", "k2");
    let get_all_cmd = CommandRequest::new_hgetall("table1");

    let addr = "127.0.0.1:9876";
    let stream = TcpStream::connect(addr).await?;

    // 使用prost协议来封包
    let mut client =
        AsyncProstStream::<_, CommandResponse, CommandRequest, _>::from(stream).for_async();

    client.send(set_cmd).await?;
    // client.send(set_cmd2).await?;
    // client.send(get_cmd).await?;
    // client.send(get_cmd2).await?;
    client.send(get_all_cmd).await?;

    if let Some(Ok(data)) = client.next().await {
        info!("Got response {:?}", data)
    }

    Ok(())
}
