use anyhow::Result;
use kv_db::{
    config::ClientConfig, error::KvError, pb::abi::CommandRequest, start_client_with_config,
    ProstClientStream,
};
use std::time::Duration;
use tokio::time;
use tokio_util::compat::Compat;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let config: Result<ClientConfig, toml::de::Error> =
        toml::from_str(include_str!("../fixtures/client.conf"));
    let mut yamux = start_client_with_config(config?).await?;
    let mut client = yamux.open_stream().await?;

    // send cmd
    let cmd = CommandRequest::new_hset("table1", "key2", "hahah");
    let data = client.execute(&cmd).await?;
    info!("Got response {:?}", data);
    // let cmd = CommandRequest::new_hgetall("table1"); //, "key", "my client");
    // let data = client.execute(cmd).await?;
    // info!("Got response {:?}", data);

    // 分成部分是为了测试yamux多路复用
    // Pub
    let topic = "lobby1";
    start_publishing(yamux.open_stream().await?, topic)?;
    //Sub
    let cmd = CommandRequest::new_subscribe(topic);
    let stream = client.execute_streaming(&cmd).await?;
    let id = stream.id;
    //UnSub
    start_unsubscribe(yamux.open_stream().await?, topic, id)?;

    Ok(())
}

fn start_publishing(
    mut stream: ProstClientStream<Compat<yamux::Stream>>,
    name: &str,
) -> Result<(), KvError> {
    let cmd = CommandRequest::new_publish(name, vec![1.into(), 2.into(), "hello".into()]);
    tokio::spawn(async move {
        time::sleep(Duration::from_millis(1000)).await;
        let res = stream.execute(&cmd).await.unwrap();
        println!("Finished publishing: {:?}", res);
    });

    Ok(())
}

fn start_unsubscribe(
    mut stream: ProstClientStream<Compat<yamux::Stream>>,
    name: &str,
    id: u32,
) -> Result<(), KvError> {
    let cmd = CommandRequest::new_unsubscribe(name, id as _);
    tokio::spawn(async move {
        time::sleep(Duration::from_millis(2000)).await;
        let res = stream.execute(&cmd).await.unwrap();
        println!("Finished unsubscribing: {:?}", res);
    });

    Ok(())
}
