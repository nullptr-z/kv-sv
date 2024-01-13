use anyhow::Result;
use criterion::{criterion_group, criterion_main, Criterion};
use kv_db::{
    config::{ClientConfig, ServerConfig, StorageConfig},
    multiplex::YamuxCtrl,
    pb::abi::CommandRequest,
    start_client_with_config, start_server_with_config,
};
use rand::seq::SliceRandom;
use std::time::Duration;
use tokio::{net::TcpStream, runtime::Builder, time};
use tokio_rustls::client::TlsStream;
use tokio_stream::StreamExt;
use tracing::{info, span};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// 启动测试服务器
async fn start_server() -> Result<()> {
    let addr = "127.0.0.1:9999";
    let mut config: ServerConfig = toml::from_str(include_str!("../fixtures/server.conf"))?;
    config.general.addr = addr.into();
    config.storage = StorageConfig::MemTable;

    tokio::spawn(async move {
        start_server_with_config(config).await.unwrap();
    });

    Ok(())
}

/// 链接测试服务
async fn connect() -> Result<YamuxCtrl<TlsStream<TcpStream>>> {
    let addr = "127.0.0.1:9999";
    let mut config: ClientConfig = toml::from_str(include_str!("../fixtures/client.conf"))?;
    config.general.addr = addr.into();
    Ok(start_client_with_config(config).await?)
}

async fn start_subscribers(topic: &'static str) -> Result<()> {
    let mut ctrl = connect().await?;
    let stream = ctrl.open_stream().await?;
    info!("C(subscriber): stream opened");
    let cmd = CommandRequest::new_subscribe(topic);
    tokio::spawn(async move {
        let mut stream = stream.execute_streaming(&cmd).await.unwrap();
        while let Some(Ok(data)) = stream.next().await {
            drop(data);
        }
    });

    Ok(())
}

async fn start_publishers(topic: &'static str, values: &'static [&'static str]) -> Result<()> {
    let mut ctrl = connect().await.unwrap();
    let mut stream = ctrl.open_stream().await.unwrap();
    info!("C(publisher): stream opened");

    // 从values中随机选一个元素
    let mut rng = rand::thread_rng();
    let v = values.choose(&mut rng).unwrap();

    let cmd = CommandRequest::new_publish(topic, vec![(*v).into()]);
    stream.execute(&cmd).await.unwrap();
    Ok(())
}

fn pubsub(c: &mut Criterion) {
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name("kv-bench")
        .install_simple()
        .unwrap();
    let opentelemeetry = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(opentelemeetry)
        .init();

    let root = span!(tracing::Level::INFO, "app_start", work_units = 2);
    let _enter = root.enter();

    // Create tokio runtime
    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("pubsub")
        .enable_all()
        .build()
        .unwrap();
    let values = &["Hello", "Zheng", "Goodbye", "World"];
    let topic = "lobby";

    // Run service and 100 subscriber, for a testing
    runtime.block_on(async {
        eprint!("preparing server and subscriber");
        start_server().await.unwrap();
        time::sleep(Duration::from_millis(50)).await;
        for _ in 0..100 {
            start_subscribers(topic).await.unwrap();
            eprint!(".");
        }
        eprintln!("Done!");
    });

    // run benchmark
    c.bench_function("publishing", move |b| {
        b.to_async(&runtime)
            .iter(|| async { start_publishers(topic, values).await })
    });
}

criterion_group! {
  name=benches;
  config=Criterion::default().sample_size(10);
  targets=pubsub
}

criterion_main!(benches);
