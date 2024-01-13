use anyhow::Result;
use kv_db::{config::ServerConfig, start_server_with_config};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let config: Result<ServerConfig, toml::de::Error> =
        toml::from_str(include_str!("../fixtures/server.conf"));
    start_server_with_config(config?).await
}

// use anyhow::Result;
// use kv_db::multiplex::YamuxCtrl;
// use kv_db::sled_db::SledDB;
// use kv_db::tls::TlsServerAcceptor;
// use kv_db::{service_builder::ServiceBuilder, ProstServerStream};
// use tokio::net::TcpListener;
// use tokio_util::compat::FuturesAsyncReadCompatExt;
// use tracing::info;

// #[tokio::main]
// async fn main() -> Result<()> {
//     tracing_subscriber::fmt::init();
//     let addr = "127.0.0.1:9876";
//     // tudo: 从配置文件中读取
//     let server_cert = include_str!("../fixtures/server.cert");
//     let server_key = include_str!("../fixtures/server.key");

//     let tls = TlsServerAcceptor::new(server_cert, server_key, None)?;
//     let service = ServiceBuilder::new(SledDB::new("db/")).finish();
//     let listener = TcpListener::bind(addr).await?;
//     info!("Start listening on{}", addr);

//     loop {
//         // let tls = tls.clone();
//         let (tcp_stream, addr) = listener.accept().await?;
//         info!("Clietn {:?} connected", addr);
//         // 使用TLS协议包装TCP
//         let tls_stream = tls.accept(tcp_stream).await?;
//         let service = service.clone();
//         YamuxCtrl::new_server(tls_stream, None, move |stream| {
//             // 使用Post序列化数据流
//             let stream = ProstServerStream::new(stream.compat(), service.clone());
//             // tokio::spawn(async move { stream.process().await });
//             async move {
//                 stream.process().await.unwrap();
//                 Ok(())
//             }
//         });
//     }
// }
