pub mod config;
pub mod error;
pub mod network;
pub mod pb;
pub mod service;
pub mod storage;

use std::time::Duration;

pub use network::*;
pub use service::*;
pub use storage::*;

use anyhow::Result;
use config::{ClientConfig, ServerConfig};
use network::tls::{TlsClientConnector, TlsServerAcceptor};
use storage::{memory::MemTable, sled_db::SledDB};
use tokio::{
    net::{TcpListener, TcpStream},
    time,
};
use tokio_util::compat::FuturesAsyncReadCompatExt as _;
use tracing::info;

use crate::{multiplex::YamuxCtrl, service_builder::ServiceBuilder};

pub async fn start_client_with_config(
    config: ClientConfig,
) -> Result<YamuxCtrl<tokio_rustls::client::TlsStream<TcpStream>>> {
    let addr = &config.general.addr;
    let tls = &config.tls;
    let identity = tls
        .identity
        .as_ref()
        .map(|(cert, key)| (key.as_str(), cert.as_str()));
    let connector = TlsClientConnector::new(&tls.domain, identity, tls.ca.as_deref())?;
    let stream = TcpStream::connect(addr).await?;
    let stream = connector.connect(stream).await?;
    Ok(YamuxCtrl::new_client(stream, None))
}

/// 通过配置文件创建KV Service
pub async fn start_server_with_config(config: ServerConfig) -> Result<()> {
    match &config.storage {
        config::StorageConfig::MemTable => start_server(MemTable::default(), config).await?,
        config::StorageConfig::SledDB(path) => start_server(SledDB::new(path), config).await?,
    };

    Ok(())
}

async fn start_server<Store: Storage>(store: Store, config: ServerConfig) -> Result<()> {
    let service = ServiceBuilder::new(store).finish();
    let tls = &config.tls;
    let tls = TlsServerAcceptor::new(&tls.cert, &tls.key, tls.ca.as_deref())?;
    let addr = &config.general.addr;
    let listener = TcpListener::bind(addr).await?;
    info!("Start listening on{}", addr);

    // loop {
    //     let (tcp_stream, addr) = listener.accept().await?;
    //     info!("Clietn {:?} connected", addr);
    //     // 使用TLS协议包装TCP
    //     let tls_stream = tls.accept(tcp_stream).await?;
    //     let service = service.clone();
    //     YamuxCtrl::new_server(tls_stream, None, move |stream| {
    //         // 使用Post序列化数据流
    //         let stream = ProstServerStream::new(stream.compat(), service.clone());
    //         async move {
    //             // 人为延迟
    //             // time::sleep(Duration::from_millis(100)).await;
    //             stream.process().await.unwrap();
    //             Ok(())
    //         }
    //     });
    // }
    loop {
        let (tcp_stream, addr) = listener.accept().await?;
        info!("Clietn {:?} connected", addr);
        let tls = tls.clone();
        // 使用TLS协议包装TCP
        let service = service.clone();
        tokio::spawn(async move {
            let tls_stream = tls.accept(tcp_stream).await.unwrap();
            YamuxCtrl::new_server(tls_stream, None, move |stream| {
                let service = service.clone();
                async move {
                    let stream = ProstServerStream::new(stream.compat(), service.clone());
                    stream.process().await.unwrap();
                    Ok(())
                }
            });
        });
    }
}
