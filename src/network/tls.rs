use crate::error::{CertError, IOError, KvError};
use anyhow::Result;
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::{
    client,
    rustls::{
        internal::pemfile, AllowAnyAuthenticatedClient, Certificate, ClientConfig, NoClientAuth,
        PrivateKey, RootCertStore, ServerConfig,
    },
    server,
    webpki::DNSNameRef,
    TlsAcceptor, TlsConnector,
};

/// KV Server 自己的 ALPN (Application-Layer Protocol Negotiation)
const ALPN_KV: &str = "kv";

/// 存放 TLS ServerConfig 并提供 accept 方法把底层的协议转换成 TLS
#[derive(Clone)]
pub struct TlsServerAcceptor {
    inner: Arc<ServerConfig>,
}

/// 存放 TLS Client 并提供 connect 方法把底层协议转换成 TLS
#[derive(Clone)]
pub struct TlsClientConnector {
    pub config: Arc<ClientConfig>,
    pub domain: Arc<String>,
}

impl TlsClientConnector {
    /// 加载 client cert/CA cert, 生成 ClientConfig
    pub fn new(
        domain: impl Into<String>,
        identity: Option<(&str, &str)>,
        server_ca: Option<&str>,
    ) -> Result<Self, KvError> {
        let mut config = ClientConfig::new();

        // 如果存在客户端证书，则加载
        if let Some((cert, key)) = identity {
            let certs = load_certs(cert)?;
            let key = load_key(key)?;
            config.set_single_client_cert(certs, key)?;
        }

        // 如果有签署服务器的 CA 证书，则加载它，这样服务器证书不在根证书链
        // 但是这个 CA 证书能验证它，也可以
        if let Some(cert) = server_ca {
            let mut buf = Cursor::new(cert);
            config.root_store.add_pem_file(&mut buf).unwrap();
        } else {
            // 加载本地信任的根证书链
            config.root_store = match rustls_native_certs::load_native_certs() {
                Ok(store) | Err((Some(store), _)) => store,
                Err((None, error)) => return Err(error.into()),
            };
        }

        Ok(Self {
            config: Arc::new(config),
            domain: Arc::new(domain.into()),
        })
    }

    /// 触发 TLS 协议，把底层的 stream 转换成 TLS stream
    pub async fn connect<S>(&self, stream: S) -> Result<client::TlsStream<S>, KvError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let dns = DNSNameRef::try_from_ascii_str(self.domain.as_str())?;

        // TlsStream 满足 AsyncRead + AsyncWrite + Unpin + Send 约束
        let stream = TlsConnector::from(self.config.clone())
            .connect(dns, stream)
            .await
            .to_error()?;

        Ok(stream)
    }
}

impl TlsServerAcceptor {
    /// 加载 server cert / CA cert, 生成 ServerConfig
    pub fn new(cert: &str, key: &str, client_ca: Option<&str>) -> Result<Self, KvError> {
        let certs = load_certs(cert)?;
        let key = load_key(key)?;

        let mut config = match client_ca {
            None => ServerConfig::new(NoClientAuth::new()),
            Some(cert) => {
                // if if client the cert is issueed  some a CA , then loading into chain of trust that a CA cert
                // 如果客户端证书是某个 CA 证书签发的，则把这个CA证书加载到信任链中
                let mut cert = Cursor::new(cert);
                let mut client_root_cert_store = RootCertStore::empty();
                client_root_cert_store
                    .add_pem_file(&mut cert)
                    .to_error("server", "cert")?;
                let client_auth = AllowAnyAuthenticatedClient::new(client_root_cert_store);
                ServerConfig::new(client_auth)
            }
        };

        // loading server cert; 加载服务器证书
        config.set_single_cert(certs, key)?;
        let protocols = ALPN_KV.as_bytes().to_vec();
        config.set_protocols(&[protocols]);

        Ok(Self {
            inner: Arc::new(config),
        })
    }

    // 触发 TLS 协议，把底层的 stream 转换成 TLS stream
    pub async fn accept<S>(&self, stream: S) -> Result<server::TlsStream<S>, KvError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let acceptor = TlsAcceptor::from(self.inner.clone());
        Ok(acceptor.accept(stream).await.to_error()?)
    }
}

fn load_certs(cert: &str) -> Result<Vec<Certificate>, KvError> {
    let mut cert = Cursor::new(cert);
    pemfile::certs(&mut cert).map_err(|_| KvError::ConvertError("server".into(), "cert"))
}

fn load_key(key: &str) -> Result<PrivateKey, KvError> {
    let mut cursor = Cursor::new(key);

    // first, trying to load private key using PKCS8
    if let Ok(mut keys) = pemfile::pkcs8_private_keys(&mut cursor) {
        if !keys.is_empty() {
            return Ok(keys.remove(0));
        }
    }

    // trying to load RSA again
    cursor.set_position(0);
    if let Ok(mut keys) = pemfile::rsa_private_keys(&mut cursor) {
        if !keys.is_empty() {
            return Ok(keys.remove(0));
        }
    }

    // unsupported private key type
    Err(KvError::CertificateParseError(
        "private".into(),
        "key".into(),
    ))
}

#[cfg(test)]
mod tests {

    use std::net::SocketAddr;

    use super::*;
    use anyhow::Result;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    const CA_CERT: &str = include_str!("../../fixtures/ca.cert");
    const CLIENT_CERT: &str = include_str!("../../fixtures/client.cert");
    const CLIENT_KEY: &str = include_str!("../../fixtures/client.key");
    const SERVER_CERT: &str = include_str!("../../fixtures/server.cert");
    const SERVER_KEY: &str = include_str!("../../fixtures/server.key");

    #[tokio::test]
    async fn tls_should_work() -> Result<()> {
        let ca = Some(CA_CERT);

        let addr = start_server(None).await?;

        let connector = TlsClientConnector::new("kvserver.acme.inc", None, ca)?;
        let stream = TcpStream::connect(addr).await?;
        let mut stream = connector.connect(stream).await?;
        stream.write_all(b"hello world!").await?;
        let mut buf = [0; 12];
        stream.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"hello world!");

        Ok(())
    }

    #[tokio::test]
    async fn tls_with_client_cert_should_work() -> Result<()> {
        let client_identity = Some((CLIENT_CERT, CLIENT_KEY));
        let ca = Some(CA_CERT);

        let addr = start_server(ca.clone()).await?;

        let connector = TlsClientConnector::new("kvserver.acme.inc", client_identity, ca)?;
        let stream = TcpStream::connect(addr).await?;
        let mut stream = connector.connect(stream).await?;
        stream.write_all(b"hello world!").await?;
        let mut buf = [0; 12];
        stream.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"hello world!");

        Ok(())
    }

    #[tokio::test]
    async fn tls_with_bad_domain_should_not_work() -> Result<()> {
        let addr = start_server(None).await?;

        let connector = TlsClientConnector::new("kvserver1.acme.inc", None, Some(CA_CERT))?;
        let stream = TcpStream::connect(addr).await?;
        let result = connector.connect(stream).await;
        assert!(result.is_err());

        Ok(())
    }

    async fn start_server(ca: Option<&str>) -> Result<SocketAddr> {
        let acceptor = TlsServerAcceptor::new(SERVER_CERT, SERVER_KEY, ca)?;

        let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = echo.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = echo.accept().await.unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            let mut buf = [0; 12];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        Ok(addr)
    }
}
