use crate::error::KvError;
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ServerConfig {
    pub general: GeneralConfig,
    pub storage: StorageConfig,
    pub tls: ServerTlsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClientConfig {
    pub general: GeneralConfig,
    pub tls: ClientTlsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GeneralConfig {
    pub addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "args")]
pub enum StorageConfig {
    MemTable,
    SledDB(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ServerTlsConfig {
    pub cert: String,
    pub key: String,
    pub ca: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClientTlsConfig {
    pub domain: String,
    pub identity: Option<(String, String)>,
    pub ca: Option<String>,
}

impl ServerConfig {
    pub fn load(path: &str) -> Result<Self, KvError> {
        let config = fs::read_to_string(path)?;
        let config = toml::from_str(&config)?;
        Ok(config)
    }
}

#[cfg(test)]
mod config_tests {
    use crate::config::*;

    #[test]
    fn server_config_should_be_loaded() {
        let result: Result<ServerConfig, toml::de::Error> =
            toml::from_str(include_str!("../fixtures/server.conf"));
        assert!(result.is_ok());
    }

    #[test]
    fn client_config_should_be_loaded() {
        let result: Result<ClientConfig, toml::de::Error> =
            toml::from_str(include_str!("../fixtures/client.conf"));
        assert!(result.is_ok());
    }
}
