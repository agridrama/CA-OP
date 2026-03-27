use std::{
    env,
    path::{Path, PathBuf},
    time::Duration,
};

use config::{Config, ConfigError, Environment, File};
use omnipaxos_kv::common::{kv::{ClientId, NodeId}, utils::Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientConfig {
    pub location: String,
    pub client_id: ClientId,
    pub server_id: NodeId,
    pub server_address: String,
    pub requests: Vec<RequestInterval>,
    pub sync_time: Option<Timestamp>,
    pub summary_filepath: String,
    pub output_filepath: String,
}

impl ClientConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let config_file = match env::var("CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires CONFIG_FILE environment variable to be set"),
        };
        let config = Config::builder()
            .add_source(File::with_name(&config_file))
            // Add-in/overwrite settings with environment variables (with a prefix of OMNIPAXOS)
            .add_source(Environment::with_prefix("OMNIPAXOS").try_parsing(true))
            .build()?;
        let mut client_config: Self = config.try_deserialize()?;
        let config_dir = Path::new(&config_file)
            .parent()
            .unwrap_or_else(|| Path::new("."));
        client_config.summary_filepath =
            Self::resolve_output_path(config_dir, &client_config.summary_filepath);
        client_config.output_filepath =
            Self::resolve_output_path(config_dir, &client_config.output_filepath);
        Ok(client_config)
    }

    fn resolve_output_path(config_dir: &Path, filepath: &str) -> String {
        let path = Path::new(filepath);
        if path.is_absolute() {
            return filepath.to_string();
        }

        let resolved: PathBuf = config_dir.join(path);
        resolved.to_string_lossy().into_owned()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct RequestInterval {
    pub duration_sec: u64,
    pub requests_per_sec: u64,
    pub read_ratio: f64,
}

impl RequestInterval {
    pub fn get_read_ratio(&self) -> f64 {
        self.read_ratio
    }

    pub fn get_interval_duration(&self) -> Duration {
        Duration::from_secs(self.duration_sec)
    }

    pub fn get_request_delay(&self) -> Duration {
        if self.requests_per_sec == 0 {
            return Duration::from_secs(999999);
        }
        let delay_us = 1_000_000 / self.requests_per_sec;
        assert!(delay_us != 0);
        Duration::from_micros(delay_us)
    }
}
