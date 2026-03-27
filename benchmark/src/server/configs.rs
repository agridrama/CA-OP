use std::{
    env,
    path::{Path, PathBuf},
};

use config::{Config, ConfigError, Environment, File};
use omnipaxos_kv::{clock::simulator::ClockConfig, BenchmarkProtocol};
use omnipaxos_kv::omnipaxos_api::{
    util::{FlexibleQuorum, NodeId},
    ClusterConfig as OmnipaxosClusterConfig, OmniPaxosConfig,
    ServerConfig as OmnipaxosServerConfig,
};
use serde::{Deserialize, Serialize};

#[cfg(feature = "protocol-caop")]
type BenchmarkOwdConfig = omnipaxos_kv::omnipaxos_api::dom::OwdEstimatorConfig;

#[cfg(feature = "protocol-baseline")]
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct BenchmarkOwdConfig {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClusterConfig {
    pub nodes: Vec<NodeId>,
    pub node_addrs: Vec<String>,
    pub initial_leader: NodeId,
    pub initial_flexible_quorum: Option<FlexibleQuorum>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LocalConfig {
    pub location: Option<String>,
    #[serde(default)]
    pub protocol: BenchmarkProtocol,
    pub server_id: NodeId,
    pub listen_address: String,
    pub listen_port: u16,
    pub num_clients: usize,
    pub output_filepath: String,
    #[serde(default)]
    pub owd_config: BenchmarkOwdConfig,
    #[serde(default)]
    pub clock: ClockConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OmniPaxosKVConfig {
    #[serde(flatten)]
    pub local: LocalConfig,
    #[serde(flatten)]
    pub cluster: ClusterConfig,
}

impl Into<OmniPaxosConfig> for OmniPaxosKVConfig {
    fn into(self) -> OmniPaxosConfig {
        let cluster_config = OmnipaxosClusterConfig {
            configuration_id: 1,
            nodes: self.cluster.nodes,
            flexible_quorum: self.cluster.initial_flexible_quorum,
        };
        let server_config = OmnipaxosServerConfig {
            pid: self.local.server_id,
            leader_priority: if self.local.server_id == self.cluster.initial_leader {
                1
            } else {
                0
            },
            #[cfg(feature = "protocol-caop")]
            owd_config: self.local.owd_config,
            #[cfg(feature = "protocol-caop")]
            enable_fast_path: self.local.protocol.enable_fast_path(),
            ..Default::default()
        };
        OmniPaxosConfig {
            cluster_config,
            server_config,
        }
    }
}

impl OmniPaxosKVConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let local_config_file = match env::var("SERVER_CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires SERVER_CONFIG_FILE environment variable to be set"),
        };
        let cluster_config_file = match env::var("CLUSTER_CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires CLUSTER_CONFIG_FILE environment variable to be set"),
        };
        let config = Config::builder()
            .add_source(File::with_name(&local_config_file))
            .add_source(File::with_name(&cluster_config_file))
            // Add-in/overwrite settings with environment variables (with a prefix of OMNIPAXOS)
            .add_source(
                Environment::with_prefix("OMNIPAXOS")
                    .try_parsing(true)
                    .list_separator(",")
                    .with_list_parse_key("node_addrs"),
            )
            .build()?;
        let mut server_config: Self = config.try_deserialize()?;
        let local_config_dir = Path::new(&local_config_file)
            .parent()
            .unwrap_or_else(|| Path::new("."));
        server_config.local.output_filepath =
            Self::resolve_output_path(local_config_dir, &server_config.local.output_filepath);
        Ok(server_config)
    }

    pub fn get_peers(&self, node: NodeId) -> Vec<NodeId> {
        self.cluster
            .nodes
            .iter()
            .cloned()
            .filter(|&id| id != node)
            .collect()
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
