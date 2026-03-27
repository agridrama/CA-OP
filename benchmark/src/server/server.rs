use crate::{configs::OmniPaxosKVConfig, database::Database, network::Network};
use chrono::Utc;
use log::*;
use omnipaxos_kv::omnipaxos_api::{
    messages::Message,
    util::{LogEntry, NodeId},
    OmniPaxosConfig,
};
#[cfg(feature = "protocol-caop")]
use omnipaxos_kv::omnipaxos_api::{messages::sequence_paxos::EntryId, OmniPaxos};
#[cfg(feature = "protocol-baseline")]
use omnipaxos_kv::omnipaxos_api::OmniPaxos;
use omnipaxos_kv::clock::simulator::Clock;
use omnipaxos_kv::common::{kv::*, messages::*, utils::Timestamp};
use omnipaxos_kv::BenchmarkProtocol;
use omnipaxos_kv::omnipaxos_storage_api::memory_storage::MemoryStorage;
use serde::Serialize;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::OnceLock,
    time::Duration,
};

#[cfg(feature = "protocol-caop")]
type OmniPaxosInstance = OmniPaxos<'static, Command, MemoryStorage<Command>, Clock>;
#[cfg(feature = "protocol-baseline")]
type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;
const NETWORK_BATCH_SIZE: usize = 100;
const LEADER_WAIT: Duration = Duration::from_secs(1);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);
const STATS_WRITE_INTERVAL: Duration = Duration::from_secs(5);
const DOM_OWD_SAMPLE_INTERVAL: Duration = Duration::from_millis(500);

static CLOCK: OnceLock<Clock> = OnceLock::new();

#[cfg(feature = "protocol-caop")]
fn build_omnipaxos(
    config: OmniPaxosConfig,
    storage: MemoryStorage<Command>,
    clock: &'static Clock,
) -> OmniPaxosInstance {
    config.build(storage, clock).unwrap()
}

#[cfg(feature = "protocol-baseline")]
fn build_omnipaxos(
    config: OmniPaxosConfig,
    storage: MemoryStorage<Command>,
    _clock: &'static Clock,
) -> OmniPaxosInstance {
    config.build(storage).unwrap()
}

#[cfg(feature = "protocol-caop")]
fn poll_omnipaxos(omnipaxos: &mut OmniPaxosInstance) {
    omnipaxos.poll();
}

#[cfg(feature = "protocol-baseline")]
fn poll_omnipaxos(_omnipaxos: &mut OmniPaxosInstance) {}

#[cfg(feature = "protocol-caop")]
fn append_entry(omnipaxos: &mut OmniPaxosInstance, command: Command, from: ClientId, command_id: CommandId) {
    omnipaxos
        .append_with_id(
            command,
            EntryId {
                client_id: from,
                command_id,
            },
        )
        .expect("Append to Omnipaxos log failed");
}

#[cfg(feature = "protocol-baseline")]
fn append_entry(omnipaxos: &mut OmniPaxosInstance, command: Command, _from: ClientId, _command_id: CommandId) {
    omnipaxos
        .append(command)
        .expect("Append to Omnipaxos log failed");
}

#[cfg(feature = "protocol-caop")]
fn decision_counters(omnipaxos: &OmniPaxosInstance) -> (u64, u64) {
    omnipaxos.get_fast_path_ratio()
}

#[cfg(feature = "protocol-baseline")]
fn decision_counters(omnipaxos: &OmniPaxosInstance) -> (u64, u64) {
    (0, omnipaxos.get_decided_idx() as u64)
}

#[derive(Clone, Serialize)]
struct DomOwdOutput {
    incoming_estimates: std::collections::HashMap<NodeId, i64>,
    outgoing_estimates: std::collections::HashMap<NodeId, i64>,
    max_outgoing_estimate: i64,
}

#[derive(Serialize)]
struct DomOwdCsvRow {
    timestamp_ms: Timestamp,
    server_id: NodeId,
    metric: &'static str,
    peer_id: Option<NodeId>,
    owd_us: i64,
}

#[derive(Clone, Serialize)]
struct DecisionStatsOutput {
    protocol: &'static str,
    fast_path_decisions: u64,
    slow_path_decisions: u64,
    fast_path_ratio: f64,
}

#[derive(Serialize)]
struct DecisionStatsHistoryPoint {
    timestamp_ms: Timestamp,
    decision_stats: DecisionStatsOutput,
}

pub struct OmniPaxosServer {
    id: NodeId,
    database: Database,
    network: Network,
    omnipaxos: OmniPaxosInstance,
    current_decided_idx: usize,
    omnipaxos_msg_buffer: Vec<Message<Command>>,
    config: OmniPaxosKVConfig,
    peers: Vec<NodeId>,
    decision_stats_history: Vec<DecisionStatsHistoryPoint>,
}

impl OmniPaxosServer {
    pub async fn new(config: OmniPaxosKVConfig) -> Self {
        // Initialize OmniPaxos instance
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        let clock = CLOCK.get_or_init(|| {
            let mut clock_cfg = config.local.clock.clone();
            clock_cfg.node_id = config.local.server_id;
            Clock::new(clock_cfg)
        });
        let omnipaxos = build_omnipaxos(omnipaxos_config, storage, clock);
        // Waits for client and server network connections to be established
        let network = Network::new(config.clone(), NETWORK_BATCH_SIZE).await;
        OmniPaxosServer {
            id: config.local.server_id,
            database: Database::new(),
            network,
            omnipaxos,
            current_decided_idx: 0,
            omnipaxos_msg_buffer,
            peers: config.get_peers(config.local.server_id),
            config,
            decision_stats_history: Vec::with_capacity(16),
        }
    }

    pub async fn run(&mut self) {
        // Save config to output file
        self.init_dom_owd_csv().expect("Failed to initialise OWD CSV");
        self.record_dom_owd_snapshot()
            .expect("Failed to write initial OWD snapshot");
        self.record_decision_stats_snapshot();
        self.save_output().expect("Failed to write to file");
        let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // We don't use Omnipaxos leader election at first and instead force a specific initial leader
        self.establish_initial_leader(&mut cluster_msg_buf, &mut client_msg_buf)
            .await;
        // Main event loop with leader election
        let mut election_interval = tokio::time::interval(ELECTION_TIMEOUT);
        let mut stats_interval = tokio::time::interval(STATS_WRITE_INTERVAL);
        let mut dom_owd_interval = tokio::time::interval(DOM_OWD_SAMPLE_INTERVAL);
        let mut poll_timeout = tokio::time::interval(Duration::from_millis(1));
        loop {
            tokio::select! {
                _ = election_interval.tick() => {
                    self.omnipaxos.tick();
                    self.send_outgoing_msgs();
                },
                _ = dom_owd_interval.tick() => {
                    self.record_dom_owd_snapshot()
                        .expect("Failed to write OWD snapshot");
                },
                _ = stats_interval.tick() => {
                    self.record_decision_stats_snapshot();
                    self.save_output().expect("Failed to write stats");
                },
                _ = poll_timeout.tick() => {
                    poll_omnipaxos(&mut self.omnipaxos);
                    self.send_outgoing_msgs();
                }
                _ = self.network.cluster_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_cluster_messages(&mut cluster_msg_buf).await;
                },
                _ = self.network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(&mut client_msg_buf).await;
                },
            }
        }
    }

    // Ensures cluster is connected and initial leader is promoted before returning.
    // Once the leader is established it chooses a synchronization point which the
    // followers relay to their clients to begin the experiment.
    async fn establish_initial_leader(
        &mut self,
        cluster_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>,
        client_msg_buffer: &mut Vec<(ClientId, ClientMessage)>,
    ) {
        let mut leader_takeover_interval = tokio::time::interval(LEADER_WAIT);
        loop {
            tokio::select! {
                _ = leader_takeover_interval.tick(), if self.config.cluster.initial_leader == self.id => {
                    if let Some((curr_leader, is_accept_phase)) = self.omnipaxos.get_current_leader(){
                        if curr_leader == self.id && is_accept_phase {
                            info!("{}: Leader fully initialized", self.id);
                            let experiment_sync_start = (Utc::now() + Duration::from_secs(2)).timestamp_millis();
                            self.send_cluster_start_signals(experiment_sync_start);
                            self.send_client_start_signals(experiment_sync_start);
                            break;
                        }
                    }
                    info!("{}: Attempting to take leadership", self.id);
                    self.omnipaxos.try_become_leader();
                    self.send_outgoing_msgs();
                },
                _ = self.network.cluster_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE) => {
                    let recv_start = self.handle_cluster_messages(cluster_msg_buffer).await;
                    self.send_outgoing_msgs();
                    if recv_start {
                        break;
                    }
                },
                _ = self.network.client_messages.recv_many(client_msg_buffer, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(client_msg_buffer).await;
                },
            }
        }
    }

    fn handle_decided_entries(&mut self) {
        // TODO: Can use a read_raw here to avoid allocation
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.current_decided_idx < new_decided_idx {
            let decided_entries = self
                .omnipaxos
                .read_decided_suffix(self.current_decided_idx)
                .unwrap();
            self.current_decided_idx = new_decided_idx;
            debug!("Decided {new_decided_idx}");
            let decided_commands = decided_entries
                .into_iter()
                .filter_map(|e| match e {
                    LogEntry::Decided(cmd) => Some(cmd),
                    _ => unreachable!(),
                })
                .collect();
            self.update_database_and_respond(decided_commands);
        }
    }

    fn update_database_and_respond(&mut self, commands: Vec<Command>) {
        // TODO: batching responses possible here (batch at handle_cluster_messages)
        for command in commands {
            let read = self.database.handle_command(command.kv_cmd);
            if command.coordinator_id == self.id {
                let response = match read {
                    Some(read_result) => ServerMessage::Read(command.id, read_result),
                    None => ServerMessage::Write(command.id),
                };
                self.network.send_to_client(command.client_id, response);
            }
        }
    }

    fn send_outgoing_msgs(&mut self) {
        self.omnipaxos
            .take_outgoing_messages(&mut self.omnipaxos_msg_buffer);
        for msg in self.omnipaxos_msg_buffer.drain(..) {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network.send_to_cluster(to, cluster_msg);
        }
    }

    async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
        for (from, message) in messages.drain(..) {
            match message {
                ClientMessage::Append(command_id, kv_command) => {
                    self.append_to_log(from, command_id, kv_command)
                }
            }
        }
        self.send_outgoing_msgs();
    }

    async fn handle_cluster_messages(
        &mut self,
        messages: &mut Vec<(NodeId, ClusterMessage)>,
    ) -> bool {
        let mut received_start_signal = false;
        for (from, message) in messages.drain(..) {
            trace!("{}: Received {message:?}", self.id);
            match message {
                ClusterMessage::OmniPaxosMessage(m) => {
                    self.omnipaxos.handle_incoming(m);
                    self.handle_decided_entries();
                }
                ClusterMessage::LeaderStartSignal(start_time) => {
                    debug!("Received start message from peer {from}");
                    received_start_signal = true;
                    self.send_client_start_signals(start_time);
                }
            }
        }
        self.send_outgoing_msgs();
        received_start_signal
    }

    fn append_to_log(&mut self, from: ClientId, command_id: CommandId, kv_command: KVCommand) {
        let command = Command {
            client_id: from,
            coordinator_id: self.id,
            id: command_id,
            kv_cmd: kv_command,
        };
        append_entry(&mut self.omnipaxos, command, from, command_id);
    }

    fn send_cluster_start_signals(&mut self, start_time: Timestamp) {
        for peer in &self.peers {
            debug!("Sending start message to peer {peer}");
            let msg = ClusterMessage::LeaderStartSignal(start_time);
            self.network.send_to_cluster(*peer, msg);
        }
    }

    fn send_client_start_signals(&mut self, start_time: Timestamp) {
        for client_id in self.network.connected_client_ids() {
            debug!("Sending start message to client {client_id}");
            let msg = ServerMessage::StartSignal(start_time);
            self.network.send_to_client(client_id, msg.clone());
        }
    }

    fn current_dom_owd_output(&self) -> DomOwdOutput {
        #[cfg(feature = "protocol-baseline")]
        {
            return DomOwdOutput {
                incoming_estimates: std::collections::HashMap::new(),
                outgoing_estimates: std::collections::HashMap::new(),
                max_outgoing_estimate: 0,
            };
        }

        #[cfg(feature = "protocol-caop")]
        {
        let owd_snapshot = self.omnipaxos.get_dom_owd_snapshot();
        DomOwdOutput {
            incoming_estimates: owd_snapshot.incoming_estimates,
            outgoing_estimates: owd_snapshot.outgoing_estimates,
            max_outgoing_estimate: owd_snapshot.max_outgoing_estimate,
        }
        }
    }

    fn current_decision_stats_output(&self) -> DecisionStatsOutput {
        let (fast, slow) = decision_counters(&self.omnipaxos);
        let protocol = self.config.local.protocol;
        let (fast_path_decisions, slow_path_decisions, fast_path_ratio) =
            if protocol == BenchmarkProtocol::Baseline {
                let decided = self.omnipaxos.get_decided_idx() as u64;
                (0, decided.max(slow), 0.0)
            } else {
                let total = fast + slow;
                let ratio = if total > 0 {
                    fast as f64 / total as f64
                } else {
                    0.0
                };
                (fast, slow, ratio)
            };
        DecisionStatsOutput {
            protocol: protocol.as_str(),
            fast_path_decisions,
            slow_path_decisions,
            fast_path_ratio,
        }
    }

    fn dom_owd_csv_path(&self) -> PathBuf {
        let output_path = Path::new(&self.config.local.output_filepath);
        let stem = output_path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .expect("output filepath must have a valid stem");
        output_path.with_file_name(format!("{stem}-owd.csv"))
    }

    fn init_dom_owd_csv(&self) -> Result<(), std::io::Error> {
        let file = File::create(self.dom_owd_csv_path())?;
        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(file);
        writer.write_record(["timestamp_ms", "server_id", "metric", "peer_id", "owd_us"])?;
        writer.flush()?;
        Ok(())
    }

    fn record_dom_owd_snapshot(&mut self) -> Result<(), std::io::Error> {
        let dom_owd = self.current_dom_owd_output();
        let timestamp_ms = Utc::now().timestamp_millis();
        let file = OpenOptions::new().append(true).open(self.dom_owd_csv_path())?;
        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(file);

        writer.serialize(DomOwdCsvRow {
            timestamp_ms,
            server_id: self.id,
            metric: "max_outgoing",
            peer_id: None,
            owd_us: dom_owd.max_outgoing_estimate,
        })?;

        for (peer_id, owd_us) in &dom_owd.incoming_estimates {
            writer.serialize(DomOwdCsvRow {
                timestamp_ms,
                server_id: self.id,
                metric: "incoming",
                peer_id: Some(*peer_id),
                owd_us: *owd_us,
            })?;
        }

        for (peer_id, owd_us) in &dom_owd.outgoing_estimates {
            writer.serialize(DomOwdCsvRow {
                timestamp_ms,
                server_id: self.id,
                metric: "outgoing",
                peer_id: Some(*peer_id),
                owd_us: *owd_us,
            })?;
        }

        writer.flush()?;
        Ok(())
    }

    fn record_decision_stats_snapshot(&mut self) {
        let decision_stats = self.current_decision_stats_output();
        self.decision_stats_history.push(DecisionStatsHistoryPoint {
            timestamp_ms: Utc::now().timestamp_millis(),
            decision_stats,
        });
    }

    fn save_output(&mut self) -> Result<(), std::io::Error> {
        let dom_owd = self.current_dom_owd_output();
        let decision_stats = self.current_decision_stats_output();
        let output = serde_json::json!({
            "config": &self.config,
            "protocol": self.config.local.protocol.as_str(),
            "owd_config": &self.config.local.owd_config,
            "clock_config": &self.config.local.clock,
            "fast_path_decisions": decision_stats.fast_path_decisions,
            "slow_path_decisions": decision_stats.slow_path_decisions,
            "fast_path_ratio": decision_stats.fast_path_ratio,
            "decision_stats_history": &self.decision_stats_history,
            "dom_owd": &dom_owd,
        });
        //let config_json = serde_json::to_string_pretty(&self.config)?;
        //let mut output_file = File::create(&self.config.local.output_filepath)?;
        //output_file.write_all(config_json.as_bytes())?;
        //output_file.flush()?;
        //Ok(())
        let output_json = serde_json::to_string_pretty(&output)?;
        let mut output_file = File::create(&self.config.local.output_filepath)?;
        output_file.write_all(output_json.as_bytes())?;
        output_file.flush()?;
        Ok(())
    }
}
