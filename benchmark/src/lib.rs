pub mod common;
pub mod clock;

use serde::{Deserialize, Serialize};

#[cfg(all(feature = "protocol-caop", feature = "protocol-baseline"))]
compile_error!("Enable only one of protocol-caop or protocol-baseline");

#[cfg(feature = "protocol-caop")]
pub use caop_omnipaxos as omnipaxos_api;
#[cfg(feature = "protocol-caop")]
pub use caop_omnipaxos_storage as omnipaxos_storage_api;
#[cfg(feature = "protocol-caop")]
pub extern crate caop_omnipaxos as omnipaxos;

#[cfg(feature = "protocol-baseline")]
pub use baseline_omnipaxos as omnipaxos_api;
#[cfg(feature = "protocol-baseline")]
pub use baseline_omnipaxos_storage as omnipaxos_storage_api;
#[cfg(feature = "protocol-baseline")]
pub extern crate baseline_omnipaxos as omnipaxos;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkProtocol {
    #[serde(rename = "caop")]
    #[default]
    CaOp,
    Baseline,
}

impl BenchmarkProtocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CaOp => "caop",
            Self::Baseline => "baseline",
        }
    }

    pub fn enable_fast_path(self) -> bool {
        matches!(self, Self::CaOp)
    }
}
