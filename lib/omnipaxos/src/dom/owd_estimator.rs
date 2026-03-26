use crate::errors::{valid_config, ConfigError};
use crate::util::NodeId;
#[cfg(any(feature = "serde", feature = "toml_config"))]
use serde::Deserialize;
#[cfg(feature = "serde")]
use serde::Serialize;
use std::collections::{HashMap, VecDeque};

/// Configuration for the one-way delay estimator.
#[derive(Copy, Clone, Debug, PartialEq)]
#[cfg_attr(any(feature = "serde", feature = "toml_config"), derive(Deserialize))]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(any(feature = "serde", feature = "toml_config"), serde(default))]
pub struct OwdEstimatorConfig {
    /// The size of the sliding window used to estimate the one-way delay.
    /// I.e. the number of samples to store per sender.
    pub window_size: usize,
    /// The upper bound on the one-way delay.
    pub max_owd: i64,
    /// The error bound factor for the one-way delay.
    pub uncertainty_beta: i64,
    /// The strategy used to estimate the one-way delay.
    pub strategy: EstimatorStrategy,
}

impl OwdEstimatorConfig {
    /// Creates a new configuration used to configure an OWD estimator.
    pub fn new(
        window_size: usize,
        max_owd: i64,
        uncertainty_beta: i64,
        strategy: EstimatorStrategy,
    ) -> Result<Self, ConfigError> {
        let config = Self {
            window_size,
            max_owd,
            uncertainty_beta,
            strategy,
        };

        config.validate()?;
        Ok(config)
    }

    /// Validates the OWD estimator configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        valid_config!(self.window_size > 0, "window_size must be > 0");
        valid_config!(self.max_owd >= 0, "max_owd must be >= 0");
        valid_config!(self.uncertainty_beta >= 0, "uncertainty_beta must be >= 0");

        if let EstimatorStrategy::Percentile { percentile } = self.strategy {
            valid_config!(
                (0.0..=1.0).contains(&percentile),
                "percentile must be in [0.0, 1.0]"
            );
        }

        Ok(())
    }

    pub(crate) fn max_owd(&self) -> i64 {
        self.max_owd
    }
}

impl Default for OwdEstimatorConfig {
    fn default() -> Self {
        Self {
            window_size: 10,
            max_owd: 10_000,
            uncertainty_beta: 3,
            strategy: EstimatorStrategy::Percentile { percentile: 0.5 },
        }
    }
}

/// The strategy that should be used when estimating the one-way delay.
#[derive(Copy, Clone, Debug, PartialEq)]
#[cfg_attr(any(feature = "serde", feature = "toml_config"), derive(Deserialize))]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(
    any(feature = "serde", feature = "toml_config"),
    serde(tag = "type", rename_all = "snake_case")
)]
pub enum EstimatorStrategy {
    /// Estimate the one-way delay using a percentile of the observed delays.
    Percentile {
        /// The percentile to use for estimating the one-way delay.
        percentile: f64,
    },
    /// Estimate the one-way delay using a moving average of the observed delays.
    MovingAverage,
    /// Uses a fixed one-way delay value. Deadlines are computed using the configured maximum OWD.
    Fixed,
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct OwdSample {
    /// Observed one-way delay sample.
    measured_owd: i64,
    /// Uncertainty of the sender's timestamp when the message was sent.
    sender_uncertainty: i64,
    /// Uncertainty of the receiver's timestamp when the message was received.
    receiver_uncertainty: i64,
}

impl OwdSample {
    pub(crate) fn new(
        sent_time: i64,
        received_time: i64,
        sender_uncertainty: i64,
        receiver_uncertainty: i64,
    ) -> Self {
        Self {
            measured_owd: (received_time - sent_time).max(0),
            sender_uncertainty: sender_uncertainty.max(0),
            receiver_uncertainty: receiver_uncertainty.max(0),
        }
    }

    fn uncertainty_sum(&self) -> i64 {
        self.sender_uncertainty + self.receiver_uncertainty
    }
}

pub(crate) struct OwdEstimator {
    config: OwdEstimatorConfig,
    samples: HashMap<NodeId, VecDeque<OwdSample>>,
    estimates: HashMap<NodeId, i64>,
}

impl OwdEstimator {
    /// Creates a new OWD estimator with the given configuration.
    pub(crate) fn with_config(config: OwdEstimatorConfig) -> Self {
        Self {
            config,
            samples: HashMap::new(),
            estimates: HashMap::new(),
        }
    }

    /// Returns the current one-way delay estimate for the peer with id `id`.
    pub(crate) fn estimate_for(&self, id: NodeId) -> i64 {
        self.estimates
            .get(&id)
            .copied()
            .unwrap_or(self.config.max_owd)
    }

    #[cfg(feature = "benchmark")]
    pub(crate) fn estimates_snapshot(&self) -> HashMap<NodeId, i64> {
        self.estimates.clone()
    }

    /// Inserts a new OWD sample for the given sender and updates the
    /// sliding-window estimate of the one-way delay for that sender.
    pub(crate) fn update(&mut self, id: NodeId, sample: OwdSample) {
        let window_size = self.config.window_size;

        let samples = self
            .samples
            .entry(id)
            .or_insert_with(|| VecDeque::with_capacity(window_size));

        // Sliding window functionality
        if samples.len() == window_size {
            samples.pop_front();
        }
        samples.push_back(sample);

        let estimate = Self::estimate_from_window(
            samples,
            self.config.strategy,
            self.config.uncertainty_beta,
            self.config.max_owd,
        );
        self.estimates.insert(id, estimate);
    }

    fn estimate_from_window(
        samples: &VecDeque<OwdSample>,
        strategy: EstimatorStrategy,
        beta: i64,
        max_owd: i64,
    ) -> i64 {
        debug_assert!(!samples.is_empty());

        let base_estimate = match strategy {
            EstimatorStrategy::Percentile { percentile } => {
                Self::percentile_delay(samples, percentile)
            }
            EstimatorStrategy::MovingAverage => Self::moving_average_delay(samples),
            EstimatorStrategy::Fixed => {
                return max_owd;
            }
        };

        // Uncertainty of the most recent sample
        let latest = samples.back().expect("window should not be empty");
        let uncertainty_margin = beta.saturating_mul(latest.uncertainty_sum());
        let estimate = base_estimate.saturating_add(uncertainty_margin);

        if estimate <= 0 || estimate > max_owd {
            max_owd
        } else {
            estimate
        }
    }

    fn percentile_delay(samples: &VecDeque<OwdSample>, percentile: f64) -> i64 {
        let mut delays: Vec<i64> = samples.iter().map(|sample| sample.measured_owd).collect();
        delays.sort_unstable();

        let p = percentile.clamp(0.0, 1.0);
        let index = ((delays.len() - 1) as f64 * p).round() as usize;
        delays[index]
    }

    fn moving_average_delay(samples: &VecDeque<OwdSample>) -> i64 {
        let sum: i64 = samples.iter().map(|sample| sample.measured_owd).sum();
        sum / samples.len() as i64
    }
}

/// A tracker for OWD estimates for peers.
pub(crate) struct OutgoingOwdTracker {
    estimates: HashMap<NodeId, i64>,
    default_owd: i64,
}

impl OutgoingOwdTracker {
    /// Creates a new tracker with the given default OWD.
    pub(crate) fn new(default_owd: i64) -> Self {
        Self {
            estimates: HashMap::new(),
            default_owd,
        }
    }

    /// Updates the OWD estimate for the given peer.
    pub(crate) fn update(&mut self, receiver: NodeId, estimated_owd: i64) {
        self.estimates.insert(receiver, estimated_owd.max(0));
    }

    /// Returns the current OWD estimate for the given peer.
    pub(crate) fn get(&self, receiver: NodeId) -> i64 {
        self.estimates
            .get(&receiver)
            .copied()
            .unwrap_or(self.default_owd)
    }

    /// Returns the maximum OWD estimate across all peers.
    pub(crate) fn get_max_owd(&self) -> i64 {
        self.estimates
            .values()
            .copied()
            .max()
            .unwrap_or(self.default_owd)
    }

    #[cfg(feature = "benchmark")]
    pub(crate) fn estimates_snapshot(&self) -> HashMap<NodeId, i64> {
        self.estimates.clone()
    }
}
