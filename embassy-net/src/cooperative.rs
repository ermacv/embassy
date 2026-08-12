use embassy_time::Duration;
use smoltcp::iface::{PollIngressSingleResult, PollResult};

pub(crate) const DIRECTION_QUANTUM: u8 = 1;
pub(crate) const INGRESS_LIMIT: u8 = 32;
pub(crate) const EGRESS_LIMIT: u8 = 32;

pub(crate) fn take_start_direction(next_starts_with_ingress: &mut bool) -> bool {
    let starts_with_ingress = *next_starts_with_ingress;
    *next_starts_with_ingress = !starts_with_ingress;
    starts_with_ingress
}

/// Maximum uninterrupted residence of one directionally fair network poll.
#[derive(Clone, Copy, Debug)]
pub struct CooperativeConfig {
    /// Longest residence allowed before the runner returns to its executor.
    pub max_poll_duration: Duration,
    #[cfg(feature = "cooperative-scheduler-telemetry")]
    observer: Option<fn(CooperativePollReport)>,
}

impl CooperativeConfig {
    /// Create a strictly interleaved runner policy capped at 32 ingress
    /// packets and 32 egress passes per executor poll.
    pub const fn new(max_poll_duration: Duration) -> Self {
        Self {
            max_poll_duration,
            #[cfg(feature = "cooperative-scheduler-telemetry")]
            observer: None,
        }
    }

    /// Observe aggregate scheduler work after each runner poll.
    #[cfg(feature = "cooperative-scheduler-telemetry")]
    pub const fn with_observer(mut self, observer: fn(CooperativePollReport)) -> Self {
        self.observer = Some(observer);
        self
    }

    #[cfg(feature = "cooperative-scheduler-telemetry")]
    pub(crate) fn observe(self, report: CooperativePollReport) {
        if let Some(observer) = self.observer {
            observer(report);
        }
    }
}

/// Why one bounded cooperative poll returned ownership to the executor.
#[cfg(feature = "cooperative-scheduler-telemetry")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CooperativePollExit {
    /// Both directions reported no remaining work.
    Drained,
    /// At least one directional work limit was reached.
    WorkBudget,
    /// The residence deadline was reached.
    TimeBudget,
    /// Application egress is waiting for a driver TX credit and ingress is drained.
    EgressCredit,
}

/// Aggregate evidence from one cooperative runner poll.
#[cfg(feature = "cooperative-scheduler-telemetry")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CooperativePollReport {
    /// Number of ingress primitives invoked.
    pub ingress_calls: u8,
    /// Number of ingress packets processed.
    pub ingress_packets: u8,
    /// Number of egress primitives invoked.
    pub egress_passes: u8,
    /// Number of driver TX tokens issued to egress.
    pub egress_tx_tokens: u32,
    /// Whether application egress exhausted its TX credits.
    pub egress_blocked: bool,
    /// Whether ingress reached its packet limit.
    pub ingress_budget_exhausted: bool,
    /// Whether egress reached its pass limit.
    pub egress_budget_exhausted: bool,
    /// Whether this poll started with ingress rather than egress.
    pub started_with_ingress: bool,
    /// Poll residence in microseconds.
    pub elapsed_micros: u64,
    /// Primary reason for returning to the executor.
    pub exit: CooperativePollExit,
}

pub(crate) struct CooperativePollState {
    pub(crate) ingress_calls: u8,
    pub(crate) ingress_packets: u8,
    pub(crate) egress_passes: u8,
    pub(crate) egress_tx_tokens: u32,
    pub(crate) ingress_drained: bool,
    pub(crate) egress_drained: bool,
    pub(crate) egress_blocked: bool,
    #[cfg(feature = "cooperative-scheduler-telemetry")]
    pub(crate) started_with_ingress: bool,
}

impl CooperativePollState {
    pub(crate) const fn new(started_with_ingress: bool) -> Self {
        #[cfg(not(feature = "cooperative-scheduler-telemetry"))]
        let _ = started_with_ingress;
        Self {
            ingress_calls: 0,
            ingress_packets: 0,
            egress_passes: 0,
            egress_tx_tokens: 0,
            ingress_drained: false,
            egress_drained: false,
            egress_blocked: false,
            #[cfg(feature = "cooperative-scheduler-telemetry")]
            started_with_ingress,
        }
    }

    pub(crate) fn can_poll_ingress(&self) -> bool {
        !self.ingress_drained && self.ingress_packets < INGRESS_LIMIT
    }

    pub(crate) fn can_poll_egress(&self) -> bool {
        !self.egress_drained && !self.egress_blocked && self.egress_passes < EGRESS_LIMIT
    }

    pub(crate) fn can_poll(&self) -> bool {
        self.can_poll_ingress() || self.can_poll_egress()
    }

    pub(crate) fn record_ingress(&mut self, result: PollIngressSingleResult) {
        self.ingress_calls = self.ingress_calls.saturating_add(1);
        match result {
            PollIngressSingleResult::None => self.ingress_drained = true,
            PollIngressSingleResult::PacketProcessed => {
                self.ingress_packets = self.ingress_packets.saturating_add(1);
                self.egress_drained = false;
            }
            PollIngressSingleResult::SocketStateChanged => {
                self.ingress_packets = self.ingress_packets.saturating_add(1);
                self.egress_drained = false;
            }
        }
    }

    pub(crate) fn record_egress(&mut self, result: PollResult, tx_tokens: u32, blocked: bool) {
        self.egress_passes = self.egress_passes.saturating_add(1);
        self.egress_tx_tokens = self.egress_tx_tokens.saturating_add(tx_tokens);
        self.egress_blocked |= blocked;
        self.egress_drained = result == PollResult::None && tx_tokens == 0 && !blocked;
    }

    pub(crate) fn should_self_wake(&self, time_exhausted: bool) -> bool {
        if time_exhausted {
            return self.can_poll_ingress() || self.can_poll_egress();
        }
        (!self.ingress_drained && self.ingress_packets >= INGRESS_LIMIT)
            || (!self.egress_drained && !self.egress_blocked && self.egress_passes >= EGRESS_LIMIT)
    }

    #[cfg(feature = "cooperative-scheduler-telemetry")]
    pub(crate) fn report(&self, elapsed_micros: u64, time_exhausted: bool) -> CooperativePollReport {
        let ingress_budget_exhausted = !self.ingress_drained && self.ingress_packets >= INGRESS_LIMIT;
        let egress_budget_exhausted =
            !self.egress_drained && !self.egress_blocked && self.egress_passes >= EGRESS_LIMIT;
        let exit = if time_exhausted {
            CooperativePollExit::TimeBudget
        } else if ingress_budget_exhausted || egress_budget_exhausted {
            CooperativePollExit::WorkBudget
        } else if self.egress_blocked && self.ingress_drained {
            CooperativePollExit::EgressCredit
        } else {
            CooperativePollExit::Drained
        };
        CooperativePollReport {
            ingress_calls: self.ingress_calls,
            ingress_packets: self.ingress_packets,
            egress_passes: self.egress_passes,
            egress_tx_tokens: self.egress_tx_tokens,
            egress_blocked: self.egress_blocked,
            ingress_budget_exhausted,
            egress_budget_exhausted,
            started_with_ingress: self.started_with_ingress,
            elapsed_micros,
            exit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn egress_credit_exhaustion_does_not_suppress_ingress() {
        let mut state = CooperativePollState::new(true);
        state.record_egress(PollResult::None, 0, true);
        assert!(state.egress_blocked);
        assert!(!state.can_poll_egress());
        assert!(state.can_poll_ingress());
    }

    #[test]
    fn ingress_budget_self_wakes() {
        let mut state = CooperativePollState::new(true);
        for _ in 0..INGRESS_LIMIT {
            state.record_ingress(PollIngressSingleResult::PacketProcessed);
        }
        state.egress_drained = true;
        assert!(state.should_self_wake(false));
    }

    #[test]
    fn egress_budget_self_wakes() {
        let mut state = CooperativePollState::new(false);
        state.ingress_drained = true;
        for _ in 0..EGRESS_LIMIT {
            state.record_egress(PollResult::SocketStateChanged, 1, false);
        }
        assert!(state.should_self_wake(false));
    }

    #[test]
    fn drained_poll_does_not_self_wake() {
        let mut state = CooperativePollState::new(true);
        state.record_ingress(PollIngressSingleResult::None);
        state.record_egress(PollResult::None, 0, false);
        assert!(!state.should_self_wake(false));
    }

    #[test]
    fn egress_credit_waits_for_driver_when_ingress_is_drained() {
        let mut state = CooperativePollState::new(true);
        state.record_ingress(PollIngressSingleResult::None);
        state.record_egress(PollResult::None, 0, true);
        assert!(!state.should_self_wake(false));
    }

    #[test]
    fn time_budget_wakes_only_when_runnable_work_remains() {
        let mut active = CooperativePollState::new(true);
        active.record_ingress(PollIngressSingleResult::PacketProcessed);
        assert!(active.should_self_wake(true));

        let mut blocked = CooperativePollState::new(true);
        blocked.record_ingress(PollIngressSingleResult::None);
        blocked.record_egress(PollResult::None, 0, true);
        assert!(!blocked.should_self_wake(true));
    }

    #[test]
    fn first_direction_alternates_between_polls() {
        let mut next = true;
        assert!(take_start_direction(&mut next));
        assert!(!take_start_direction(&mut next));
        assert!(take_start_direction(&mut next));
    }
}
