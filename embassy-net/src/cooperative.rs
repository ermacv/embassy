use embassy_time::Duration;
use smoltcp::iface::{PollIngressSingleResult, PollResult};

pub(crate) fn take_start_direction(next_starts_with_ingress: &mut bool, force_ingress: &mut bool) -> bool {
    if core::mem::replace(force_ingress, false) {
        true
    } else {
        let starts_with_ingress = *next_starts_with_ingress;
        *next_starts_with_ingress = !starts_with_ingress;
        starts_with_ingress
    }
}

/// Packet-based cooperative network service policy.
///
/// The packet budgets are the primary scheduling boundary. The duration is a
/// defensive guard for an unexpectedly expensive primitive, not a batching
/// target.
#[derive(Clone, Copy, Debug)]
pub struct CooperativeConfig {
    pub(crate) ingress_frames: u8,
    pub(crate) egress_frames: u8,
    /// Longest residence allowed before the runner returns to its executor.
    pub max_poll_duration: Duration,
    #[cfg(feature = "cooperative-scheduler-telemetry")]
    observer: Option<fn(CooperativePollReport)>,
}

impl CooperativeConfig {
    /// Create a packet-based service policy.
    pub const fn new(ingress_frames: u8, egress_frames: u8, max_poll_duration: Duration) -> Self {
        assert!(ingress_frames != 0, "ingress frame budget must not be zero");
        assert!(egress_frames != 0, "egress frame budget must not be zero");
        Self {
            ingress_frames,
            egress_frames,
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
    /// Neither direction reported immediately runnable work.
    Drained,
    /// At least one packet or scan budget was reached.
    WorkBudget,
    /// The defensive residence deadline was reached.
    TimeBudget,
    /// Application egress is waiting for a driver TX credit.
    EgressCredit,
}

/// Aggregate evidence from one cooperative runner poll.
#[cfg(feature = "cooperative-scheduler-telemetry")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CooperativePollReport {
    /// Number of ingress primitives invoked.
    pub ingress_calls: u8,
    /// Number of ingress frames processed.
    pub ingress_packets: u8,
    /// Number of complete socket egress scans.
    pub egress_passes: u8,
    /// Number of real driver TX tokens issued.
    pub egress_tx_tokens: u32,
    /// Whether application egress exhausted hardware TX credits.
    pub egress_blocked: bool,
    /// Whether the ingress frame budget was consumed.
    pub ingress_budget_exhausted: bool,
    /// Whether the egress token or defensive scan budget was consumed.
    pub egress_budget_exhausted: bool,
    /// Whether this turn began with ingress.
    pub started_with_ingress: bool,
    /// Total turn residence, including interrupt preemption.
    pub elapsed_micros: u64,
    /// Primary reason for returning ownership to the executor.
    pub exit: CooperativePollExit,
}

pub(crate) struct CooperativePollState {
    pub(crate) ingress_calls: u8,
    pub(crate) ingress_packets: u8,
    pub(crate) egress_passes: u8,
    pub(crate) egress_tx_tokens: u32,
    pub(crate) ingress_unavailable: bool,
    pub(crate) egress_no_work: bool,
    pub(crate) egress_hardware_blocked: bool,
    pub(crate) egress_software_blocked: bool,
    ingress_budget: u8,
    egress_budget: u8,
    #[cfg(feature = "cooperative-scheduler-telemetry")]
    pub(crate) started_with_ingress: bool,
}

impl CooperativePollState {
    pub(crate) const fn new(config: CooperativeConfig, started_with_ingress: bool) -> Self {
        #[cfg(not(feature = "cooperative-scheduler-telemetry"))]
        let _ = started_with_ingress;
        Self {
            ingress_calls: 0,
            ingress_packets: 0,
            egress_passes: 0,
            egress_tx_tokens: 0,
            ingress_unavailable: false,
            egress_no_work: false,
            egress_hardware_blocked: false,
            egress_software_blocked: false,
            ingress_budget: config.ingress_frames,
            egress_budget: config.egress_frames,
            #[cfg(feature = "cooperative-scheduler-telemetry")]
            started_with_ingress,
        }
    }

    pub(crate) fn can_poll_ingress(&self) -> bool {
        !self.ingress_unavailable && self.ingress_packets < self.ingress_budget
    }

    pub(crate) fn can_poll_egress(&self) -> bool {
        !self.egress_no_work
            && !self.egress_hardware_blocked
            && !self.egress_software_blocked
            && self.egress_tx_tokens < u32::from(self.egress_budget)
            // A token is the fairness unit. This additional bound merely
            // prevents a broken zero-token socket pass from spinning.
            && self.egress_passes < self.egress_budget
    }

    pub(crate) fn record_ingress(&mut self, result: PollIngressSingleResult) {
        self.ingress_calls = self.ingress_calls.saturating_add(1);
        match result {
            PollIngressSingleResult::None => self.ingress_unavailable = true,
            PollIngressSingleResult::PacketProcessed | PollIngressSingleResult::SocketStateChanged => {
                self.ingress_packets = self.ingress_packets.saturating_add(1);
                self.egress_no_work = false;
            }
        }
    }

    pub(crate) fn record_egress(
        &mut self,
        result: PollResult,
        tx_tokens: u32,
        hardware_blocked: bool,
        software_blocked: bool,
    ) {
        self.egress_passes = self.egress_passes.saturating_add(1);
        self.egress_tx_tokens = self.egress_tx_tokens.saturating_add(tx_tokens);
        self.egress_hardware_blocked |= hardware_blocked;
        self.egress_software_blocked |= software_blocked;
        self.egress_no_work = result == PollResult::None && tx_tokens == 0 && !hardware_blocked && !software_blocked;
    }

    pub(crate) fn ingress_budget_exhausted(&self) -> bool {
        !self.ingress_unavailable && self.ingress_packets >= self.ingress_budget
    }

    pub(crate) fn egress_budget_exhausted(&self) -> bool {
        !self.egress_no_work
            && !self.egress_hardware_blocked
            && (self.egress_software_blocked
                || self.egress_tx_tokens >= u32::from(self.egress_budget)
                || self.egress_passes >= self.egress_budget)
    }

    /// A hardware-credit stop must first return CPU ownership to the radio
    /// task. If egress stopped before RX was polled at all, schedule exactly
    /// one later ingress-first turn; Embassy fairness places other ready tasks
    /// before it. An already serviced ingress direction waits for its normal
    /// driver wake instead of keeping the network task continuously runnable.
    pub(crate) fn needs_forced_ingress_followup(&self) -> bool {
        self.egress_hardware_blocked && self.ingress_calls == 0
    }

    pub(crate) fn should_self_wake(&self, time_exhausted: bool) -> bool {
        if self.egress_hardware_blocked {
            return self.needs_forced_ingress_followup();
        }
        if time_exhausted {
            return self.can_poll_ingress() || self.can_poll_egress();
        }
        self.ingress_budget_exhausted() || self.egress_budget_exhausted()
    }

    #[cfg(feature = "cooperative-scheduler-telemetry")]
    pub(crate) fn report(&self, elapsed_micros: u64, time_exhausted: bool) -> CooperativePollReport {
        let ingress_budget_exhausted = self.ingress_budget_exhausted();
        let egress_budget_exhausted = self.egress_budget_exhausted();
        let exit = if time_exhausted {
            CooperativePollExit::TimeBudget
        } else if ingress_budget_exhausted || egress_budget_exhausted {
            CooperativePollExit::WorkBudget
        } else if self.egress_hardware_blocked {
            CooperativePollExit::EgressCredit
        } else {
            CooperativePollExit::Drained
        };
        CooperativePollReport {
            ingress_calls: self.ingress_calls,
            ingress_packets: self.ingress_packets,
            egress_passes: self.egress_passes,
            egress_tx_tokens: self.egress_tx_tokens,
            egress_blocked: self.egress_hardware_blocked,
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

    const CONFIG: CooperativeConfig = CooperativeConfig::new(4, 4, Duration::from_micros(750));

    #[test]
    fn hardware_credit_yields_then_forces_ingress_followup() {
        let mut state = CooperativePollState::new(CONFIG, false);
        state.record_egress(PollResult::None, 0, true, false);
        assert!(!state.can_poll_egress());
        assert!(state.needs_forced_ingress_followup());
        assert!(state.should_self_wake(false));
    }

    #[test]
    fn hardware_credit_does_not_reschedule_already_serviced_ingress() {
        let mut state = CooperativePollState::new(CONFIG, true);
        state.record_ingress(PollIngressSingleResult::PacketProcessed);
        state.record_egress(PollResult::None, 0, true, false);
        assert!(!state.needs_forced_ingress_followup());
        assert!(!state.should_self_wake(false));
    }

    #[test]
    fn hardware_credit_waits_when_ingress_was_proved_unavailable() {
        let mut state = CooperativePollState::new(CONFIG, true);
        state.record_ingress(PollIngressSingleResult::None);
        state.record_egress(PollResult::None, 0, true, false);
        assert!(!state.needs_forced_ingress_followup());
        assert!(!state.should_self_wake(false));
    }

    #[test]
    fn packet_budgets_are_real_work_units() {
        let mut state = CooperativePollState::new(CONFIG, true);
        for _ in 0..4 {
            state.record_ingress(PollIngressSingleResult::PacketProcessed);
        }
        state.record_egress(PollResult::SocketStateChanged, 4, false, false);
        assert!(state.ingress_budget_exhausted());
        assert!(state.egress_budget_exhausted());
        assert!(state.should_self_wake(false));
    }

    #[test]
    fn software_tx_budget_is_not_hardware_credit_exhaustion() {
        let mut state = CooperativePollState::new(CONFIG, false);
        state.record_egress(PollResult::None, 4, false, true);
        assert!(state.egress_budget_exhausted());
        assert!(!state.egress_hardware_blocked);
        assert!(state.should_self_wake(false));
    }

    #[test]
    fn first_direction_alternates_except_for_forced_ingress() {
        let mut next = true;
        let mut force = false;
        assert!(take_start_direction(&mut next, &mut force));
        assert!(!take_start_direction(&mut next, &mut force));
        force = true;
        assert!(take_start_direction(&mut next, &mut force));
        assert!(!force);
        assert!(take_start_direction(&mut next, &mut force));
    }
}
