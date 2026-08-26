use xarxa::iface::{PollIngressSingleResult, PollResult};

pub(crate) const DIRECTION_QUANTUM: u8 = 4;
// Never let one network poll consume the complete 64-packet application
// socket reserve. Returning after half of that capacity gives the socket
// owner an executor turn before ingress can fill the queue by itself.
pub(crate) const INGRESS_LIMIT: u8 = 32;
pub(crate) const EGRESS_LIMIT: u8 = 32;

pub(crate) fn take_start_direction(next_starts_with_ingress: &mut bool) -> bool {
    let starts_with_ingress = *next_starts_with_ingress;
    *next_starts_with_ingress = !starts_with_ingress;
    starts_with_ingress
}

#[cfg(feature = "cooperative-scheduler-telemetry")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Why one bounded work-conserving poll returned to the executor.
pub enum CooperativePollExit {
    /// Neither direction reported runnable work.
    Drained,
    /// At least one direction reached its production work budget.
    WorkBudget,
    /// Application egress is waiting for a driver TX credit.
    EgressCredit,
}

#[cfg(feature = "cooperative-scheduler-telemetry")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Aggregate evidence from one production scheduler turn.
pub struct CooperativePollReport {
    /// Number of ingress primitives invoked.
    pub ingress_calls: u8,
    /// Number of ingress packets processed.
    pub ingress_packets: u8,
    /// Number of bounded egress socket passes invoked.
    pub egress_passes: u8,
    /// Number of driver TX tokens issued to application egress.
    pub egress_tx_tokens: u32,
    /// Whether application egress exhausted hardware TX credits.
    pub egress_blocked: bool,
    /// Whether ingress reached its frame budget.
    pub ingress_budget_exhausted: bool,
    /// Whether egress reached its issued-frame budget.
    pub egress_budget_exhausted: bool,
    /// Whether this turn started with ingress.
    pub started_with_ingress: bool,
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
        !self.egress_drained && !self.egress_blocked && self.egress_tx_tokens < u32::from(EGRESS_LIMIT)
    }

    pub(crate) fn can_poll(&self) -> bool {
        self.can_poll_ingress() || self.can_poll_egress()
    }

    pub(crate) fn record_ingress(&mut self, result: PollIngressSingleResult) {
        self.ingress_calls = self.ingress_calls.saturating_add(1);
        match result {
            PollIngressSingleResult::None => self.ingress_drained = true,
            PollIngressSingleResult::PacketProcessed | PollIngressSingleResult::SocketStateChanged => {
                self.ingress_packets = self.ingress_packets.saturating_add(1);
                self.egress_drained = false;
            }
        }
    }

    pub(crate) fn record_egress(&mut self, result: PollResult, tx_tokens: u32, blocked: bool, quantum_exhausted: bool) {
        self.egress_passes = self.egress_passes.saturating_add(1);
        self.egress_tx_tokens = self.egress_tx_tokens.saturating_add(tx_tokens);
        self.egress_blocked |= blocked;
        self.egress_drained = result == PollResult::None && tx_tokens == 0 && !blocked && !quantum_exhausted;
    }

    pub(crate) fn should_self_wake(&self) -> bool {
        (!self.ingress_drained && self.ingress_packets >= INGRESS_LIMIT)
            || (!self.egress_drained && !self.egress_blocked && self.egress_tx_tokens >= u32::from(EGRESS_LIMIT))
    }

    #[cfg(feature = "cooperative-scheduler-telemetry")]
    pub(crate) fn report(&self) -> CooperativePollReport {
        let ingress_budget_exhausted = !self.ingress_drained && self.ingress_packets >= INGRESS_LIMIT;
        let egress_budget_exhausted =
            !self.egress_drained && !self.egress_blocked && self.egress_tx_tokens >= u32::from(EGRESS_LIMIT);
        let exit = if ingress_budget_exhausted || egress_budget_exhausted {
            CooperativePollExit::WorkBudget
        } else if self.egress_blocked {
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
            exit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn egress_credit_does_not_block_reserved_ingress() {
        let mut state = CooperativePollState::new(true);
        state.record_egress(PollResult::None, 0, true, false);
        assert!(!state.can_poll_egress());
        assert!(state.can_poll_ingress());
        assert!(!state.should_self_wake());
    }

    #[test]
    fn ingress_and_egress_budgets_self_wake() {
        let mut ingress = CooperativePollState::new(true);
        for _ in 0..INGRESS_LIMIT {
            ingress.record_ingress(PollIngressSingleResult::PacketProcessed);
        }
        ingress.egress_drained = true;
        assert!(!ingress.can_poll());
        assert!(ingress.should_self_wake());

        let mut egress = CooperativePollState::new(false);
        egress.ingress_drained = true;
        for _ in 0..u32::from(EGRESS_LIMIT) / u32::from(DIRECTION_QUANTUM) {
            egress.record_egress(
                PollResult::SocketStateChanged,
                u32::from(DIRECTION_QUANTUM),
                false,
                true,
            );
        }
        assert!(!egress.can_poll());
        assert!(egress.should_self_wake());
    }

    #[test]
    fn a_quantum_stop_is_not_a_natural_drain() {
        let mut state = CooperativePollState::new(false);
        state.record_egress(PollResult::None, u32::from(DIRECTION_QUANTUM), false, true);
        assert!(!state.egress_drained);
        assert!(state.can_poll_egress());
    }

    #[test]
    fn drained_or_credit_blocked_poll_does_not_spin() {
        let mut drained = CooperativePollState::new(true);
        drained.record_ingress(PollIngressSingleResult::None);
        drained.record_egress(PollResult::None, 0, false, false);
        assert!(!drained.should_self_wake());

        let mut blocked = CooperativePollState::new(true);
        blocked.record_ingress(PollIngressSingleResult::None);
        blocked.record_egress(PollResult::None, 0, true, false);
        assert!(!blocked.should_self_wake());
    }

    #[test]
    fn first_direction_alternates() {
        let mut next = true;
        assert!(take_start_direction(&mut next));
        assert!(!take_start_direction(&mut next));
        assert!(take_start_direction(&mut next));
    }
}
