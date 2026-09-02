use core::task::Context;

use embassy_net_driver::{Capabilities, Checksum, Driver, PacketMeta, RxToken, TxToken};
#[cfg(feature = "tx-egress-metadata")]
use embassy_net_driver::{
    EgressAdmission, EgressDemand, EgressDemandId, EgressDemandLevel, EgressDemandUpdate, EgressGrantCompletion,
    EgressGrantMode, EgressKey, EgressRoute, HardwareAddress,
};
use xarxa::phy::{self, Medium};

pub(crate) struct DriverAdapter<'d, 'c, T>
where
    T: Driver,
{
    // must be Some when actually using this to rx/tx
    pub cx: Option<&'d mut Context<'c>>,
    pub inner: &'d mut T,
    pub medium: Medium,
    pub tx_exhausted: bool,
    pub tx_tokens_issued: u32,
    pub tx_token_limit: Option<u32>,
    pub tx_budget_exhausted: bool,
}

impl<T: Driver> DriverAdapter<'_, '_, T> {
    pub fn take_tx_exhausted(&mut self) -> bool {
        core::mem::replace(&mut self.tx_exhausted, false)
    }

    pub fn set_tx_token_limit(&mut self, limit: Option<u32>) {
        self.tx_token_limit = limit;
        self.tx_budget_exhausted = false;
    }

    pub fn take_tx_budget_exhausted(&mut self) -> bool {
        core::mem::replace(&mut self.tx_budget_exhausted, false)
    }
}

impl<'d, 'c, T> phy::Device for DriverAdapter<'d, 'c, T>
where
    T: Driver,
{
    type RxToken<'a>
        = RxTokenAdapter<T::RxToken<'a>>
    where
        Self: 'a;
    type TxToken<'a>
        = TxTokenAdapter<T::TxToken<'a>>
    where
        Self: 'a;

    fn receive(&mut self) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.inner
            .receive(unwrap!(self.cx.as_deref_mut()))
            .map(|(rx, tx)| (RxTokenAdapter(rx), TxTokenAdapter(tx)))
    }

    /// Construct a transmit token.
    fn transmit(&mut self) -> Option<Self::TxToken<'_>> {
        if self.tx_token_limit.is_some_and(|limit| self.tx_tokens_issued >= limit) {
            self.tx_budget_exhausted = true;
            return None;
        }
        let token = self.inner.transmit(unwrap!(self.cx.as_deref_mut())).map(TxTokenAdapter);

        if token.is_some() {
            self.tx_tokens_issued = self.tx_tokens_issued.saturating_add(1);
        } else {
            self.tx_exhausted = true;
        }

        token
    }

    #[cfg(feature = "tx-egress-metadata")]
    fn transmit_control(&mut self) -> Option<Self::TxToken<'_>> {
        if self.tx_token_limit.is_some_and(|limit| self.tx_tokens_issued >= limit) {
            self.tx_budget_exhausted = true;
            return None;
        }
        let token = self
            .inner
            .transmit_control(unwrap!(self.cx.as_deref_mut()))
            .map(TxTokenAdapter);

        if token.is_some() {
            self.tx_tokens_issued = self.tx_tokens_issued.saturating_add(1);
        } else {
            self.tx_exhausted = true;
        }

        token
    }

    #[cfg(feature = "tx-egress-metadata")]
    fn egress_key(&mut self, route: phy::EgressRoute) -> phy::EgressKey {
        let destination = match route.destination {
            phy::EgressHardwareAddress::Ethernet(address) => HardwareAddress::Ethernet(address),
            phy::EgressHardwareAddress::Ieee802154(address) => HardwareAddress::Ieee802154(address),
            phy::EgressHardwareAddress::Ip => HardwareAddress::Ip,
            _ => return phy::EgressKey::from_route(route),
        };
        let key = self.inner.egress_key(EgressRoute {
            destination,
            traffic_class: route.traffic_class,
        });
        phy::EgressKey::from_words(key.words())
    }

    #[cfg(feature = "tx-egress-metadata")]
    fn transmit_for(&mut self, egress: phy::EgressKey) -> phy::EgressAdmission<Self::TxToken<'_>> {
        if self.tx_token_limit.is_some_and(|limit| self.tx_tokens_issued >= limit) {
            self.tx_budget_exhausted = true;
            return phy::EgressAdmission::GlobalExhausted;
        }
        let request = EgressKey::from_words(egress.words());
        match self.inner.transmit_for(unwrap!(self.cx.as_deref_mut()), request) {
            EgressAdmission::Granted(token) => {
                self.tx_tokens_issued = self.tx_tokens_issued.saturating_add(1);
                phy::EgressAdmission::Granted(TxTokenAdapter(token))
            }
            EgressAdmission::GlobalExhausted => {
                self.tx_exhausted = true;
                phy::EgressAdmission::GlobalExhausted
            }
            EgressAdmission::KeyDeferred => phy::EgressAdmission::KeyDeferred,
        }
    }

    #[cfg(feature = "tx-egress-metadata")]
    fn transmit_granted(&mut self, grant_serial: core::num::NonZeroU32) -> phy::EgressAdmission<Self::TxToken<'_>> {
        if self.tx_token_limit.is_some_and(|limit| self.tx_tokens_issued >= limit) {
            self.tx_budget_exhausted = true;
            return phy::EgressAdmission::GlobalExhausted;
        }
        match self
            .inner
            .transmit_granted(unwrap!(self.cx.as_deref_mut()), grant_serial)
        {
            EgressAdmission::Granted(token) => {
                self.tx_tokens_issued = self.tx_tokens_issued.saturating_add(1);
                phy::EgressAdmission::Granted(TxTokenAdapter(token))
            }
            EgressAdmission::GlobalExhausted => {
                self.tx_exhausted = true;
                phy::EgressAdmission::GlobalExhausted
            }
            EgressAdmission::KeyDeferred => phy::EgressAdmission::KeyDeferred,
        }
    }

    /// Get a description of device capabilities.
    fn capabilities(&self) -> phy::DeviceCapabilities {
        fn convert(c: Checksum) -> phy::Checksum {
            match c {
                Checksum::Both => phy::Checksum::Both,
                Checksum::Tx => phy::Checksum::Tx,
                Checksum::Rx => phy::Checksum::Rx,
                Checksum::None => phy::Checksum::None,
            }
        }
        let caps: Capabilities = self.inner.capabilities();
        let mut smolcaps = phy::DeviceCapabilities::default();

        smolcaps.max_transmission_unit = caps.max_transmission_unit;
        smolcaps.max_burst_size = caps.max_burst_size;
        smolcaps.medium = self.medium.to_driver();
        smolcaps.checksum.ipv4 = convert(caps.checksum.ipv4);
        smolcaps.checksum.tcp = convert(caps.checksum.tcp);
        smolcaps.checksum.udp = convert(caps.checksum.udp);
        #[cfg(feature = "proto-ipv4")]
        {
            smolcaps.checksum.icmpv4 = convert(caps.checksum.icmpv4);
        }
        #[cfg(feature = "proto-ipv6")]
        {
            smolcaps.checksum.icmpv6 = convert(caps.checksum.icmpv6);
        }

        smolcaps
    }

    fn egress_schedule(&mut self) -> Option<phy::EgressSchedule> {
        self.inner.egress_schedule().map(|schedule| {
            phy::EgressSchedule::new(
                schedule.max_packets_per_key(),
                schedule.dispatch_quantum(),
                schedule.epoch(),
                match schedule.grant_mode() {
                    EgressGrantMode::StackSelected => phy::EgressGrantMode::StackSelected,
                    EgressGrantMode::Shadow => phy::EgressGrantMode::Shadow,
                    EgressGrantMode::Authoritative => phy::EgressGrantMode::Authoritative,
                },
            )
        })
    }

    #[cfg(feature = "tx-egress-metadata")]
    fn update_egress_demand(&mut self, update: phy::EgressDemandUpdate) {
        let update = match update {
            phy::EgressDemandUpdate::Reset { schedule_epoch } => EgressDemandUpdate::Reset { schedule_epoch },
            phy::EgressDemandUpdate::Active(demand) => EgressDemandUpdate::Active(EgressDemand::new(
                EgressDemandId::new(demand.id().schedule_epoch(), demand.id().activation()),
                EgressKey::from_words(demand.key().words()),
                EgressDemandLevel::new(demand.level().ready_units(), demand.level().horizon_ready()),
            )),
            phy::EgressDemandUpdate::Inactive { id, key } => EgressDemandUpdate::Inactive {
                id: EgressDemandId::new(id.schedule_epoch(), id.activation()),
                key: EgressKey::from_words(key.words()),
            },
        };
        self.inner.update_egress_demand(unwrap!(self.cx.as_deref_mut()), update);
    }

    #[cfg(feature = "tx-egress-metadata")]
    fn poll_egress_grant(&mut self) -> Option<phy::EgressBurstGrant> {
        self.inner
            .poll_egress_grant(unwrap!(self.cx.as_deref_mut()))
            .map(|grant| {
                let demand = grant.demand();
                phy::EgressBurstGrant::new(
                    grant.serial(),
                    phy::EgressDemand::new(
                        phy::EgressDemandId::new(demand.id().schedule_epoch(), demand.id().activation()),
                        phy::EgressKey::from_words(demand.key().words()),
                        phy::EgressDemandLevel::new(demand.level().ready_units(), demand.level().horizon_ready()),
                    ),
                    grant.frame_credits(),
                    grant.airtime_hundred_nanoseconds(),
                )
            })
    }

    #[cfg(feature = "tx-egress-metadata")]
    fn finish_egress_grant(&mut self, completion: phy::EgressGrantCompletion) {
        self.inner.finish_egress_grant(
            unwrap!(self.cx.as_deref_mut()),
            EgressGrantCompletion::new(
                completion.serial(),
                completion.used_frames(),
                completion
                    .remaining()
                    .map(|remaining| EgressDemandLevel::new(remaining.ready_units(), remaining.horizon_ready())),
            ),
        );
    }
}

pub(crate) struct RxTokenAdapter<T>(T)
where
    T: RxToken;

impl<T> phy::RxToken for RxTokenAdapter<T>
where
    T: RxToken,
{
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        self.0.consume(|buf| {
            #[cfg(feature = "packet-trace")]
            trace!("embassy device rx: {:02x}", buf);
            f(buf)
        })
    }

    fn meta(&self) -> phy::PacketMeta {
        into_xarxa_meta(self.0.meta())
    }
}

pub(crate) struct TxTokenAdapter<T>(T)
where
    T: TxToken;

impl<T> phy::TxToken for TxTokenAdapter<T>
where
    T: TxToken,
{
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        self.0.consume(len, |buf| {
            let r = f(buf);
            #[cfg(feature = "packet-trace")]
            trace!("embassy device tx: {:02x}", buf);
            r
        })
    }

    fn set_meta(&mut self, meta: phy::PacketMeta) {
        self.0.set_meta(into_embassy_net_meta(meta));
    }
}

#[cfg(feature = "packetmeta-timestamp")]
pub(crate) fn into_xarxa_timestamp(timestamp: embassy_net_driver::Timestamp) -> xarxa::phy::Timestamp {
    xarxa::phy::Timestamp {
        seconds: timestamp.seconds,
        quarter_nanos: timestamp.quarter_nanos,
    }
}

#[allow(unused, reason = "meta isn't used if no features are enabled")]
pub(crate) fn into_xarxa_meta(meta: PacketMeta) -> phy::PacketMeta {
    let mut out_meta = phy::PacketMeta::default();
    #[cfg(feature = "packetmeta-id")]
    {
        out_meta.id = meta.id;
    }
    #[cfg(feature = "packetmeta-timestamp")]
    {
        out_meta.timestamp = meta.timestamp.map(into_xarxa_timestamp);
        out_meta.request_timestamp = meta.request_timestamp;
    }
    out_meta
}

#[cfg(feature = "packetmeta-timestamp")]
pub(crate) fn into_embassy_net_timestamp(timestamp: xarxa::phy::Timestamp) -> embassy_net_driver::Timestamp {
    embassy_net_driver::Timestamp {
        seconds: timestamp.seconds,
        quarter_nanos: timestamp.quarter_nanos,
    }
}

#[allow(unused, reason = "meta isn't used if no features are enabled")]
pub(crate) fn into_embassy_net_meta(meta: phy::PacketMeta) -> PacketMeta {
    let mut out_meta = PacketMeta::default();
    #[cfg(feature = "packetmeta-id")]
    {
        out_meta.id = meta.id;
    }
    #[cfg(feature = "packetmeta-timestamp")]
    {
        out_meta.timestamp = meta.timestamp.map(into_embassy_net_timestamp);
        out_meta.request_timestamp = meta.request_timestamp;
    }
    out_meta
}

#[cfg(test)]
mod tests {
    use core::task::{Context, Waker};

    use embassy_net_driver::{Driver, HardwareAddress, LinkState, RxToken, TxToken};
    use xarxa::phy::Device;

    use super::*;

    struct TestDriver {
        transmit_calls: u32,
        #[cfg(feature = "tx-egress-metadata")]
        control_transmit_calls: u32,
        tx_available: bool,
        #[cfg(feature = "tx-egress-metadata")]
        keyed_result: u8,
        #[cfg(feature = "tx-egress-metadata")]
        last_egress: Option<embassy_net_driver::EgressKey>,
        #[cfg(feature = "tx-egress-metadata")]
        schedule: Option<embassy_net_driver::EgressSchedule>,
        #[cfg(feature = "tx-egress-metadata")]
        last_demand: Option<embassy_net_driver::EgressDemandUpdate>,
    }

    struct TestRxToken;

    impl RxToken for TestRxToken {
        fn consume<R, F>(self, f: F) -> R
        where
            F: FnOnce(&mut [u8]) -> R,
        {
            f(&mut [])
        }
    }

    struct TestTxToken;

    impl TxToken for TestTxToken {
        fn consume<R, F>(self, _len: usize, f: F) -> R
        where
            F: FnOnce(&mut [u8]) -> R,
        {
            f(&mut [])
        }
    }

    impl Driver for TestDriver {
        type RxToken<'a> = TestRxToken;
        type TxToken<'a> = TestTxToken;

        fn receive(&mut self, _cx: &mut Context<'_>) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
            None
        }

        fn transmit(&mut self, _cx: &mut Context<'_>) -> Option<Self::TxToken<'_>> {
            self.transmit_calls += 1;
            self.tx_available.then_some(TestTxToken)
        }

        #[cfg(feature = "tx-egress-metadata")]
        fn transmit_control(&mut self, _cx: &mut Context<'_>) -> Option<Self::TxToken<'_>> {
            self.control_transmit_calls += 1;
            self.tx_available.then_some(TestTxToken)
        }

        #[cfg(feature = "tx-egress-metadata")]
        fn egress_key(&mut self, route: embassy_net_driver::EgressRoute) -> embassy_net_driver::EgressKey {
            assert_eq!(
                route,
                embassy_net_driver::EgressRoute {
                    destination: HardwareAddress::Ethernet([2, 3, 4, 5, 6, 7]),
                    traffic_class: 0x28,
                }
            );
            embassy_net_driver::EgressKey::from_words([11, 13, 17, 19])
        }

        #[cfg(feature = "tx-egress-metadata")]
        fn transmit_for(
            &mut self,
            _cx: &mut Context<'_>,
            egress: embassy_net_driver::EgressKey,
        ) -> embassy_net_driver::EgressAdmission<Self::TxToken<'_>> {
            self.transmit_calls += 1;
            self.last_egress = Some(egress);
            match self.keyed_result {
                0 => embassy_net_driver::EgressAdmission::Granted(TestTxToken),
                1 => embassy_net_driver::EgressAdmission::GlobalExhausted,
                _ => embassy_net_driver::EgressAdmission::KeyDeferred,
            }
        }

        #[cfg(feature = "tx-egress-metadata")]
        fn egress_schedule(&mut self) -> Option<embassy_net_driver::EgressSchedule> {
            self.schedule
        }

        #[cfg(feature = "tx-egress-metadata")]
        fn update_egress_demand(&mut self, _cx: &mut Context<'_>, update: embassy_net_driver::EgressDemandUpdate) {
            self.last_demand = Some(update);
        }

        fn link_state(&mut self, _cx: &mut Context<'_>) -> LinkState {
            LinkState::Up
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::default()
        }

        fn hardware_address(&self) -> HardwareAddress {
            HardwareAddress::Ethernet([0; 6])
        }
    }

    fn adapter<'d, 'c>(driver: &'d mut TestDriver, cx: &'d mut Context<'c>) -> DriverAdapter<'d, 'c, TestDriver> {
        DriverAdapter {
            cx: Some(cx),
            inner: driver,
            medium: Medium::Ethernet,
            tx_exhausted: false,
            tx_tokens_issued: 0,
            tx_token_limit: None,
            tx_budget_exhausted: false,
        }
    }

    #[test]
    fn artificial_tx_budget_is_not_hardware_exhaustion() {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut driver = TestDriver {
            transmit_calls: 0,
            #[cfg(feature = "tx-egress-metadata")]
            control_transmit_calls: 0,
            tx_available: true,
            #[cfg(feature = "tx-egress-metadata")]
            keyed_result: 0,
            #[cfg(feature = "tx-egress-metadata")]
            last_egress: None,
            #[cfg(feature = "tx-egress-metadata")]
            schedule: None,
            #[cfg(feature = "tx-egress-metadata")]
            last_demand: None,
        };
        let mut adapter = adapter(&mut driver, &mut cx);
        adapter.set_tx_token_limit(Some(2));

        assert!(Device::transmit(&mut adapter).is_some());
        assert!(Device::transmit(&mut adapter).is_some());
        assert!(Device::transmit(&mut adapter).is_none());
        assert_eq!(adapter.inner.transmit_calls, 2);
        assert!(adapter.take_tx_budget_exhausted());
        assert!(!adapter.take_tx_exhausted());

        adapter.set_tx_token_limit(Some(3));
        assert!(Device::transmit(&mut adapter).is_some());
        assert_eq!(adapter.inner.transmit_calls, 3);
        assert!(!adapter.take_tx_budget_exhausted());
    }

    #[test]
    #[cfg(feature = "tx-egress-metadata")]
    fn control_admission_reaches_the_distinct_driver_reserve() {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut driver = TestDriver {
            transmit_calls: 0,
            control_transmit_calls: 0,
            tx_available: true,
            keyed_result: 0,
            last_egress: None,
            schedule: None,
            last_demand: None,
        };
        let mut adapter = adapter(&mut driver, &mut cx);

        assert!(Device::transmit_control(&mut adapter).is_some());
        assert_eq!(adapter.inner.control_transmit_calls, 1);
        assert_eq!(adapter.inner.transmit_calls, 0);
    }

    #[test]
    fn hardware_tx_exhaustion_remains_distinct() {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut driver = TestDriver {
            transmit_calls: 0,
            #[cfg(feature = "tx-egress-metadata")]
            control_transmit_calls: 0,
            tx_available: false,
            #[cfg(feature = "tx-egress-metadata")]
            keyed_result: 0,
            #[cfg(feature = "tx-egress-metadata")]
            last_egress: None,
            #[cfg(feature = "tx-egress-metadata")]
            schedule: None,
            #[cfg(feature = "tx-egress-metadata")]
            last_demand: None,
        };
        let mut adapter = adapter(&mut driver, &mut cx);
        adapter.set_tx_token_limit(Some(1));

        assert!(Device::transmit(&mut adapter).is_none());
        assert_eq!(adapter.inner.transmit_calls, 1);
        assert!(adapter.take_tx_exhausted());
        assert!(!adapter.take_tx_budget_exhausted());
    }

    #[test]
    #[cfg(feature = "tx-egress-metadata")]
    fn keyed_admission_preserves_driver_classification_and_refusal_class() {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut driver = TestDriver {
            transmit_calls: 0,
            control_transmit_calls: 0,
            tx_available: true,
            keyed_result: 0,
            last_egress: None,
            schedule: Some(embassy_net_driver::EgressSchedule::new(
                core::num::NonZeroU8::new(32).unwrap(),
                core::num::NonZeroU8::new(4).unwrap(),
                7,
                embassy_net_driver::EgressGrantMode::StackSelected,
            )),
            last_demand: None,
        };
        let mut adapter = adapter(&mut driver, &mut cx);
        let route = phy::EgressRoute {
            destination: phy::EgressHardwareAddress::Ethernet([2, 3, 4, 5, 6, 7]),
            traffic_class: 0x28,
        };
        let request = Device::egress_key(&mut adapter, route);
        assert_eq!(request, phy::EgressKey::from_words([11, 13, 17, 19]));
        assert!(matches!(
            Device::transmit_for(&mut adapter, request),
            phy::EgressAdmission::Granted(_)
        ));
        assert_eq!(
            adapter.inner.last_egress,
            Some(embassy_net_driver::EgressKey::from_words([11, 13, 17, 19]))
        );

        adapter.inner.keyed_result = 2;
        assert!(matches!(
            Device::transmit_for(&mut adapter, request),
            phy::EgressAdmission::KeyDeferred
        ));
        assert!(!adapter.take_tx_exhausted());

        adapter.inner.keyed_result = 1;
        assert!(matches!(
            Device::transmit_for(&mut adapter, request),
            phy::EgressAdmission::GlobalExhausted
        ));
        assert!(adapter.take_tx_exhausted());

        let schedule = Device::egress_schedule(&mut adapter).unwrap();
        assert_eq!(schedule.max_packets_per_key().get(), 32);
        assert_eq!(schedule.dispatch_quantum().get(), 4);
        assert_eq!(schedule.epoch(), 7);
        assert_eq!(schedule.grant_mode(), phy::EgressGrantMode::StackSelected);
    }

    #[test]
    #[cfg(feature = "tx-egress-metadata")]
    fn egress_demand_identity_and_level_cross_the_stack_adapter() {
        use core::num::{NonZeroU16, NonZeroU32};

        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut driver = TestDriver {
            transmit_calls: 0,
            control_transmit_calls: 0,
            tx_available: true,
            keyed_result: 0,
            last_egress: None,
            schedule: None,
            last_demand: None,
        };
        let mut adapter = adapter(&mut driver, &mut cx);
        let id = phy::EgressDemandId::new(7, NonZeroU32::new(11).unwrap());
        let key = phy::EgressKey::from_words([2, 3, 5, 7]);
        Device::update_egress_demand(
            &mut adapter,
            phy::EgressDemandUpdate::Active(phy::EgressDemand::new(
                id,
                key,
                phy::EgressDemandLevel::new(NonZeroU16::new(13).unwrap(), true),
            )),
        );

        assert_eq!(
            adapter.inner.last_demand,
            Some(embassy_net_driver::EgressDemandUpdate::Active(
                embassy_net_driver::EgressDemand::new(
                    embassy_net_driver::EgressDemandId::new(7, NonZeroU32::new(11).unwrap()),
                    embassy_net_driver::EgressKey::from_words([2, 3, 5, 7]),
                    embassy_net_driver::EgressDemandLevel::new(NonZeroU16::new(13).unwrap(), true),
                )
            ))
        );
    }

    #[test]
    #[cfg(feature = "tx-egress-metadata")]
    fn grant_and_completion_cross_the_stack_adapter_losslessly() {
        use core::num::{NonZeroU8, NonZeroU16, NonZeroU32};

        struct GrantDriver {
            grant: Option<embassy_net_driver::EgressBurstGrant>,
            completion: Option<embassy_net_driver::EgressGrantCompletion>,
            admission_serial: Option<NonZeroU32>,
        }

        impl Driver for GrantDriver {
            type RxToken<'a> = TestRxToken;
            type TxToken<'a> = TestTxToken;

            fn receive(&mut self, _cx: &mut Context<'_>) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
                None
            }

            fn transmit(&mut self, _cx: &mut Context<'_>) -> Option<Self::TxToken<'_>> {
                None
            }

            fn transmit_granted(
                &mut self,
                _cx: &mut Context<'_>,
                grant_serial: NonZeroU32,
            ) -> embassy_net_driver::EgressAdmission<Self::TxToken<'_>> {
                self.admission_serial = Some(grant_serial);
                embassy_net_driver::EgressAdmission::Granted(TestTxToken)
            }

            fn poll_egress_grant(&mut self, _cx: &mut Context<'_>) -> Option<embassy_net_driver::EgressBurstGrant> {
                self.grant.take()
            }

            fn finish_egress_grant(
                &mut self,
                _cx: &mut Context<'_>,
                completion: embassy_net_driver::EgressGrantCompletion,
            ) {
                self.completion = Some(completion);
            }

            fn link_state(&mut self, _cx: &mut Context<'_>) -> LinkState {
                LinkState::Up
            }

            fn capabilities(&self) -> Capabilities {
                Capabilities::default()
            }

            fn hardware_address(&self) -> HardwareAddress {
                HardwareAddress::Ethernet([0; 6])
            }
        }

        let demand = embassy_net_driver::EgressDemand::new(
            embassy_net_driver::EgressDemandId::new(9, NonZeroU32::new(4).unwrap()),
            embassy_net_driver::EgressKey::from_words([2, 3, 5, 7]),
            embassy_net_driver::EgressDemandLevel::new(NonZeroU16::new(32).unwrap(), true),
        );
        let grant = embassy_net_driver::EgressBurstGrant::new(
            NonZeroU32::new(17).unwrap(),
            demand,
            NonZeroU8::new(32).unwrap(),
            NonZeroU32::new(21_000).unwrap(),
        );
        let mut driver = GrantDriver {
            grant: Some(grant),
            completion: None,
            admission_serial: None,
        };
        let mut cx = Context::from_waker(Waker::noop());
        let mut adapter = DriverAdapter {
            cx: Some(&mut cx),
            inner: &mut driver,
            medium: Medium::Ethernet,
            tx_exhausted: false,
            tx_tokens_issued: 0,
            tx_token_limit: None,
            tx_budget_exhausted: false,
        };

        let observed = Device::poll_egress_grant(&mut adapter).unwrap();
        assert_eq!(observed.serial(), grant.serial());
        assert_eq!(observed.demand().key().words(), demand.key().words());
        assert_eq!(observed.frame_credits(), grant.frame_credits());
        assert!(matches!(
            Device::transmit_granted(&mut adapter, observed.serial()),
            phy::EgressAdmission::Granted(_)
        ));
        assert_eq!(adapter.inner.admission_serial, Some(grant.serial()));
        let remaining = phy::EgressDemandLevel::new(NonZeroU16::new(3).unwrap(), false);
        Device::finish_egress_grant(
            &mut adapter,
            phy::EgressGrantCompletion::new(observed.serial(), 29, Some(remaining)),
        );

        assert_eq!(
            adapter.inner.completion,
            Some(embassy_net_driver::EgressGrantCompletion::new(
                grant.serial(),
                29,
                Some(embassy_net_driver::EgressDemandLevel::new(
                    NonZeroU16::new(3).unwrap(),
                    false,
                )),
            ))
        );
    }
}
