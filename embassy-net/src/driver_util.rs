use core::task::Context;

use embassy_net_driver::{Capabilities, Checksum, Driver, PacketMeta, RxToken, TxToken};
#[cfg(feature = "tx-egress-metadata")]
use embassy_net_driver::{EgressAdmission, EgressKey, HardwareAddress};
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
    fn transmit_for(&mut self, egress: phy::EgressKey) -> phy::EgressAdmission<Self::TxToken<'_>> {
        if self.tx_token_limit.is_some_and(|limit| self.tx_tokens_issued >= limit) {
            self.tx_budget_exhausted = true;
            return phy::EgressAdmission::GlobalExhausted;
        }
        let destination = match egress.destination {
            phy::EgressHardwareAddress::Ethernet(address) => HardwareAddress::Ethernet(address),
            phy::EgressHardwareAddress::Ieee802154(address) => HardwareAddress::Ieee802154(address),
            phy::EgressHardwareAddress::Ip => HardwareAddress::Ip,
            _ => return phy::EgressAdmission::KeyDeferred,
        };
        let request = EgressKey {
            destination,
            traffic_class: egress.traffic_class,
        };
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
            )
        })
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
        tx_available: bool,
        #[cfg(feature = "tx-egress-metadata")]
        keyed_result: u8,
        #[cfg(feature = "tx-egress-metadata")]
        last_egress: Option<embassy_net_driver::EgressKey>,
        #[cfg(feature = "tx-egress-metadata")]
        schedule: Option<embassy_net_driver::EgressSchedule>,
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
            tx_available: true,
            #[cfg(feature = "tx-egress-metadata")]
            keyed_result: 0,
            #[cfg(feature = "tx-egress-metadata")]
            last_egress: None,
            #[cfg(feature = "tx-egress-metadata")]
            schedule: None,
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
    fn hardware_tx_exhaustion_remains_distinct() {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut driver = TestDriver {
            transmit_calls: 0,
            tx_available: false,
            #[cfg(feature = "tx-egress-metadata")]
            keyed_result: 0,
            #[cfg(feature = "tx-egress-metadata")]
            last_egress: None,
            #[cfg(feature = "tx-egress-metadata")]
            schedule: None,
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
    fn keyed_admission_preserves_destination_and_refusal_class() {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut driver = TestDriver {
            transmit_calls: 0,
            tx_available: true,
            keyed_result: 0,
            last_egress: None,
            schedule: Some(embassy_net_driver::EgressSchedule::new(
                core::num::NonZeroU8::new(32).unwrap(),
                core::num::NonZeroU8::new(4).unwrap(),
                7,
            )),
        };
        let mut adapter = adapter(&mut driver, &mut cx);
        let request = phy::EgressKey {
            destination: phy::EgressHardwareAddress::Ethernet([2, 3, 4, 5, 6, 7]),
            traffic_class: 0x28,
        };
        assert!(matches!(
            Device::transmit_for(&mut adapter, request),
            phy::EgressAdmission::Granted(_)
        ));
        assert_eq!(
            adapter.inner.last_egress,
            Some(embassy_net_driver::EgressKey {
                destination: HardwareAddress::Ethernet([2, 3, 4, 5, 6, 7]),
                traffic_class: 0x28,
            })
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
    }
}
