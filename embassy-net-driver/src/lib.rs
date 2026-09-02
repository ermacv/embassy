#![no_std]
#![allow(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

#[cfg(feature = "tx-egress-metadata")]
use core::num::{NonZeroU8, NonZeroU16, NonZeroU32};
use core::task::Context;

/// Metadata associated to a packet.
///
/// The packet metadata is a set of attributes associated to network packets
/// as they travel up or down the stack. The metadata is get/set by the
/// [`Driver`] implementations or by the user when sending/receiving packets from a
/// socket.
///
/// Metadata fields are enabled via Cargo features. If no field is enabled, this
/// struct becomes zero-sized, which allows the compiler to optimize it out as if
/// the packet metadata mechanism didn't exist at all.
///
/// This struct is marked as `#[non_exhaustive]`. This means it is not possible to
/// create it directly by specifying all fields. You have to instead create it with
/// default values and then set the fields you want. This makes adding metadata
/// fields a non-breaking change.
///
/// ```rust
/// let mut meta = embassy_net_driver::PacketMeta::default();
/// #[cfg(feature = "packetmeta-id")]
/// {
///     meta.id = 15;
/// }
/// ```
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Default)]
#[non_exhaustive]
pub struct PacketMeta {
    #[cfg(feature = "packetmeta-id")]
    /// An opaque identifier for this packet.
    ///
    /// On received packets, this is set by the [`Device`]. On packets to transmit,
    /// this is set by the user and passed down to the [`Device`]; it is also what
    /// correlates a transmit timestamp back to the packet that produced it, see
    /// [`Device::poll_tx_timestamp`].
    pub id: u32,

    #[cfg(feature = "packetmeta-timestamp")]
    /// The time at which this packet was received, as measured by the device.
    ///
    /// `None` if the device did not timestamp this packet. Devices commonly only
    /// timestamp a subset of received packets, e.g. only PTP event messages.
    ///
    /// This field is only meaningful on received packets. It is ignored on packets
    /// to transmit: at the time a packet is handed to the device, it has not been
    /// transmitted yet, so its transmit timestamp does not exist yet. Use
    /// [`Self::request_timestamp`] and [`Device::poll_tx_timestamp`] instead.
    pub timestamp: Option<Timestamp>,

    #[cfg(feature = "packetmeta-timestamp")]
    /// Request that the device timestamp this packet as it is transmitted.
    ///
    /// The resulting timestamp is reported back later, out of band, via
    /// [`Device::poll_tx_timestamp`], tagged with this packet's [`Self::id`].
    ///
    /// This field is only meaningful on packets to transmit. It is ignored on
    /// received packets.
    ///
    /// Timestamping is opt-in per packet because hardware typically has only a
    /// handful of transmit timestamp slots. Requesting a timestamp for every packet
    /// will cause most of them to be dropped.
    pub request_timestamp: bool,
}

/// Stack-resolved route available before device-specific egress classification.
#[cfg(feature = "tx-egress-metadata")]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct EgressRoute {
    /// Link-layer destination selected after route and neighbor lookup.
    pub destination: HardwareAddress,
    /// Packet traffic class. Zero denotes the default best-effort class.
    pub traffic_class: u8,
}

/// Opaque driver-owned scheduling identity for one resolved egress route.
///
/// Link-layer destinations and physical radio peers are not equivalent on all
/// devices. Drivers canonicalize [`EgressRoute`] before the stack groups
/// queues or requests final TX backing.
#[cfg(feature = "tx-egress-metadata")]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct EgressKey([u32; 4]);

#[cfg(feature = "tx-egress-metadata")]
impl EgressKey {
    /// Construct one driver-owned scheduling key.
    ///
    /// A driver using keyed scheduling must keep this classification stable
    /// for one [`EgressSchedule::epoch`] and advance the epoch whenever a route
    /// could map to a different scheduling domain.
    pub const fn from_words(words: [u32; 4]) -> Self {
        Self(words)
    }

    /// Return the opaque representation for a stack adapter.
    pub const fn words(self) -> [u32; 4] {
        self.0
    }

    /// Losslessly classify a route for drivers without a narrower hardware
    /// scheduling domain.
    pub const fn from_route(route: EgressRoute) -> Self {
        let (kind, low, high) = match route.destination {
            HardwareAddress::Ethernet(address) => (
                1,
                u32::from_le_bytes([address[0], address[1], address[2], address[3]]),
                u16::from_le_bytes([address[4], address[5]]) as u32,
            ),
            HardwareAddress::Ieee802154(address) => (
                2,
                u32::from_le_bytes([address[0], address[1], address[2], address[3]]),
                u32::from_le_bytes([address[4], address[5], address[6], address[7]]),
            ),
            HardwareAddress::Ip => (3, 0, 0),
        };
        Self([kind, low, high, route.traffic_class as u32])
    }
}

/// Result of requesting final TX backing for one resolved egress key.
///
/// Global storage pressure and a key-specific scheduler defer have different
/// queue semantics and must never be collapsed into one `None` result.
#[cfg(feature = "tx-egress-metadata")]
#[derive(Debug)]
pub enum EgressAdmission<T> {
    /// Final backing and one affine admission credit were granted.
    Granted(T),
    /// No final TX backing is currently available for any key.
    GlobalExhausted,
    /// This key is valid but currently outside its scheduler/admission grant.
    KeyDeferred,
}

#[cfg(feature = "tx-egress-metadata")]
impl<T> EgressAdmission<T> {
    /// Transform a granted token without changing either refusal reason.
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> EgressAdmission<U> {
        match self {
            Self::Granted(token) => EgressAdmission::Granted(map(token)),
            Self::GlobalExhausted => EgressAdmission::GlobalExhausted,
            Self::KeyDeferred => EgressAdmission::KeyDeferred,
        }
    }
}

/// Bounded interface-wide scheduling requested by a keyed driver.
///
/// The network stack owns packet queues and resolved link keys. The driver
/// owns final admission and can still defer individual keys through
/// [`EgressAdmission`].
#[cfg(feature = "tx-egress-metadata")]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct EgressSchedule {
    max_packets_per_key: NonZeroU8,
    dispatch_quantum: NonZeroU8,
    epoch: u32,
    grant_mode: EgressGrantMode,
}

/// How a keyed network interface treats a driver-issued egress quantum.
#[cfg(feature = "tx-egress-metadata")]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum EgressGrantMode {
    /// Preserve stack-owned key selection and do not poll driver grants.
    StackSelected,
    /// Observe and report a driver grant without changing packet selection.
    Shadow,
    /// Emit only the exact key and prefix named by a driver grant.
    Authoritative,
}

#[cfg(feature = "tx-egress-metadata")]
impl EgressSchedule {
    /// Create one valid keyed scheduling configuration.
    pub const fn new(
        max_packets_per_key: NonZeroU8,
        dispatch_quantum: NonZeroU8,
        epoch: u32,
        grant_mode: EgressGrantMode,
    ) -> Self {
        Self {
            max_packets_per_key,
            dispatch_quantum,
            epoch,
            grant_mode,
        }
    }

    /// Maximum contiguous packet run selected for one resolved key.
    pub const fn max_packets_per_key(self) -> NonZeroU8 {
        self.max_packets_per_key
    }

    /// Maximum packets emitted from one socket during one interface pass.
    pub const fn dispatch_quantum(self) -> NonZeroU8 {
        self.dispatch_quantum
    }

    /// Driver-owned lifecycle epoch for this scheduling domain.
    pub const fn epoch(self) -> u32 {
        self.epoch
    }

    /// Select stack-owned, observational or authoritative grant behavior.
    pub const fn grant_mode(self) -> EgressGrantMode {
        self.grant_mode
    }
}

/// Stable identity of one nonempty stack-owned egress-demand lifetime.
#[cfg(feature = "tx-egress-metadata")]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct EgressDemandId {
    schedule_epoch: u32,
    activation: NonZeroU32,
}

#[cfg(feature = "tx-egress-metadata")]
impl EgressDemandId {
    /// Construct one demand identity.
    pub const fn new(schedule_epoch: u32, activation: NonZeroU32) -> Self {
        Self {
            schedule_epoch,
            activation,
        }
    }

    /// Driver-owned route-classification epoch.
    pub const fn schedule_epoch(self) -> u32 {
        self.schedule_epoch
    }

    /// Stack-owned nonempty-lifetime serial.
    pub const fn activation(self) -> NonZeroU32 {
        self.activation
    }
}

/// Coalesced amount of currently visible work for one egress demand.
#[cfg(feature = "tx-egress-metadata")]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct EgressDemandLevel {
    ready_units: NonZeroU16,
    horizon_ready: bool,
}

#[cfg(feature = "tx-egress-metadata")]
impl EgressDemandLevel {
    /// Construct one nonempty demand level.
    pub const fn new(ready_units: NonZeroU16, horizon_ready: bool) -> Self {
        Self {
            ready_units,
            horizon_ready,
        }
    }

    /// Bounded point-in-time work estimate.
    pub const fn ready_units(self) -> NonZeroU16 {
        self.ready_units
    }

    /// Whether the stack's useful queueing horizon is currently ready.
    pub const fn horizon_ready(self) -> bool {
        self.horizon_ready
    }
}

/// Identity and current level of one active opaque egress key.
#[cfg(feature = "tx-egress-metadata")]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct EgressDemand {
    id: EgressDemandId,
    key: EgressKey,
    level: EgressDemandLevel,
}

#[cfg(feature = "tx-egress-metadata")]
impl EgressDemand {
    /// Construct one active demand observation.
    pub const fn new(id: EgressDemandId, key: EgressKey, level: EgressDemandLevel) -> Self {
        Self { id, key, level }
    }

    /// Nonempty-lifetime identity.
    pub const fn id(self) -> EgressDemandId {
        self.id
    }

    /// Opaque driver scheduling key.
    pub const fn key(self) -> EgressKey {
        self.key
    }

    /// Coalesced queue level.
    pub const fn level(self) -> EgressDemandLevel {
        self.level
    }
}

/// One ordered stack-to-driver egress-demand transition.
#[cfg(feature = "tx-egress-metadata")]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum EgressDemandUpdate {
    /// Discard every demand from an older route-classification epoch.
    Reset {
        /// New driver-owned scheduling epoch.
        schedule_epoch: u32,
    },
    /// Activate a key or update its coalesced useful level.
    Active(EgressDemand),
    /// End one exact nonempty lifetime.
    Inactive {
        /// Terminal demand identity.
        id: EgressDemandId,
        /// Opaque key retained for direct consumer lookup.
        key: EgressKey,
    },
}

/// One bounded driver-selected egress quantum.
#[cfg(feature = "tx-egress-metadata")]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct EgressBurstGrant {
    serial: NonZeroU32,
    demand: EgressDemand,
    frame_credits: NonZeroU8,
    airtime_hundred_nanoseconds: NonZeroU32,
}

#[cfg(feature = "tx-egress-metadata")]
impl EgressBurstGrant {
    /// Construct one identity-bound, non-zero driver quantum.
    pub const fn new(
        serial: NonZeroU32,
        demand: EgressDemand,
        frame_credits: NonZeroU8,
        airtime_hundred_nanoseconds: NonZeroU32,
    ) -> Self {
        Self {
            serial,
            demand,
            frame_credits,
            airtime_hundred_nanoseconds,
        }
    }

    /// Monotonic driver-owner grant identity.
    pub const fn serial(self) -> NonZeroU32 {
        self.serial
    }

    /// Exact software-demand lifetime selected by the driver.
    pub const fn demand(self) -> EgressDemand {
        self.demand
    }

    /// Maximum number of final packets which may spend this quantum.
    pub const fn frame_credits(self) -> NonZeroU8 {
        self.frame_credits
    }

    /// Conservative complete-quantum airtime reservation in 100 ns units.
    pub const fn airtime_hundred_nanoseconds(self) -> NonZeroU32 {
        self.airtime_hundred_nanoseconds
    }
}

/// Exact stack-side close record for one driver-issued quantum.
#[cfg(feature = "tx-egress-metadata")]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct EgressGrantCompletion {
    serial: NonZeroU32,
    used_frames: u8,
    remaining: Option<EgressDemandLevel>,
}

#[cfg(feature = "tx-egress-metadata")]
impl EgressGrantCompletion {
    /// Construct one terminal stack-side grant record.
    pub const fn new(serial: NonZeroU32, used_frames: u8, remaining: Option<EgressDemandLevel>) -> Self {
        Self {
            serial,
            used_frames,
            remaining,
        }
    }

    /// Exact driver-owner grant identity being closed.
    pub const fn serial(self) -> NonZeroU32 {
        self.serial
    }

    /// Number of final packets materialized from this grant.
    pub const fn used_frames(self) -> u8 {
        self.used_frames
    }

    /// Exact remaining level for the same demand, or `None` when it ended.
    pub const fn remaining(self) -> Option<EgressDemandLevel> {
        self.remaining
    }
}

/// The timestamp of a transmitted packet, reported by [`Device::poll_tx_timestamp`].
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct TxTimestamp {
    /// The [`PacketMeta::id`] of the packet this timestamp belongs to.
    pub id: u32,

    /// The time at which the packet was transmitted, as measured by the device.
    pub timestamp: Timestamp,
}

/// Representation of an hardware address, such as an Ethernet address or an IEEE802.15.4 address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[non_exhaustive]
pub enum HardwareAddress {
    /// Ethernet medium, with a A six-octet Ethernet address.
    ///
    /// Devices of this type send and receive Ethernet frames,
    /// and interfaces using it must do neighbor discovery via ARP or NDISC.
    ///
    /// Examples of devices of this type are Ethernet, WiFi (802.11), Linux `tap`, and VPNs in tap (layer 2) mode.
    Ethernet([u8; 6]),
    /// 6LoWPAN over IEEE802.15.4, with an eight-octet address.
    Ieee802154([u8; 8]),
    /// Indicates that a Driver is IP-native, and has no hardware address.
    ///
    /// Devices of this type send and receive IP frames, without an
    /// Ethernet header. MAC addresses are not used, and no neighbor discovery (ARP, NDISC) is done.
    ///
    /// Examples of devices of this type are the Linux `tun`, PPP interfaces, VPNs in tun (layer 3) mode.
    Ip,
}

/// Main `embassy-net` driver API.
///
/// This is essentially an interface for sending and receiving raw network frames.
///
/// The interface is based on _tokens_, which are types that allow to receive/transmit a
/// single packet. The `receive` and `transmit` functions only construct such tokens, the
/// real sending/receiving operation are performed when the tokens are consumed.
pub trait Driver {
    /// A token to receive a single network packet.
    type RxToken<'a>: RxToken
    where
        Self: 'a;

    /// A token to transmit a single network packet.
    type TxToken<'a>: TxToken
    where
        Self: 'a;

    /// Poll the driver for timestamps and return a pair of (id, `Timestamp`)
    ///
    /// Ids may be reused, and therefore this method should be called before calling
    /// `receive` or `transmit` to avoid deducing an incorrect association.
    #[allow(unused_variables)]
    fn poll_timestamp(&mut self, cx: &mut Context) -> Option<TxTimestamp> {
        None
    }

    /// Construct a token pair consisting of one receive token and one transmit token.
    ///
    /// If there is a packet ready to be received, this function must return `Some`.
    /// If there isn't, it must return `None`, and wake `cx.waker()` when a packet is ready.
    ///
    /// The additional transmit token makes it possible to generate a reply packet based
    /// on the contents of the received packet. For example, this makes it possible to
    /// handle arbitrarily large ICMP echo ("ping") requests, where the all received bytes
    /// need to be sent back, without heap allocation.
    fn receive(&mut self, cx: &mut Context) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)>;

    /// Construct a transmit token.
    ///
    /// If there is free space in the transmit buffer to transmit a packet, this function must return `Some`.
    /// If there isn't, it must return `None`, and wake `cx.waker()` when space becomes available.
    ///
    /// Note that [`TxToken::consume`] is infallible, so it is not allowed to return a token
    /// if there is no free space and fail later.
    fn transmit(&mut self, cx: &mut Context) -> Option<Self::TxToken<'_>>;

    /// Construct a token for bounded network-control traffic.
    ///
    /// The default shares ordinary TX capacity. Drivers with authoritative
    /// keyed scheduling may override this with a fixed reserve so DHCP, DNS
    /// and ICMP cannot be deadlocked behind saturated bulk traffic. This path
    /// must not be used for uncatalogued TCP or raw bulk providers.
    #[cfg(feature = "tx-egress-metadata")]
    fn transmit_control(&mut self, cx: &mut Context) -> Option<Self::TxToken<'_>> {
        self.transmit(cx)
    }

    /// Canonicalize a stack-resolved route into the driver scheduling domain.
    #[cfg(feature = "tx-egress-metadata")]
    fn egress_key(&mut self, route: EgressRoute) -> EgressKey {
        EgressKey::from_route(route)
    }

    /// Request a TX token for a driver-classified egress key.
    ///
    /// The default is the ordinary global admission contract. Key-aware
    /// drivers return [`EgressAdmission::KeyDeferred`] only for peer/VIF/TID
    /// policy; an empty global pool is [`EgressAdmission::GlobalExhausted`].
    #[cfg(feature = "tx-egress-metadata")]
    #[allow(unused_variables)]
    fn transmit_for(&mut self, cx: &mut Context, egress: EgressKey) -> EgressAdmission<Self::TxToken<'_>> {
        match self.transmit(cx) {
            Some(token) => EgressAdmission::Granted(token),
            None => EgressAdmission::GlobalExhausted,
        }
    }

    /// Spend one packet credit from an exact driver-issued egress grant.
    ///
    /// The stack has already selected a queue matching the grant. An
    /// authoritative driver must validate the serial, epoch and remaining
    /// credit, and derive physical packet metadata from its retained grant.
    /// The default rejects because ordinary drivers own no such authority.
    #[cfg(feature = "tx-egress-metadata")]
    #[allow(unused_variables)]
    fn transmit_granted(&mut self, cx: &mut Context, grant_serial: NonZeroU32) -> EgressAdmission<Self::TxToken<'_>> {
        EgressAdmission::KeyDeferred
    }

    /// Get the link state.
    ///
    /// This function must return the current link state of the device, and wake `cx.waker()` when
    /// the link state changes.
    fn link_state(&mut self, cx: &mut Context) -> LinkState;

    /// Get a description of device capabilities.
    fn capabilities(&self) -> Capabilities;

    /// Request bounded keyed scheduling before final driver admission.
    ///
    /// `None` keeps ordinary FIFO dispatch. A schedule groups stack-resolved
    /// keys but does not reserve storage or authorize a peer.
    #[cfg(feature = "tx-egress-metadata")]
    fn egress_schedule(&mut self) -> Option<EgressSchedule> {
        None
    }

    /// Observe one coalesced stack-owned egress-demand transition.
    ///
    /// The update carries no packet, SRAM reservation or transmit authority.
    /// Drivers without asynchronous keyed policy may ignore it.
    #[cfg(feature = "tx-egress-metadata")]
    #[allow(unused_variables)]
    fn update_egress_demand(&mut self, cx: &mut Context, update: EgressDemandUpdate) {}

    /// Poll one driver-selected quantum after publishing software demand.
    #[cfg(feature = "tx-egress-metadata")]
    #[allow(unused_variables)]
    fn poll_egress_grant(&mut self, cx: &mut Context) -> Option<EgressBurstGrant> {
        None
    }

    /// Close one exact driver-issued quantum with the stack's final remaining
    /// demand snapshot.
    #[cfg(feature = "tx-egress-metadata")]
    #[allow(unused_variables)]
    fn finish_egress_grant(&mut self, cx: &mut Context, completion: EgressGrantCompletion) {}

    /// Get the device's hardware address.
    ///
    /// The returned hardware address also determines the "medium" of this driver. This indicates
    /// what kind of packet the sent/received bytes are, and determines some behaviors of
    /// the interface. For example, ARP/NDISC address resolution is only done for Ethernet mediums.
    fn hardware_address(&self) -> HardwareAddress;
}

impl<T: ?Sized + Driver> Driver for &mut T {
    type RxToken<'a>
        = T::RxToken<'a>
    where
        Self: 'a;
    type TxToken<'a>
        = T::TxToken<'a>
    where
        Self: 'a;

    fn poll_timestamp(&mut self, cx: &mut Context) -> Option<TxTimestamp> {
        T::poll_timestamp(self, cx)
    }

    fn transmit(&mut self, cx: &mut Context) -> Option<Self::TxToken<'_>> {
        T::transmit(self, cx)
    }
    #[cfg(feature = "tx-egress-metadata")]
    fn transmit_control(&mut self, cx: &mut Context) -> Option<Self::TxToken<'_>> {
        T::transmit_control(self, cx)
    }
    #[cfg(feature = "tx-egress-metadata")]
    fn egress_key(&mut self, route: EgressRoute) -> EgressKey {
        T::egress_key(self, route)
    }

    #[cfg(feature = "tx-egress-metadata")]
    fn transmit_for(&mut self, cx: &mut Context, egress: EgressKey) -> EgressAdmission<Self::TxToken<'_>> {
        T::transmit_for(self, cx, egress)
    }
    #[cfg(feature = "tx-egress-metadata")]
    fn transmit_granted(&mut self, cx: &mut Context, grant_serial: NonZeroU32) -> EgressAdmission<Self::TxToken<'_>> {
        T::transmit_granted(self, cx, grant_serial)
    }
    fn receive(&mut self, cx: &mut Context) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        T::receive(self, cx)
    }
    fn capabilities(&self) -> Capabilities {
        T::capabilities(self)
    }

    #[cfg(feature = "tx-egress-metadata")]
    fn egress_schedule(&mut self) -> Option<EgressSchedule> {
        T::egress_schedule(self)
    }
    #[cfg(feature = "tx-egress-metadata")]
    fn update_egress_demand(&mut self, cx: &mut Context, update: EgressDemandUpdate) {
        T::update_egress_demand(self, cx, update)
    }
    #[cfg(feature = "tx-egress-metadata")]
    fn poll_egress_grant(&mut self, cx: &mut Context) -> Option<EgressBurstGrant> {
        T::poll_egress_grant(self, cx)
    }
    #[cfg(feature = "tx-egress-metadata")]
    fn finish_egress_grant(&mut self, cx: &mut Context, completion: EgressGrantCompletion) {
        T::finish_egress_grant(self, cx, completion)
    }
    fn link_state(&mut self, cx: &mut Context) -> LinkState {
        T::link_state(self, cx)
    }
    fn hardware_address(&self) -> HardwareAddress {
        T::hardware_address(self)
    }
}

#[cfg(all(test, feature = "tx-egress-metadata"))]
mod egress_tests {
    use super::{EgressKey, EgressRoute, HardwareAddress};

    #[test]
    fn default_key_is_lossless_and_includes_traffic_class() {
        let route = EgressRoute {
            destination: HardwareAddress::Ethernet([0x02, 1, 2, 3, 4, 5]),
            traffic_class: 6,
        };
        let other_destination = EgressRoute {
            destination: HardwareAddress::Ethernet([0x02, 1, 2, 3, 4, 6]),
            traffic_class: 6,
        };
        let other_class = EgressRoute {
            traffic_class: 7,
            ..route
        };

        assert_eq!(EgressKey::from_route(route), EgressKey::from_route(route));
        assert_ne!(EgressKey::from_route(route), EgressKey::from_route(other_destination));
        assert_ne!(EgressKey::from_route(route), EgressKey::from_route(other_class));
    }
}

/// A representation of a hardware packet timestamp.
///
/// This is a reading of the *device's own clock*, not of the `Instant` the stack is polled
/// with. Such a clock is usually called a "PTP hardware clock" or PHC. It has an arbitrary
/// epoch (often, but not necessarily, the time since the device was reset) and it drifts
/// with respect to any other clock in the system unless something is actively disciplining
/// it. Do not mix `Timestamp` and `Instant` values.
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy, Default)]
pub struct Timestamp {
    /// Whole seconds.
    pub seconds: u32,
    /// Fraction of a second, in units of 0.25 nanoseconds.
    ///
    /// Always less than `4_000_000_000`, i.e. less than one whole second.
    pub quarter_nanos: u32,
}

impl Timestamp {
    /// Construct a timestamp from seconds and nanoseconds
    #[inline]
    pub const fn from_seconds_and_nanos(seconds: u32, nanos: u32) -> Self {
        Self {
            seconds,
            quarter_nanos: nanos << 2,
        }
    }

    /// Get the nanoseconds for this timestamp
    #[inline]
    pub const fn nanos(&self) -> u32 {
        self.quarter_nanos >> 2
    }
}

/// A token to receive a single network packet.
pub trait RxToken {
    /// Get the buffer for this packet.
    fn buf(&mut self) -> &mut [u8] {
        &mut []
    }

    /// Consumes the token to receive a single network packet.
    ///
    /// This method receives a packet and then calls the given closure `f` with the raw
    /// packet bytes as argument.
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R;

    /// The Packet metadata associated with the frame received by this [`RxToken`]
    fn meta(&self) -> PacketMeta {
        PacketMeta::default()
    }
}

/// A token to transmit a single network packet.
pub trait TxToken {
    /// Consumes the token to send a single network packet.
    ///
    /// This method constructs a transmit buffer of size `len` and calls the passed
    /// closure `f` with a mutable reference to that buffer. The closure should construct
    /// a valid network packet (e.g. an ethernet packet) in the buffer. When the closure
    /// returns, the transmit buffer is sent out.
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R;

    /// The Packet metadata to be associated with the frame to be transmitted by
    /// this [`TxToken`].
    #[allow(unused_variables)]
    fn set_meta(&mut self, meta: PacketMeta) {}
}

/// A description of device capabilities.
///
/// Higher-level protocols may achieve higher throughput or lower latency if they consider
/// the bandwidth or packet size limitations.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[non_exhaustive]
pub struct Capabilities {
    /// Maximum transmission unit.
    ///
    /// The network device is unable to send or receive frames larger than the value returned
    /// by this function.
    ///
    /// For Ethernet devices, this is the maximum Ethernet frame size, including the Ethernet header (14 octets), but
    /// *not* including the Ethernet FCS (4 octets). Therefore, Ethernet MTU = IP MTU + 14.
    ///
    /// Note that in Linux and other OSes, "MTU" is the IP MTU, not the Ethernet MTU, even for Ethernet
    /// devices. This is a common source of confusion.
    ///
    /// Most common IP MTU is 1500. Minimum is 576 (for IPv4) or 1280 (for IPv6). Maximum is 9216 octets.
    pub max_transmission_unit: usize,

    /// Maximum burst size, in terms of MTU.
    ///
    /// The network device is unable to send or receive bursts large than the value returned
    /// by this function.
    ///
    /// If `None`, there is no fixed limit on burst size, e.g. if network buffers are
    /// dynamically allocated.
    pub max_burst_size: Option<usize>,

    /// Checksum behavior.
    ///
    /// If the network device is capable of verifying or computing checksums for some protocols,
    /// it can request that the stack not do so in software to improve performance.
    pub checksum: ChecksumCapabilities,

    /// If set to true, hardware timestamps are supported.
    pub timestamp: bool,
}

/// A description of checksum behavior for every supported protocol.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[non_exhaustive]
pub struct ChecksumCapabilities {
    /// Checksum behavior for IPv4.
    pub ipv4: Checksum,
    /// Checksum behavior for UDP.
    pub udp: Checksum,
    /// Checksum behavior for TCP.
    pub tcp: Checksum,
    /// Checksum behavior for ICMPv4.
    pub icmpv4: Checksum,
    /// Checksum behavior for ICMPv6.
    pub icmpv6: Checksum,
}

/// A description of checksum behavior for a particular protocol.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum Checksum {
    /// Verify checksum when receiving and compute checksum when sending.
    Both,
    /// Verify checksum when receiving.
    Rx,
    /// Compute checksum before sending.
    Tx,
    /// Ignore checksum completely.
    None,
}

impl Default for Checksum {
    fn default() -> Checksum {
        Checksum::Both
    }
}

/// The link state of a network device.
#[derive(PartialEq, Eq, Clone, Copy)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum LinkState {
    /// The link is down.
    Down,
    /// The link is up.
    Up,
}
