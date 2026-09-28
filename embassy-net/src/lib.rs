#![no_std]
#![allow(async_fn_in_trait)]
#![allow(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

//! ## Feature flags
#![doc = document_features::document_features!(feature_label = r#"<span class="stab portability"><code>{feature}</code></span>"#)]

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(not(any(feature = "ipv4", feature = "ipv6")))]
compile_error!("You must enable at least one of the following features: ipv4, ipv6");

// This mod MUST go first, so that the others see its macros.
pub(crate) mod fmt;

#[cfg(feature = "dns")]
pub mod dns;
pub mod iface;
#[cfg(any(feature = "medium-ethernet", feature = "medium-ieee802154"))]
mod neighbor;
#[cfg(feature = "_raw")]
pub mod raw;
pub mod route;
#[cfg(feature = "tcp")]
pub mod tcp;
mod time;
#[cfg(feature = "udp")]
pub mod udp;

use core::cell::RefCell;
use core::future::{Future, poll_fn};
use core::mem::MaybeUninit;
use core::pin::pin;
use core::task::{Context, Poll};

use embassy_sync::waitqueue::WakerRegistration;
use embassy_time::{Instant, Timer};
use xarxa::driver::{Driver, LinkState, PacketBufAllocator, PacketPoolWaiter};
#[cfg(feature = "hostname")]
use xarxa::error::HostnameTooLong;
use xarxa::iface::IfaceHandle;
pub use xarxa::{PollBudget, config, error, wire};
pub use xarxa_driver as driver;

use crate::iface::{AddIfaceError, Iface};
#[cfg(any(feature = "medium-ethernet", feature = "medium-ieee802154"))]
pub use crate::neighbor::{Neighbor, NeighborCache, NeighborState};
use crate::route::Routes;
use crate::time::{duration_from_xarxa, now_to_xarxa};

/// Error returned by `try_*` socket methods.
///
/// `WouldBlock` indicates the operation would block (e.g. no data available,
/// send buffer full). `Other` wraps the socket-specific error type for any
/// other failure.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum TryError<T> {
    /// The operation would block; try again later.
    WouldBlock,
    /// A socket-specific error occurred.
    Other(T),
}

/// Memory storage needed for a network stack.
///
/// The stack holds this for the rest of the program: put it in a `static`
/// (with `StaticCell`), or declare it before the stack.
///
/// This holds only the stack-wide state. The drivers live wherever the caller
/// puts them, and are handed to [`Stack::add_iface_borrowed`].
///
/// Socket storage is not here either: the stack has a fixed number of socket
/// slots per type, set by the `*-socket-count-N` features of `xarxa`. Packet
/// buffers come from the [`PacketBufAllocator`] given to [`Stack::new`], so the
/// application places them in the memory domain the system needs.
pub struct StackStorage<'d> {
    stack: MaybeUninit<xarxa::Stack<'d>>,
    inner: MaybeUninit<RefCell<Inner<'d>>>,
}

impl<'d> StackStorage<'d> {
    /// Create the storage for a stack.
    pub const fn new() -> Self {
        Self {
            stack: MaybeUninit::uninit(),
            inner: MaybeUninit::uninit(),
        }
    }
}

impl Default for StackStorage<'_> {
    fn default() -> Self {
        Self::new()
    }
}

/// Whether the runner must be woken after a [`Stack::with`] closure, to process
/// what the closure changed.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum WakeRunner {
    Wake,
    NoWake,
}
pub(crate) use WakeRunner::{NoWake, Wake};

/// `Wake` if `wake`, `NoWake` otherwise.
pub(crate) fn wake_if(wake: bool) -> WakeRunner {
    if wake { Wake } else { NoWake }
}

/// Pass `r` through, waking the runner if it is `Ok`.
pub(crate) fn wake_if_ok<T, E>(r: Result<T, E>) -> (Result<T, E>, WakeRunner) {
    let wake = wake_if(r.is_ok());
    (r, wake)
}

pub(crate) struct Inner<'d> {
    pub(crate) stack: &'d mut xarxa::Stack<'d>,
    /// Packet work done in one runner turn.
    poll_budget: PollBudget,
    /// Wakes the runner when the general packet pool frees a buffer.
    packet_pool_waiter: PacketPoolWaiter,
    /// Waker used for triggering polls.
    pub(crate) waker: WakerRegistration,
    /// Sum of every interface's configuration generation at the last poll.
    pub(crate) config_generation: u32,
    #[cfg(feature = "dns")]
    pub(crate) dns: xarxa::dns::DnsClient,
    #[cfg(feature = "dns")]
    pub(crate) dns_waker: WakerRegistration,
    /// DNS servers set by hand, used on top of the ones DHCPv4 learns.
    #[cfg(feature = "dns")]
    pub(crate) static_dns_servers: heapless::Vec<wire::IpAddr, { config::DNS_MAX_SERVER_COUNT }>,
}

/// A network stack.
///
/// This is a handle to the stack created by [`Stack::new`]. It's `Copy`, so
/// you can pass it by value instead of by reference.
#[derive(Copy, Clone)]
pub struct Stack<'d> {
    pub(crate) inner: &'d core::cell::RefCell<Inner<'d>>,
}

impl<'d> Stack<'d> {
    /// Create a network stack.
    ///
    /// `random_seed` seeds the stack's PRNG, which picks TCP initial sequence
    /// numbers and ephemeral ports. This should be random, or at least different
    /// at every boot.
    ///
    /// `packet_allocator` supplies every packet the stack itself creates. Packets
    /// received from a driver keep their own pool origin, which may use a
    /// different capacity and memory placement.
    ///
    /// The stack starts out with no interfaces: add them with
    /// [`add_iface_borrowed`](Self::add_iface_borrowed).
    ///
    /// # Panics
    ///
    /// Panics if another asynchronous stack already owns the allocator's wake
    /// registration. Use a separate general packet pool for each independently
    /// polled stack.
    pub fn new(
        storage: &'d mut StackStorage<'d>,
        random_seed: u64,
        packet_allocator: PacketBufAllocator,
    ) -> (Self, Runner<'d>) {
        let packet_pool_waiter = unwrap!(
            packet_allocator.try_claim_waiter(),
            "an async network stack requires a packet pool with no other waiter"
        );
        let StackStorage {
            stack,
            inner: inner_slot,
        } = storage;
        // Build the array-backed protocol state in its final storage slot.
        // Moving it through a local and then into `Inner` would double the
        // temporary storage construction needs.
        let stack = stack.write(xarxa::Stack::new(random_seed, packet_allocator));

        #[cfg(feature = "dns")]
        // The stack is brand new, so its UDP socket table can only be full if it
        // has no slots at all.
        let dns = unwrap!(
            xarxa::dns::DnsClient::new(stack, &[]).ok(),
            "the DNS client needs a UDP socket, raise the `udp-socket-count-N` feature of xarxa"
        );

        let inner = Inner {
            stack,
            poll_budget: DEFAULT_POLL_BUDGET,
            packet_pool_waiter,
            waker: WakerRegistration::new(),
            config_generation: 0,
            #[cfg(feature = "dns")]
            dns,
            #[cfg(feature = "dns")]
            dns_waker: WakerRegistration::new(),
            #[cfg(feature = "dns")]
            static_dns_servers: heapless::Vec::new(),
        };

        let inner = &*inner_slot.write(core::cell::RefCell::new(inner));
        let stack = Stack { inner };
        (stack, Runner { stack })
    }

    /// Borrow the stack. `f` says whether the runner must be woken afterwards.
    pub(crate) fn with<R>(&self, f: impl FnOnce(&mut Inner<'d>) -> (R, WakeRunner)) -> R {
        let mut inner = self.inner.borrow_mut();
        let (r, wake) = f(&mut inner);
        if wake == Wake {
            inner.waker.wake();
        }
        r
    }

    /// Take the timestamp of an already-transmitted packet, sent with
    /// [`PacketMeta::request_timestamp`](crate::driver::PacketMeta::request_timestamp) set.
    ///
    /// The timestamps of every interface land in one queue, which the [`Runner`] fills
    /// from the drivers. This only reads it, so run the runner concurrently.
    ///
    /// Timestamps arrive an arbitrary time after the packet was sent, possibly out of
    /// order, and possibly never: a device may not support transmit timestamping, may
    /// have run out of timestamp slots, or the queue may have been full (its capacity
    /// is [`TX_TIMESTAMP_QUEUE_COUNT`](crate::config::TX_TIMESTAMP_QUEUE_COUNT)). Time
    /// out waiting for one rather than expecting it.
    ///
    /// The queue has one consumer, so the packet ids it reports back must be unique
    /// across everything the application sends, on every interface. Don't reuse an id
    /// while a timestamp for the old packet can still arrive. Removing an interface
    /// does not drop the timestamps it already queued.
    #[cfg(feature = "packetmeta-timestamp")]
    pub fn poll_tx_timestamp(&self) -> Option<driver::TxTimestamp> {
        self.with(|i| (i.stack.poll_tx_timestamp(), NoWake))
    }

    /// Wait for a TX timestamp from the stack-wide queue.
    ///
    /// Only one task may wait at a time. See [`Self::poll_tx_timestamp`] for packet ID
    /// and delivery requirements. Cancelling a pending wait consumes no timestamp.
    #[cfg(feature = "packetmeta-timestamp")]
    pub fn tx_timestamp(&self) -> impl Future<Output = driver::TxTimestamp> + '_ {
        poll_fn(|cx| {
            self.with(|i| {
                i.stack.register_tx_timestamp_waker(cx.waker());
                (i.stack.poll_tx_timestamp().map_or(Poll::Pending, Poll::Ready), NoWake)
            })
        })
    }

    /// Add an interface to the stack, returning it.
    ///
    /// The stack owns the boxed device, so this needs the `alloc` feature.
    /// Without alloc, use the borrowing [`add_iface_borrowed`](Self::add_iface_borrowed).
    ///
    /// Configure the interface after adding it. At minimum, you will want to
    /// add an IP address to it.
    ///
    /// # Errors
    /// - `Full`: if the stack has no room for another interface. Only possible
    ///   without the `alloc` feature, where the limit is
    ///   [`IFACE_COUNT`](crate::config::IFACE_COUNT).
    /// - `UnsupportedMedium`: if the build has no `medium-*` feature for the
    ///   device's medium.
    /// - `HardwareAddrMismatch`: if the hardware address the device reports is
    ///   not of the kind its medium uses.
    #[cfg(feature = "alloc")]
    pub fn add_iface(&self, driver: alloc::boxed::Box<dyn Driver + 'd>) -> Result<Iface<'d>, AddIfaceError> {
        let handle = self.with(|i| wake_if_ok(i.stack.add_iface(driver)))?;
        Ok(self.iface(handle))
    }

    /// Add an interface to the stack, lending it the device, and returning it.
    ///
    /// The device is borrowed for as long as the stack lives. With a `StaticCell`
    /// that is `'static`; with a local, the enclosing scope.
    /// Otherwise this is `add_iface`.
    ///
    /// # Example
    /// ```ignore
    /// static ETH: StaticCell<Device> = StaticCell::new();
    /// let eth = stack.add_iface_borrowed(ETH.init(device)).unwrap();
    /// ```
    ///
    /// # Errors
    /// - `Full`: if the stack has no room for another interface. Only possible
    ///   without the `alloc` feature, where the limit is
    ///   [`IFACE_COUNT`](crate::config::IFACE_COUNT).
    /// - `UnsupportedMedium`: if the build has no `medium-*` feature for the
    ///   device's medium.
    /// - `HardwareAddrMismatch`: if the hardware address the device reports is
    ///   not of the kind its medium uses.
    pub fn add_iface_borrowed(&self, driver: &'d mut dyn Driver) -> Result<Iface<'d>, AddIfaceError> {
        let handle = self.with(|i| wake_if_ok(i.stack.add_iface_borrowed(driver)))?;
        Ok(self.iface(handle))
    }

    /// Get an interface by its handle.
    ///
    /// # Panics
    /// Panics if the handle is stale (the interface was removed).
    pub fn iface(&self, handle: IfaceHandle) -> Iface<'d> {
        self.with(|i| {
            // Check the handle is live, so a bad one panics here instead of somewhere
            // deeper the first time the interface is used.
            let _ = i.stack.iface(handle).capabilities();
            ((), NoWake)
        });
        Iface::new(*self, handle)
    }

    /// Remove an interface from the stack.
    ///
    /// # Panics
    /// Panics if the handle is stale (the interface was already removed).
    pub fn remove_iface(&self, handle: IfaceHandle) {
        self.with(|i| (i.stack.remove_iface(handle), Wake))
    }

    /// Iterate over the interfaces added to the stack.
    pub fn ifaces(&self) -> impl Iterator<Item = Iface<'d>> + 'd {
        let stack = *self;
        let mut n = 0;
        core::iter::from_fn(move || {
            let handle = stack.with(|i| {
                let mut iter = i.stack.ifaces();
                for _ in 0..n {
                    if iter.next().is_none() {
                        return (None, NoWake);
                    }
                }
                (iter.next().map(|(handle, _)| handle), NoWake)
            })?;
            n += 1;
            Some(stack.iface(handle))
        })
    }

    /// The stack's hostname, or `None` if not set.
    #[cfg(feature = "hostname")]
    pub fn hostname<R>(&self, f: impl FnOnce(Option<&str>) -> R) -> R {
        self.with(|i| (f(i.stack.hostname()), NoWake))
    }

    /// Set the stack's hostname.
    ///
    /// If set, it is sent to the DHCP server in outgoing DHCP messages, as the
    /// host name option.
    ///
    /// An empty string clears the hostname.
    ///
    /// # Errors
    /// - `HostnameTooLong`: if `hostname` is longer than 63 bytes. The hostname
    ///   is left unchanged.
    #[cfg(feature = "hostname")]
    pub fn set_hostname(&self, hostname: &str) -> Result<(), HostnameTooLong> {
        self.with(|i| (i.stack.set_hostname(hostname), NoWake))
    }

    /// Get the packet reassembly timeout.
    ///
    /// This is how long the fragments of an incoming IPv4 or 6LoWPAN packet are
    /// kept while waiting for the rest of it. The default is 60 seconds.
    #[cfg(any(feature = "ipv4-reassembly", feature = "sixlowpan-reassembly"))]
    pub fn reassembly_timeout(&self) -> embassy_time::Duration {
        self.with(|i| (time::duration_from_xarxa(i.stack.reassembly_timeout()), NoWake))
    }

    /// Set the packet reassembly timeout.
    ///
    /// Fragments of an incoming IPv4 or 6LoWPAN packet that is not complete by
    /// then are dropped, and the packet buffer they were kept in is freed.
    #[cfg(any(feature = "ipv4-reassembly", feature = "sixlowpan-reassembly"))]
    pub fn set_reassembly_timeout(&self, timeout: embassy_time::Duration) {
        self.with(|i| (i.stack.set_reassembly_timeout(time::duration_to_xarxa(timeout)), NoWake))
    }

    /// Access the neighbor cache.
    #[cfg(any(feature = "medium-ethernet", feature = "medium-ieee802154"))]
    pub fn neighbor_cache(&self) -> NeighborCache<'d> {
        NeighborCache::new(*self)
    }

    /// Access the routing table.
    pub fn routes(&self) -> Routes<'d> {
        Routes::new(*self)
    }

    /// Set the DNS servers to use, on top of the ones learned from DHCPv4.
    ///
    /// The runner keeps the DNS client's server list in step with the DHCPv4
    /// leases of every interface. The servers set here are used in addition to
    /// those, and come first.
    #[cfg(feature = "dns")]
    pub fn set_dns_servers(&self, servers: &[crate::wire::IpAddr]) {
        self.with(|i| {
            i.static_dns_servers.clear();
            for s in servers {
                if i.static_dns_servers.push(*s).is_err() {
                    warn!("too many DNS servers, dropping the rest");
                    break;
                }
            }
            i.update_dns_servers();
            ((), NoWake)
        })
    }

    /// Make a query for a given name and return the corresponding IP addresses.
    #[cfg(feature = "dns")]
    pub async fn dns_query(
        &self,
        name: &str,
        qtype: dns::DnsQueryType,
    ) -> Result<heapless::Vec<crate::wire::IpAddr, { xarxa::config::DNS_MAX_RESULT_COUNT }>, dns::Error> {
        use crate::wire::IpAddr;

        // For A and AAAA queries we try detect whether `name` is just an IP address
        match qtype {
            #[cfg(feature = "ipv4")]
            dns::DnsQueryType::A => {
                if let Ok(ip) = name.parse().map(IpAddr::V4) {
                    return Ok([ip].into_iter().collect());
                }
            }
            #[cfg(feature = "ipv6")]
            dns::DnsQueryType::Aaaa => {
                if let Ok(ip) = name.parse().map(IpAddr::V6) {
                    return Ok([ip].into_iter().collect());
                }
            }
            _ => {}
        }

        let query = poll_fn(|cx| {
            self.with(|i| {
                let Inner {
                    stack, dns, dns_waker, ..
                } = i;
                match dns.start_query(stack, name, qtype) {
                    Ok(handle) => (Poll::Ready(Ok::<_, dns::Error>(handle)), Wake),
                    Err(xarxa::dns::StartQueryError::NoFreeSlot) => {
                        dns_waker.register(cx.waker());
                        (Poll::Pending, NoWake)
                    }
                    Err(e) => (Poll::Ready(Err(e.into())), NoWake),
                }
            })
        })
        .await?;

        #[must_use = "to delay the drop handler invocation to the end of the scope"]
        struct OnDrop<F: FnOnce()> {
            f: core::mem::MaybeUninit<F>,
        }

        impl<F: FnOnce()> OnDrop<F> {
            fn new(f: F) -> Self {
                Self {
                    f: core::mem::MaybeUninit::new(f),
                }
            }

            fn defuse(self) {
                core::mem::forget(self)
            }
        }

        impl<F: FnOnce()> Drop for OnDrop<F> {
            fn drop(&mut self) {
                unsafe { self.f.as_ptr().read()() }
            }
        }

        let drop = OnDrop::new(|| {
            self.with(|i| {
                i.dns.cancel_query(query);
                i.dns_waker.wake();
                ((), NoWake)
            })
        });

        let res = poll_fn(|cx| {
            self.with(|i| {
                (
                    match i.dns.get_query_result(query) {
                        Ok(addrs) => {
                            i.dns_waker.wake();
                            Poll::Ready(Ok(addrs))
                        }
                        Err(xarxa::dns::GetQueryResultError::Pending) => {
                            i.dns.register_query_waker(query, cx.waker());
                            Poll::Pending
                        }
                        Err(e) => {
                            i.dns_waker.wake();
                            Poll::Ready(Err(e.into()))
                        }
                    },
                    NoWake,
                )
            })
        })
        .await;

        drop.defuse();

        res
    }

    /// Whether any interface has a non-link-local IPv6 address.
    #[cfg(all(feature = "ipv6", feature = "dns", feature = "embedded-nal"))]
    pub(crate) fn any_ipv6(&self) -> bool {
        self.with(|i| {
            let mut iter = i.stack.ifaces();
            while let Some((_, iface)) = iter.next() {
                if iface
                    .ip_addrs()
                    .iter()
                    .any(|a| matches!(a.cidr, xarxa::wire::IpCidr::V6(_)) && !is_link_local(a))
                {
                    return (true, NoWake);
                }
            }
            (false, NoWake)
        })
    }
}

/// Network stack runner.
///
/// You must call [`Runner::run()`] in a background task for the network stack to work.
pub struct Runner<'d> {
    stack: Stack<'d>,
}

// A moderate default for embedded executors. This is a cooperative scheduling
// quantum, not a link-layer aggregation size or a device queue limit.
const DEFAULT_POLL_BUDGET: PollBudget = PollBudget::new(32, 32);

impl<'d> Runner<'d> {
    /// Set the maximum packet work performed in one executor turn.
    ///
    /// Xarxa applies independent ingress and socket-egress limits and rotates
    /// busy interfaces and sockets between turns. A larger quantum amortizes
    /// polling overhead; a smaller one reduces latency for unrelated executor
    /// tasks. The default is 32 frames in each direction.
    pub fn set_poll_budget(&mut self, budget: PollBudget) {
        self.stack.with(|i| {
            i.poll_budget = budget;
            ((), NoWake)
        })
    }

    /// Run the network stack.
    ///
    /// You must call this in a background task, to process network events.
    pub async fn run(&mut self) -> ! {
        poll_fn(|cx| {
            self.stack.with(|i| (i.poll(cx), NoWake));
            Poll::<()>::Pending
        })
        .await;
        unreachable!()
    }
}

impl Inner<'_> {
    /// The sum of every interface's configuration generation.
    ///
    /// Any interface's generation changing changes this, which is all the runner
    /// needs to know that it should look again. One counter instead of one per
    /// interface, since nothing here acts on *which* interface changed.
    fn config_generation(&mut self) -> u32 {
        let mut sum = 0u32;
        let mut iter = self.stack.ifaces();
        while let Some((_, iface)) = iter.next() {
            sum = sum.wrapping_add(iface.config_generation());
        }
        sum
    }

    /// Hand the DNS client the static servers, then those every interface learned
    /// over DHCPv4.
    #[cfg(feature = "dns")]
    pub(crate) fn update_dns_servers(&mut self) {
        let mut servers: heapless::Vec<crate::wire::IpAddr, { xarxa::config::DNS_MAX_SERVER_COUNT }> =
            heapless::Vec::new();
        let mut truncated = false;

        for s in &self.static_dns_servers {
            truncated |= servers.push(*s).is_err();
        }

        #[cfg(feature = "dhcpv4")]
        {
            let mut iter = self.stack.ifaces();
            while let Some((_, iface)) = iter.next() {
                let Some(lease) = iface.dhcpv4_lease() else { continue };
                for s in &lease.dns_servers {
                    truncated |= servers.push((*s).into()).is_err();
                }
            }
        }

        if truncated {
            warn!("Number of DNS servers exceeds DNS_MAX_SERVER_COUNT, truncating list.");
        }

        self.dns.update_servers(&servers);
    }

    /// Log an interface's addresses, after something changed them.
    fn log_config(&mut self) {
        let mut iter = self.stack.ifaces();
        while let Some((handle, iface)) = iter.next() {
            info!("iface {:?}: config changed", handle);
            for addr in iface.ip_addrs() {
                info!("   addr: {:?} ({:?})", addr.cidr, addr.origin);
            }
        }
        for route in self.stack.routes().iter() {
            info!("   route: {:?}", route);
        }
    }

    fn poll(&mut self, cx: &mut Context<'_>) {
        self.waker.register(cx.waker());

        let mut iter = self.stack.ifaces();
        while let Some((_, mut iface)) = iter.next() {
            // embassy-net sleeps until the driver wakes it, so a driver that cannot
            // register a waker would stall the stack forever. Fail loudly instead.
            unwrap!(
                iface.driver_mut().register_waker(cx.waker()),
                "the driver does not support register_waker, which embassy-net requires"
            );
        }

        let now = Instant::now();
        let xnow = now_to_xarxa(now);
        let outcome = self.stack.poll_bounded(xnow, self.poll_budget);
        #[allow(unused_mut)]
        let mut deadline = outcome.deadline();

        #[cfg(feature = "dns")]
        {
            deadline = deadline.min(self.dns.poll(self.stack, xnow));
        }

        // An interface's generation is bumped whenever its addresses or routes
        // change, whoever changed them, so this catches DHCPv4 and SLAAC too.
        let generation = self.config_generation();
        if generation != self.config_generation {
            self.config_generation = generation;
            self.log_config();
            #[cfg(feature = "dns")]
            self.update_dns_servers();
        }

        // A send or poll found the general pool empty: the next freed buffer
        // polls again.
        if self.stack.take_packet_allocator_starved() {
            self.packet_pool_waiter.register(cx.waker());
        }

        if outcome.budget_exhausted() || deadline <= xnow {
            cx.waker().wake_by_ref();
        } else {
            let t = pin!(Timer::at(now + duration_from_xarxa(deadline.duration_since(xnow))));
            if t.poll(cx).is_ready() {
                cx.waker().wake_by_ref();
            }
        }
    }
}

/// Wait until `predicate` holds, re-checking whenever `iface` changes state.
pub(crate) fn wait_iface<'a>(
    stack: Stack<'a>,
    handle: IfaceHandle,
    mut predicate: impl FnMut(&mut xarxa::iface::Iface<'_, 'a>) -> bool + 'a,
) -> impl Future<Output = ()> + 'a {
    poll_fn(move |cx| {
        stack.with(|i| {
            let mut iface = i.stack.iface(handle);
            (
                if predicate(&mut iface) {
                    Poll::Ready(())
                } else {
                    iface.register_waker(cx.waker());
                    Poll::Pending
                },
                NoWake,
            )
        })
    })
}

/// Whether an address is one the stack derived by itself, rather than one that
/// counts as the interface being configured.
pub(crate) fn is_link_local(addr: &xarxa::iface::IfaceAddr) -> bool {
    #[cfg(all(any(feature = "medium-ethernet", feature = "medium-ieee802154"), feature = "ipv6"))]
    {
        addr.origin == xarxa::iface::AddrOrigin::LinkLocal
    }
    #[cfg(not(all(any(feature = "medium-ethernet", feature = "medium-ieee802154"), feature = "ipv6")))]
    {
        let _ = addr;
        false
    }
}

/// Whether an interface counts as configured: it has an address that something
/// other than IPv6 link-local autoconfiguration put there.
pub(crate) fn is_config_up(iface: &xarxa::iface::Iface<'_, '_>) -> bool {
    iface.ip_addrs().iter().any(|a| !is_link_local(a))
}

#[cfg(feature = "ipv4")]
/// Check if any IPv4 address is configured.
pub(crate) fn is_config_v4_up(iface: &xarxa::iface::Iface<'_, '_>) -> bool {
    iface.ip_addrs().iter().any(|a| a.cidr.is_ipv4())
}

#[cfg(feature = "ipv6")]
/// Check if any non link-local IPv6 address is configured.
pub(crate) fn is_config_v6_up(iface: &xarxa::iface::Iface<'_, '_>) -> bool {
    iface.ip_addrs().iter().any(|a| a.cidr.is_ipv6() && !is_link_local(a))
}

/// Whether an interface's link is up.
pub(crate) fn is_link_up(iface: &mut xarxa::iface::Iface<'_, '_>) -> bool {
    iface.link_state() == LinkState::Up
}

#[cfg(all(test, feature = "medium-ethernet", feature = "ipv4", feature = "udp"))]
mod tests {
    extern crate std;

    use core::task::{Context, Waker};
    use std::boxed::Box;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    use xarxa::driver::{Capabilities, HardwareAddress, NotSupported, PacketBuf, PacketPool, PacketPoolStorage};
    use xarxa::wire::{IpCidr, Ipv4Addr, ListenSocketAddr, SocketAddr};

    use super::*;

    struct TestDriver;

    impl Driver for TestDriver {
        fn capabilities(&self) -> Capabilities {
            Capabilities::default()
        }

        fn hardware_address(&self) -> HardwareAddress {
            HardwareAddress::Ethernet([0x02, 0, 0, 0, 0, 1])
        }

        fn register_waker(&mut self, _waker: &Waker) -> Result<(), NotSupported> {
            Ok(())
        }

        fn receive(&mut self) -> Option<PacketBuf> {
            None
        }

        fn can_transmit(&mut self) -> bool {
            true
        }

        fn transmit(&mut self, _buf: PacketBuf) -> Result<(), PacketBuf> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn stack_with_address(
        allocator: PacketBufAllocator,
        address: Ipv4Addr,
    ) -> (Stack<'static>, Runner<'static>, IfaceHandle) {
        let storage = Box::leak(Box::new(StackStorage::new()));
        let (stack, runner) = Stack::new(storage, 1, allocator);
        let iface = stack.add_iface_borrowed(Box::leak(Box::new(TestDriver))).unwrap();
        iface.add_ip_addr(IpCidr::new(address.into(), 24)).unwrap();
        (stack, runner, iface.handle())
    }

    #[test]
    fn a_sender_and_the_runner_wake_when_a_starved_packet_pool_recovers() {
        let storage = Box::leak(Box::new(PacketPoolStorage::<1>::new()));
        let pool = Box::leak(Box::new(PacketPool::new(storage)));
        let allocator = pool.allocator();
        let held = allocator.try_alloc().expect("the only packet slot must allocate");
        let (stack, runner, _) = stack_with_address(allocator, Ipv4Addr::new(192, 0, 2, 1));
        let mut socket = udp::UdpSocket::new(stack).unwrap();
        socket.bind(1234, ListenSocketAddr::UNSPECIFIED).unwrap();
        let remote = SocketAddr::new(Ipv4Addr::new(192, 0, 2, 2).into(), 4321);

        // No busy retry: the pending send and the runner both wait for the pool.
        let sender_wakes = Arc::new(WakeCount::default());
        let sender_waker = Waker::from(sender_wakes.clone());
        let mut send = pin!(socket.send_to(&[1, 2, 3], remote));
        assert!(send.as_mut().poll(&mut Context::from_waker(&sender_waker)).is_pending());
        assert_eq!(sender_wakes.0.load(Ordering::Relaxed), 0);

        let runner_wakes = Arc::new(WakeCount::default());
        let runner_waker = Waker::from(runner_wakes.clone());
        runner
            .stack
            .with(|i| (i.poll(&mut Context::from_waker(&runner_waker)), NoWake));
        assert_eq!(
            sender_wakes.0.load(Ordering::Relaxed),
            0,
            "an empty pool cannot wake the sender"
        );
        let before_release = runner_wakes.0.load(Ordering::Relaxed);

        drop(held);
        assert!(runner_wakes.0.load(Ordering::Relaxed) > before_release);

        runner
            .stack
            .with(|i| (i.poll(&mut Context::from_waker(&runner_waker)), NoWake));
        assert_eq!(
            sender_wakes.0.load(Ordering::Relaxed),
            1,
            "a freed buffer wakes the sender"
        );
        assert!(send.as_mut().poll(&mut Context::from_waker(&sender_waker)).is_ready());
    }

    #[test]
    fn storage_slots_keep_independent_stack_state() {
        let allocator = || {
            let storage = Box::leak(Box::new(PacketPoolStorage::<1>::new()));
            Box::leak(Box::new(PacketPool::new(storage))).allocator()
        };
        let first_address = Ipv4Addr::new(192, 0, 2, 1);
        let second_address = Ipv4Addr::new(192, 0, 2, 2);
        let (first, _first_runner, first_iface) = stack_with_address(allocator(), first_address);
        let (second, _second_runner, second_iface) = stack_with_address(allocator(), second_address);
        first.iface(first_iface).set_ip_addrs([]).unwrap();
        assert!(!first.iface(first_iface).has_ip_addr(first_address));
        assert!(second.iface(second_iface).has_ip_addr(second_address));
    }
}
