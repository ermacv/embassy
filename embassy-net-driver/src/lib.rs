#![no_std]
#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

use core::task::Waker;

pub use xarxa_driver::config::{PACKET_BUF_ALIGN, PACKET_BUF_SIZE};
pub use xarxa_driver::{
    Capabilities, Checksum, ChecksumCapabilities, HardwareAddress, LinkState, Medium, PacketBuf, PacketBufAllocator,
    PacketMeta, PacketPool, PacketPoolStorage, PacketPoolWaiter,
};
#[cfg(feature = "packetmeta-timestamp")]
pub use xarxa_driver::{Timestamp, TxTimestamp};

/// Main `embassy-net` driver API.
///
/// A driver exchanges owned [`PacketBuf`]s with the stack: it hands received
/// frames up and takes completed frames to transmit down. Each packet returns
/// to the pool it originated from when its final owner drops it.
///
/// Unlike the synchronous [`xarxa_driver::Driver`] interface, this contract
/// requires wake support. This is what lets the Embassy runner sleep without
/// polling while retaining a small, object-safe packet ownership boundary.
pub trait Driver {
    /// Get a description of the device's capabilities.
    fn capabilities(&self) -> Capabilities;

    /// Get the device's hardware address.
    ///
    /// Its kind must match [`Capabilities::medium`].
    fn hardware_address(&self) -> HardwareAddress;

    /// Get the link state.
    fn link_state(&mut self) -> LinkState {
        LinkState::Up
    }

    /// Register the runner's waker.
    ///
    /// The driver must wake it when:
    ///
    /// - a frame is available from [`receive`](Self::receive),
    /// - transmit room reappears after [`can_transmit`](Self::can_transmit)
    ///   returned `false`, or
    /// - the value returned by [`link_state`](Self::link_state) changes.
    ///
    /// Registering a new waker replaces the old one. Wakes may be spurious and
    /// are only hints: implementations must retain the underlying level state
    /// until the runner observes it.
    fn register_waker(&mut self, waker: &Waker);

    /// Take ownership of one received frame, if one is ready.
    fn receive(&mut self) -> Option<PacketBuf>;

    /// Whether the driver can accept one more complete frame.
    ///
    /// If this returns `true`, the immediately following [`transmit`](Self::transmit)
    /// call must succeed. This reports admission into the driver's bounded
    /// software queue; it need not mean that a DMA descriptor is free now.
    fn can_transmit(&mut self) -> bool;

    /// Transfer one complete frame to the driver.
    ///
    /// On success the driver owns `buf` until terminal completion. On failure
    /// ownership is returned unchanged in `Err`.
    fn transmit(&mut self, buf: PacketBuf) -> Result<(), PacketBuf>;

    /// Poll for a completed transmit timestamp.
    #[cfg(feature = "packetmeta-timestamp")]
    fn poll_tx_timestamp(&mut self) -> Option<TxTimestamp> {
        None
    }
}

impl<T: Driver + ?Sized> Driver for &mut T {
    fn capabilities(&self) -> Capabilities {
        T::capabilities(self)
    }

    fn hardware_address(&self) -> HardwareAddress {
        T::hardware_address(self)
    }

    fn link_state(&mut self) -> LinkState {
        T::link_state(self)
    }

    fn register_waker(&mut self, waker: &Waker) {
        T::register_waker(self, waker)
    }

    fn receive(&mut self) -> Option<PacketBuf> {
        T::receive(self)
    }

    fn can_transmit(&mut self) -> bool {
        T::can_transmit(self)
    }

    fn transmit(&mut self, buf: PacketBuf) -> Result<(), PacketBuf> {
        T::transmit(self, buf)
    }

    #[cfg(feature = "packetmeta-timestamp")]
    fn poll_tx_timestamp(&mut self) -> Option<TxTimestamp> {
        T::poll_tx_timestamp(self)
    }
}
