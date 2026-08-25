# embassy-net-driver

This crate contains the driver trait necessary for adding [`embassy-net`](https://crates.io/crates/embassy-net) support
for a new hardware platform.

Drivers and the stack exchange owned packet buffers. A receive driver allocates
from an explicit pool selected by the application or driver, and hands the
resulting owner to the stack. On transmit the driver either accepts ownership
of the complete packet or returns the same owner unchanged. Dropping the last
owner returns the slot to its originating pool, so RX DMA storage and general
stack/backlog storage may live in different memory domains.

`Driver::can_transmit()` describes admission into the driver's bounded software
queue. It does not require a hardware descriptor to be immediately free. This
allows link drivers such as Wi-Fi to queue by peer and traffic class before a
short, shared DMA working set.

If you want to *use* `embassy-net` with already made drivers, you should depend on the main `embassy-net` crate, not on this crate.

If you are writing a driver, you  should depend only on this crate, not on the main `embassy-net` crate.
This will allow your driver to continue working for newer `embassy-net` major versions, without needing an update,
if the driver trait has not had breaking changes.

See also [`embassy-net-driver-channel`](https://crates.io/crates/embassy-net-driver-channel), which provides a higher-level API
to construct a driver that processes packets in its own background task and communicates with the `embassy-net` task via
packet queues for RX and TX.

## Interoperability

This crate can run on any executor.
