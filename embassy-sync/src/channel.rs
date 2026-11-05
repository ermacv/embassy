//! A queue for sending values between asynchronous tasks.
//!
//! It can be used concurrently by multiple producers (senders) and multiple
//! consumers (receivers), i.e. it is an  "MPMC channel".
//!
//! Receivers are competing for messages. So a message that is received by
//! one receiver is not received by any other.
//!
//! This queue takes a Mutex type so that various
//! targets can be attained. For example, a ThreadModeMutex can be used
//! for single-core Cortex-M targets where messages are only passed
//! between tasks running in thread mode. Similarly, a CriticalSectionMutex
//! can also be used for single-core targets where messages are to be
//! passed from exception mode e.g. out of an interrupt handler.
//!
//! This module provides a bounded channel that has a limit on the number of
//! messages that it can store, and if this limit is reached, trying to send
//! another message will result in an error being returned.
//!
//! # Example: Message passing between task and interrupt handler
//!
//! ```rust
//! use embassy_sync::channel::Channel;
//! use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
//!
//! static SHARED_CHANNEL: Channel<CriticalSectionRawMutex, u32, 8> = Channel::new();
//!
//! fn my_interrupt_handler() {
//!     // Do some work..
//!     // ...
//!     if let Err(e) = SHARED_CHANNEL.sender().try_send(42) {
//!         // Channel is full..
//!     }
//! }
//!
//! async fn my_async_task() {
//!     // ...
//!     let receiver = SHARED_CHANNEL.receiver();
//!     loop {
//!         let data_from_interrupt = receiver.receive().await;
//!         // Do something with the data.
//!     }
//! }
//! ```

use core::cell::Cell;
use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};

use crate::blocking_mutex::Mutex;
use crate::blocking_mutex::raw::RawMutex;
use crate::waitqueue::{WaitSlot, wake};

/// Send-only access to a [`Channel`].
#[derive(Debug)]
pub struct Sender<'ch, M, T, const N: usize>
where
    M: RawMutex,
{
    channel: &'ch Channel<M, T, N>,
}

impl<'ch, M, T, const N: usize> Clone for Sender<'ch, M, T, N>
where
    M: RawMutex,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<'ch, M, T, const N: usize> Copy for Sender<'ch, M, T, N> where M: RawMutex {}

impl<'ch, M, T, const N: usize> futures_sink::Sink<T> for Sender<'ch, M, T, N>
where
    M: RawMutex,
{
    type Error = TrySendError<T>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_ready_to_send(cx).map(|()| Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.try_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl<'ch, M, T, const N: usize> Sender<'ch, M, T, N>
where
    M: RawMutex,
{
    /// Sends a value.
    ///
    /// See [`Channel::send()`]
    pub fn send(&self, message: T) -> SendFuture<'ch, M, T, N> {
        self.channel.send(message)
    }

    /// Attempt to immediately send a message.
    ///
    /// See [`Channel::send()`]
    pub fn try_send(&self, message: T) -> Result<(), TrySendError<T>> {
        self.channel.try_send(message)
    }

    /// Allows a poll_fn to poll until the channel is ready to send
    ///
    /// See [`Channel::poll_ready_to_send()`]
    pub fn poll_ready_to_send(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.channel.poll_ready_to_send(cx)
    }

    /// Returns the maximum number of elements the channel can hold.
    ///
    /// See [`Channel::capacity()`]
    pub const fn capacity(&self) -> usize {
        self.channel.capacity()
    }

    /// Returns the free capacity of the channel.
    ///
    /// See [`Channel::free_capacity()`]
    pub fn free_capacity(&self) -> usize {
        self.channel.free_capacity()
    }

    /// Clears all elements in the channel.
    ///
    /// See [`Channel::clear()`]
    pub fn clear(&self) {
        self.channel.clear();
    }

    /// Returns the number of elements currently in the channel.
    ///
    /// See [`Channel::len()`]
    pub fn len(&self) -> usize {
        self.channel.len()
    }

    /// Returns whether the channel is empty.
    ///
    /// See [`Channel::is_empty()`]
    pub fn is_empty(&self) -> bool {
        self.channel.is_empty()
    }

    /// Returns whether the channel is full.
    ///
    /// See [`Channel::is_full()`]
    pub fn is_full(&self) -> bool {
        self.channel.is_full()
    }
}

/// Send-only access to a [`Channel`] without knowing channel size.
pub struct DynamicSender<'ch, T> {
    pub(crate) channel: &'ch dyn DynamicChannel<T>,
}

impl<'ch, T> Clone for DynamicSender<'ch, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'ch, T> Copy for DynamicSender<'ch, T> {}

impl<'ch, M, T, const N: usize> From<Sender<'ch, M, T, N>> for DynamicSender<'ch, T>
where
    M: RawMutex,
{
    fn from(s: Sender<'ch, M, T, N>) -> Self {
        Self { channel: s.channel }
    }
}

impl<'ch, T> DynamicSender<'ch, T> {
    /// Sends a value.
    ///
    /// See [`Channel::send()`]
    pub fn send(&self, message: T) -> DynamicSendFuture<'ch, T> {
        DynamicSendFuture {
            channel: self.channel,
            message: Some(message),
        }
    }

    /// Attempt to immediately send a message.
    ///
    /// See [`Channel::send()`]
    pub fn try_send(&self, message: T) -> Result<(), TrySendError<T>> {
        self.channel.try_send_with_context(message, None)
    }

    /// Allows a poll_fn to poll until the channel is ready to send
    ///
    /// See [`Channel::poll_ready_to_send()`]
    pub fn poll_ready_to_send(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.channel.poll_ready_to_send(cx)
    }
}

/// Send-only access to a [`Channel`] without knowing channel size.
/// This version can be sent between threads but can only be created if the underlying mutex is Sync.
pub struct SendDynamicSender<'ch, T> {
    pub(crate) channel: &'ch dyn DynamicChannel<T>,
}

impl<'ch, T> Clone for SendDynamicSender<'ch, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'ch, T> Copy for SendDynamicSender<'ch, T> {}
unsafe impl<'ch, T: Send> Send for SendDynamicSender<'ch, T> {}
unsafe impl<'ch, T: Send> Sync for SendDynamicSender<'ch, T> {}

impl<'ch, M, T, const N: usize> From<Sender<'ch, M, T, N>> for SendDynamicSender<'ch, T>
where
    M: RawMutex + Sync + Send,
{
    fn from(s: Sender<'ch, M, T, N>) -> Self {
        Self { channel: s.channel }
    }
}

impl<'ch, T> SendDynamicSender<'ch, T> {
    /// Sends a value.
    ///
    /// See [`Channel::send()`]
    pub fn send(&self, message: T) -> DynamicSendFuture<'ch, T> {
        DynamicSendFuture {
            channel: self.channel,
            message: Some(message),
        }
    }

    /// Attempt to immediately send a message.
    ///
    /// See [`Channel::send()`]
    pub fn try_send(&self, message: T) -> Result<(), TrySendError<T>> {
        self.channel.try_send_with_context(message, None)
    }

    /// Allows a poll_fn to poll until the channel is ready to send
    ///
    /// See [`Channel::poll_ready_to_send()`]
    pub fn poll_ready_to_send(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.channel.poll_ready_to_send(cx)
    }
}

/// Receive-only access to a [`Channel`].
#[derive(Debug)]
pub struct Receiver<'ch, M, T, const N: usize>
where
    M: RawMutex,
{
    channel: &'ch Channel<M, T, N>,
}

impl<'ch, M, T, const N: usize> Clone for Receiver<'ch, M, T, N>
where
    M: RawMutex,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<'ch, M, T, const N: usize> Copy for Receiver<'ch, M, T, N> where M: RawMutex {}

impl<'ch, M, T, const N: usize> Receiver<'ch, M, T, N>
where
    M: RawMutex,
{
    /// Receive the next value.
    ///
    /// See [`Channel::receive()`].
    pub fn receive(&self) -> ReceiveFuture<'_, M, T, N> {
        self.channel.receive()
    }

    /// Is a value ready to be received in the channel
    ///
    /// See [`Channel::ready_to_receive()`].
    pub fn ready_to_receive(&self) -> ReceiveReadyFuture<'_, M, T, N> {
        self.channel.ready_to_receive()
    }

    /// Attempt to immediately receive the next value.
    ///
    /// See [`Channel::try_receive()`]
    pub fn try_receive(&self) -> Result<T, TryReceiveError> {
        self.channel.try_receive()
    }

    /// Peek at the next value without removing it from the queue.
    ///
    /// See [`Channel::try_peek()`]
    pub fn try_peek(&self) -> Result<T, TryReceiveError>
    where
        T: Copy,
    {
        self.channel.try_peek()
    }

    /// Allows a poll_fn to poll until the channel is ready to receive
    ///
    /// See [`Channel::poll_ready_to_receive()`]
    pub fn poll_ready_to_receive(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.channel.poll_ready_to_receive(cx)
    }

    /// Poll the channel for the next item
    ///
    /// See [`Channel::poll_receive()`]
    pub fn poll_receive(&self, cx: &mut Context<'_>) -> Poll<T> {
        self.channel.poll_receive(cx)
    }

    /// Returns the maximum number of elements the channel can hold.
    ///
    /// See [`Channel::capacity()`]
    pub const fn capacity(&self) -> usize {
        self.channel.capacity()
    }

    /// Returns the free capacity of the channel.
    ///
    /// See [`Channel::free_capacity()`]
    pub fn free_capacity(&self) -> usize {
        self.channel.free_capacity()
    }

    /// Clears all elements in the channel.
    ///
    /// See [`Channel::clear()`]
    pub fn clear(&self) {
        self.channel.clear();
    }

    /// Returns the number of elements currently in the channel.
    ///
    /// See [`Channel::len()`]
    pub fn len(&self) -> usize {
        self.channel.len()
    }

    /// Returns whether the channel is empty.
    ///
    /// See [`Channel::is_empty()`]
    pub fn is_empty(&self) -> bool {
        self.channel.is_empty()
    }

    /// Returns whether the channel is full.
    ///
    /// See [`Channel::is_full()`]
    pub fn is_full(&self) -> bool {
        self.channel.is_full()
    }
}

/// Receive-only access to a [`Channel`] without knowing channel size.
pub struct DynamicReceiver<'ch, T> {
    pub(crate) channel: &'ch dyn DynamicChannel<T>,
}

impl<'ch, T> Clone for DynamicReceiver<'ch, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'ch, T> Copy for DynamicReceiver<'ch, T> {}

impl<'ch, T> DynamicReceiver<'ch, T> {
    /// Receive the next value.
    ///
    /// See [`Channel::receive()`].
    pub fn receive(&self) -> DynamicReceiveFuture<'_, T> {
        DynamicReceiveFuture { channel: self.channel }
    }

    /// Attempt to immediately receive the next value.
    ///
    /// See [`Channel::try_receive()`]
    pub fn try_receive(&self) -> Result<T, TryReceiveError> {
        self.channel.try_receive_with_context(None)
    }

    /// Peek at the next value without removing it from the queue.
    ///
    /// See [`Channel::try_peek()`]
    pub fn try_peek(&self) -> Result<T, TryReceiveError>
    where
        T: Copy,
    {
        self.channel.try_peek_with_context(None)
    }

    /// Allows a poll_fn to poll until the channel is ready to receive
    ///
    /// See [`Channel::poll_ready_to_receive()`]
    pub fn poll_ready_to_receive(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.channel.poll_ready_to_receive(cx)
    }

    /// Poll the channel for the next item
    ///
    /// See [`Channel::poll_receive()`]
    pub fn poll_receive(&self, cx: &mut Context<'_>) -> Poll<T> {
        self.channel.poll_receive(cx)
    }
}

impl<'ch, M, T, const N: usize> From<Receiver<'ch, M, T, N>> for DynamicReceiver<'ch, T>
where
    M: RawMutex,
{
    fn from(s: Receiver<'ch, M, T, N>) -> Self {
        Self { channel: s.channel }
    }
}

/// Receive-only access to a [`Channel`] without knowing channel size.
/// This version can be sent between threads but can only be created if the underlying mutex is Sync.
pub struct SendDynamicReceiver<'ch, T> {
    pub(crate) channel: &'ch dyn DynamicChannel<T>,
}

/// Receive-only access to a [`Channel`] without knowing channel size.
/// This version can be sent between threads but can only be created if the underlying mutex is Sync.
#[deprecated(since = "0.7.1", note = "please use `SendDynamicReceiver` instead")]
pub type SendableDynamicReceiver<'ch, T> = SendDynamicReceiver<'ch, T>;

impl<'ch, T> Clone for SendDynamicReceiver<'ch, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'ch, T> Copy for SendDynamicReceiver<'ch, T> {}
unsafe impl<'ch, T: Send> Send for SendDynamicReceiver<'ch, T> {}
unsafe impl<'ch, T: Send> Sync for SendDynamicReceiver<'ch, T> {}

impl<'ch, T> SendDynamicReceiver<'ch, T> {
    /// Receive the next value.
    ///
    /// See [`Channel::receive()`].
    pub fn receive(&self) -> DynamicReceiveFuture<'_, T> {
        DynamicReceiveFuture { channel: self.channel }
    }

    /// Attempt to immediately receive the next value.
    ///
    /// See [`Channel::try_receive()`]
    pub fn try_receive(&self) -> Result<T, TryReceiveError> {
        self.channel.try_receive_with_context(None)
    }

    /// Allows a poll_fn to poll until the channel is ready to receive
    ///
    /// See [`Channel::poll_ready_to_receive()`]
    pub fn poll_ready_to_receive(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.channel.poll_ready_to_receive(cx)
    }

    /// Poll the channel for the next item
    ///
    /// See [`Channel::poll_receive()`]
    pub fn poll_receive(&self, cx: &mut Context<'_>) -> Poll<T> {
        self.channel.poll_receive(cx)
    }
}

impl<'ch, M, T, const N: usize> From<Receiver<'ch, M, T, N>> for SendDynamicReceiver<'ch, T>
where
    M: RawMutex + Sync + Send,
{
    fn from(s: Receiver<'ch, M, T, N>) -> Self {
        Self { channel: s.channel }
    }
}

impl<'ch, M, T, const N: usize> futures_core::Stream for Receiver<'ch, M, T, N>
where
    M: RawMutex,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.channel.poll_receive(cx).map(Some)
    }
}

/// Future returned by [`Channel::receive`] and  [`Receiver::receive`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[derive(Debug)]
pub struct ReceiveFuture<'ch, M, T, const N: usize>
where
    M: RawMutex,
{
    channel: &'ch Channel<M, T, N>,
}

impl<'ch, M, T, const N: usize> Future for ReceiveFuture<'ch, M, T, N>
where
    M: RawMutex,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        self.channel.poll_receive(cx)
    }
}

/// Future returned by [`Channel::ready_to_receive`] and  [`Receiver::ready_to_receive`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[derive(Debug)]
pub struct ReceiveReadyFuture<'ch, M, T, const N: usize>
where
    M: RawMutex,
{
    channel: &'ch Channel<M, T, N>,
}

impl<'ch, M, T, const N: usize> Future for ReceiveReadyFuture<'ch, M, T, N>
where
    M: RawMutex,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.channel.poll_ready_to_receive(cx)
    }
}

/// Future returned by [`DynamicReceiver::receive`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct DynamicReceiveFuture<'ch, T> {
    channel: &'ch dyn DynamicChannel<T>,
}

impl<'ch, T> Future for DynamicReceiveFuture<'ch, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        match self.channel.try_receive_with_context(Some(cx)) {
            Ok(v) => Poll::Ready(v),
            Err(TryReceiveError::Empty) => Poll::Pending,
        }
    }
}

impl<'ch, M: RawMutex, T, const N: usize> From<ReceiveFuture<'ch, M, T, N>> for DynamicReceiveFuture<'ch, T> {
    fn from(value: ReceiveFuture<'ch, M, T, N>) -> Self {
        Self { channel: value.channel }
    }
}

/// Future returned by [`Channel::send`] and  [`Sender::send`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[derive(Debug)]
pub struct SendFuture<'ch, M, T, const N: usize>
where
    M: RawMutex,
{
    channel: &'ch Channel<M, T, N>,
    message: Option<T>,
}

impl<'ch, M, T, const N: usize> Future for SendFuture<'ch, M, T, N>
where
    M: RawMutex,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.message.take() {
            Some(m) => match self.channel.try_send_with_context(m, Some(cx)) {
                Ok(..) => Poll::Ready(()),
                Err(TrySendError::Full(m)) => {
                    self.message = Some(m);
                    Poll::Pending
                }
            },
            None => Poll::Ready(()),
        }
    }
}

impl<'ch, M, T, const N: usize> Unpin for SendFuture<'ch, M, T, N> where M: RawMutex {}

/// Future returned by [`DynamicSender::send`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct DynamicSendFuture<'ch, T> {
    channel: &'ch dyn DynamicChannel<T>,
    message: Option<T>,
}

impl<'ch, T> Future for DynamicSendFuture<'ch, T> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.message.take() {
            Some(m) => match self.channel.try_send_with_context(m, Some(cx)) {
                Ok(..) => Poll::Ready(()),
                Err(TrySendError::Full(m)) => {
                    self.message = Some(m);
                    Poll::Pending
                }
            },
            None => Poll::Ready(()),
        }
    }
}

impl<'ch, T> Unpin for DynamicSendFuture<'ch, T> {}

impl<'ch, M: RawMutex, T, const N: usize> From<SendFuture<'ch, M, T, N>> for DynamicSendFuture<'ch, T> {
    fn from(value: SendFuture<'ch, M, T, N>) -> Self {
        Self {
            channel: value.channel,
            message: value.message,
        }
    }
}

pub(crate) trait DynamicChannel<T> {
    fn try_send_with_context(&self, message: T, cx: Option<&mut Context<'_>>) -> Result<(), TrySendError<T>>;

    fn try_receive_with_context(&self, cx: Option<&mut Context<'_>>) -> Result<T, TryReceiveError>;

    fn try_peek_with_context(&self, cx: Option<&mut Context<'_>>) -> Result<T, TryReceiveError>
    where
        T: Copy;

    fn poll_ready_to_send(&self, cx: &mut Context<'_>) -> Poll<()>;
    fn poll_ready_to_receive(&self, cx: &mut Context<'_>) -> Poll<()>;

    fn poll_receive(&self, cx: &mut Context<'_>) -> Poll<T>;
}

/// Error returned by [`try_receive`](Channel::try_receive).
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum TryReceiveError {
    /// A message could not be received because the channel is empty.
    Empty,
}

/// Error returned by [`try_send`](Channel::try_send).
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum TrySendError<T> {
    /// The data could not be sent on the channel because the channel is
    /// currently full and sending would require blocking.
    Full(T),
}

/// The channel's state, kept in cells: a message's `clone` and `drop` and a
/// waker's `clone`, `wake` and `drop` are foreign code, which may call the
/// channel again, so none runs inside the state and every step leaves it whole.
struct ChannelState<T, const N: usize> {
    /// The `len` messages from `head`, in order, wrapping at `N`.
    slots: [Cell<Option<T>>; N],
    head: Cell<usize>,
    len: Cell<usize>,
    receiver_waker: WaitSlot,
    senders_waker: WaitSlot,
}

impl<T, const N: usize> fmt::Debug for ChannelState<T, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChannelState")
            .field("len", &self.len.get())
            .finish_non_exhaustive()
    }
}

impl<T, const N: usize> ChannelState<T, N> {
    const fn new() -> Self {
        ChannelState {
            slots: [const { Cell::new(None) }; N],
            head: Cell::new(0),
            len: Cell::new(0),
            receiver_waker: WaitSlot::new(),
            senders_waker: WaitSlot::new(),
        }
    }

    /// The position `offset` places after the head, for an `offset` up to `N`.
    fn position(&self, offset: usize) -> usize {
        let at = self.head.get().wrapping_add(offset);
        if at >= N { at.wrapping_sub(N) } else { at }
    }

    /// Queue `message` behind the others, or hand it back when there is no room.
    fn push(&self, message: T) -> Result<(), T> {
        let len = self.len.get();
        let place = if len < N {
            self.slots.get(self.position(len))
        } else {
            None
        };
        let Some(place) = place else {
            return Err(message);
        };
        place.set(Some(message));
        self.len.set(len.wrapping_add(1));
        Ok(())
    }

    /// The message at the head, out of the queue.
    fn pop(&self) -> Option<T> {
        let message = self.slots.get(self.head.get())?.take()?;
        self.head.set(self.position(1));
        self.len.set(self.len.get().wrapping_sub(1));
        Some(message)
    }

    /// The message at the head. `Copy` runs no foreign code.
    fn front(&self) -> Option<T>
    where
        T: Copy,
    {
        self.slots.get(self.head.get())?.get()
    }

    /// The senders to wake, when a full queue is about to get room.
    fn senders_to_wake(&self) -> Option<Waker> {
        if self.is_full() {
            self.senders_waker.take()
        } else {
            None
        }
    }

    fn is_empty(&self) -> bool {
        self.len.get() == 0
    }

    fn is_full(&self) -> bool {
        self.len.get() == N
    }
}

/// A bounded channel for communicating between asynchronous tasks
/// with backpressure.
///
/// The channel will buffer up to the provided number of messages.  Once the
/// buffer is full, attempts to `send` new messages will wait until a message is
/// received from the channel.
///
/// All data sent will become available in the same order as it was sent.
#[derive(Debug)]
pub struct Channel<M, T, const N: usize>
where
    M: RawMutex,
{
    inner: Mutex<M, ChannelState<T, N>>,
}

impl<M, T, const N: usize> Channel<M, T, N>
where
    M: RawMutex,
{
    /// Establish a new bounded channel. For example, to create one with a NoopMutex:
    ///
    /// ```
    /// use embassy_sync::channel::Channel;
    /// use embassy_sync::blocking_mutex::raw::NoopRawMutex;
    ///
    /// // Declare a bounded channel of 3 u32s.
    /// let mut channel = Channel::<NoopRawMutex, u32, 3>::new();
    /// ```
    pub const fn new() -> Self {
        Self {
            inner: Mutex::new(ChannelState::new()),
        }
    }

    /// Run `step` on the state. It answers, or hands `arg` back when the
    /// caller has to wait; with a context, the caller then stays the waiter
    /// of `place` until something wakes it.
    ///
    /// Foreign code runs only between the visits to the state: the waker
    /// `step` hands out, and the context's clone, after which `step` runs
    /// again on what the state became. The waiter is in its place before
    /// the one it displaced wakes.
    fn run<A, R>(
        &self,
        arg: A,
        cx: Option<&mut Context<'_>>,
        place: impl Fn(&ChannelState<T, N>) -> &WaitSlot,
        step: impl Fn(&ChannelState<T, N>, A) -> (Result<R, A>, Option<Waker>),
    ) -> Result<R, A> {
        let waker = cx.as_ref().map(|cx| cx.waker());
        let (answer, woken, unregistered) = self.inner.lock(|s| {
            let (answer, woken) = step(s, arg);
            let unregistered = answer.is_err() && waker.is_some_and(|waker| !place(s).holds(waker));
            (answer, woken, unregistered)
        });
        wake(woken);
        let (arg, waker) = match (answer, waker) {
            (Err(arg), Some(waker)) if unregistered => (arg, waker),
            (answer, _) => return answer,
        };
        let fresh = waker.clone();
        let (answer, woken, spare) = self.inner.lock(|s| {
            let (answer, woken) = step(s, arg);
            let spare = if answer.is_err() {
                place(s).replace(fresh)
            } else {
                Some(fresh)
            };
            (answer, woken, spare)
        });
        wake(woken);
        if answer.is_err() {
            // The displaced waiter wakes to wait again.
            wake(spare);
        } else {
            drop(spare);
        }
        answer
    }

    fn try_receive_with_context(&self, cx: Option<&mut Context<'_>>) -> Result<T, TryReceiveError> {
        self.run(
            (),
            cx,
            |s| &s.receiver_waker,
            |s, ()| {
                let woken = s.senders_to_wake();
                (s.pop().ok_or(()), woken)
            },
        )
        .map_err(|()| TryReceiveError::Empty)
    }

    fn try_peek_with_context(&self, cx: Option<&mut Context<'_>>) -> Result<T, TryReceiveError>
    where
        T: Copy,
    {
        self.run(
            (),
            cx,
            |s| &s.receiver_waker,
            |s, ()| {
                let woken = s.senders_to_wake();
                (s.front().ok_or(()), woken)
            },
        )
        .map_err(|()| TryReceiveError::Empty)
    }

    /// Poll the channel for the next message
    pub fn poll_receive(&self, cx: &mut Context<'_>) -> Poll<T> {
        match self.try_receive_with_context(Some(cx)) {
            Ok(message) => Poll::Ready(message),
            Err(TryReceiveError::Empty) => Poll::Pending,
        }
    }

    fn try_send_with_context(&self, m: T, cx: Option<&mut Context<'_>>) -> Result<(), TrySendError<T>> {
        self.run(
            m,
            cx,
            |s| &s.senders_waker,
            |s, m| match s.push(m) {
                Ok(()) => (Ok(()), s.receiver_waker.take()),
                Err(m) => (Err(m), None),
            },
        )
        .map_err(TrySendError::Full)
    }

    /// Allows a poll_fn to poll until the channel is ready to receive
    pub fn poll_ready_to_receive(&self, cx: &mut Context<'_>) -> Poll<()> {
        let ready = self.run(
            (),
            Some(cx),
            |s| &s.receiver_waker,
            |s, ()| (if s.is_empty() { Err(()) } else { Ok(()) }, None),
        );
        if ready.is_ok() { Poll::Ready(()) } else { Poll::Pending }
    }

    /// Allows a poll_fn to poll until the channel is ready to send
    pub fn poll_ready_to_send(&self, cx: &mut Context<'_>) -> Poll<()> {
        let ready = self.run(
            (),
            Some(cx),
            |s| &s.senders_waker,
            |s, ()| (if s.is_full() { Err(()) } else { Ok(()) }, None),
        );
        if ready.is_ok() { Poll::Ready(()) } else { Poll::Pending }
    }

    /// Get a sender for this channel.
    pub fn sender(&self) -> Sender<'_, M, T, N> {
        Sender { channel: self }
    }

    /// Get a receiver for this channel.
    pub fn receiver(&self) -> Receiver<'_, M, T, N> {
        Receiver { channel: self }
    }

    /// Get a sender for this channel using dynamic dispatch.
    pub fn dyn_sender(&self) -> DynamicSender<'_, T> {
        DynamicSender { channel: self }
    }

    /// Get a receiver for this channel using dynamic dispatch.
    pub fn dyn_receiver(&self) -> DynamicReceiver<'_, T> {
        DynamicReceiver { channel: self }
    }

    /// Send a value, waiting until there is capacity.
    ///
    /// Sending completes when the value has been pushed to the channel's queue.
    /// This doesn't mean the value has been received yet.
    pub fn send(&self, message: T) -> SendFuture<'_, M, T, N> {
        SendFuture {
            channel: self,
            message: Some(message),
        }
    }

    /// Attempt to immediately send a message.
    ///
    /// This method differs from [`send`](Channel::send) by returning immediately if the channel's
    /// buffer is full, instead of waiting.
    ///
    /// # Errors
    ///
    /// If the channel capacity has been reached, i.e., the channel has `n`
    /// buffered values where `n` is the argument passed to [`Channel`], then an
    /// error is returned.
    pub fn try_send(&self, message: T) -> Result<(), TrySendError<T>> {
        self.try_send_with_context(message, None)
    }

    /// Receive the next value.
    ///
    /// If there are no messages in the channel's buffer, this method will
    /// wait until a message is sent.
    pub fn receive(&self) -> ReceiveFuture<'_, M, T, N> {
        ReceiveFuture { channel: self }
    }

    /// Is a value ready to be received in the channel
    ///
    /// If there are no messages in the channel's buffer, this method will
    /// wait until there is at least one
    pub fn ready_to_receive(&self) -> ReceiveReadyFuture<'_, M, T, N> {
        ReceiveReadyFuture { channel: self }
    }

    /// Attempt to immediately receive a message.
    ///
    /// This method will either receive a message from the channel immediately or return an error
    /// if the channel is empty.
    pub fn try_receive(&self) -> Result<T, TryReceiveError> {
        self.try_receive_with_context(None)
    }

    /// Peek at the next value without removing it from the queue.
    ///
    /// This method will either receive a copy of the message from the channel immediately or return
    /// an error if the channel is empty.
    pub fn try_peek(&self) -> Result<T, TryReceiveError>
    where
        T: Copy,
    {
        self.try_peek_with_context(None)
    }

    /// Returns the maximum number of elements the channel can hold.
    pub const fn capacity(&self) -> usize {
        N
    }

    /// Returns the free capacity of the channel.
    ///
    /// This is equivalent to `capacity() - len()`
    pub fn free_capacity(&self) -> usize {
        N - self.len()
    }

    /// Clears the elements the channel holds when this is called. One sent
    /// meanwhile, by a dropped message or anyone else, stays.
    pub fn clear(&self) {
        // The whole portion leaves the state in one visit, before any drop:
        // a drop is foreign code, and what it sends is no part of the portion.
        let (messages, woken): ([Option<T>; N], _) = self.inner.lock(|s| {
            let woken = s.senders_to_wake();
            (core::array::from_fn(|_| s.pop()), woken)
        });
        wake(woken);
        drop(messages);
    }

    /// Returns the number of elements currently in the channel.
    pub fn len(&self) -> usize {
        self.inner.lock(|s| s.len.get())
    }

    /// Returns whether the channel is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.lock(ChannelState::is_empty)
    }

    /// Returns whether the channel is full.
    pub fn is_full(&self) -> bool {
        self.inner.lock(ChannelState::is_full)
    }
}

/// Implements the DynamicChannel to allow creating types that are unaware of the queue size with the
/// tradeoff cost of dynamic dispatch.
impl<M, T, const N: usize> DynamicChannel<T> for Channel<M, T, N>
where
    M: RawMutex,
{
    fn try_send_with_context(&self, m: T, cx: Option<&mut Context<'_>>) -> Result<(), TrySendError<T>> {
        Channel::try_send_with_context(self, m, cx)
    }

    fn try_receive_with_context(&self, cx: Option<&mut Context<'_>>) -> Result<T, TryReceiveError> {
        Channel::try_receive_with_context(self, cx)
    }

    fn try_peek_with_context(&self, cx: Option<&mut Context<'_>>) -> Result<T, TryReceiveError>
    where
        T: Copy,
    {
        Channel::try_peek_with_context(self, cx)
    }

    fn poll_ready_to_send(&self, cx: &mut Context<'_>) -> Poll<()> {
        Channel::poll_ready_to_send(self, cx)
    }

    fn poll_ready_to_receive(&self, cx: &mut Context<'_>) -> Poll<()> {
        Channel::poll_ready_to_receive(self, cx)
    }

    fn poll_receive(&self, cx: &mut Context<'_>) -> Poll<T> {
        Channel::poll_receive(self, cx)
    }
}

impl<M, T, const N: usize> futures_core::Stream for Channel<M, T, N>
where
    M: RawMutex,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_receive(cx).map(Some)
    }
}

impl<M, T, const N: usize> futures_sink::Sink<T> for Channel<M, T, N>
where
    M: RawMutex,
{
    type Error = TrySendError<T>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_ready_to_send(cx).map(|()| Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.try_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use futures_executor::ThreadPool;
    use futures_timer::Delay;
    use futures_util::task::SpawnExt;
    use static_cell::StaticCell;

    use super::*;
    use crate::blocking_mutex::raw::{CriticalSectionRawMutex, NoopRawMutex};

    fn capacity<M: RawMutex, T, const N: usize>(c: &Channel<M, T, N>) -> usize {
        c.capacity() - c.len()
    }

    #[test]
    fn sending_once() {
        let c = Channel::<NoopRawMutex, u32, 3>::new();
        assert!(c.try_send(1).is_ok());
        assert_eq!(capacity(&c), 2);
    }

    #[test]
    fn sending_when_full() {
        let c = Channel::<NoopRawMutex, u32, 3>::new();
        let _ = c.try_send(1);
        let _ = c.try_send(1);
        let _ = c.try_send(1);
        match c.try_send(2) {
            Err(TrySendError::Full(2)) => assert!(true),
            _ => assert!(false),
        }
        assert_eq!(capacity(&c), 0);
    }

    #[test]
    fn receiving_once_with_one_send() {
        let c = Channel::<NoopRawMutex, u32, 3>::new();
        assert!(c.try_send(1).is_ok());
        assert_eq!(c.try_receive().unwrap(), 1);
        assert_eq!(capacity(&c), 3);
    }

    #[test]
    fn receiving_when_empty() {
        let c = Channel::<NoopRawMutex, u32, 3>::new();
        match c.try_receive() {
            Err(TryReceiveError::Empty) => assert!(true),
            _ => assert!(false),
        }
        assert_eq!(capacity(&c), 3);
    }

    #[test]
    fn simple_send_and_receive() {
        let c = Channel::<NoopRawMutex, u32, 3>::new();
        assert!(c.try_send(1).is_ok());
        assert_eq!(c.try_peek().unwrap(), 1);
        assert_eq!(c.try_peek().unwrap(), 1);
        assert_eq!(c.try_receive().unwrap(), 1);
    }

    #[test]
    fn cloning() {
        let c = Channel::<NoopRawMutex, u32, 3>::new();
        let r1 = c.receiver();
        let s1 = c.sender();

        let _ = r1.clone();
        let _ = s1.clone();
    }

    #[test]
    fn dynamic_dispatch_into() {
        let c = Channel::<NoopRawMutex, u32, 3>::new();
        let s: DynamicSender<'_, u32> = c.sender().into();
        let r: DynamicReceiver<'_, u32> = c.receiver().into();

        assert!(s.try_send(1).is_ok());
        assert_eq!(r.try_receive().unwrap(), 1);
    }

    #[test]
    fn dynamic_dispatch_constructor() {
        let c = Channel::<NoopRawMutex, u32, 3>::new();
        let s = c.dyn_sender();
        let r = c.dyn_receiver();

        assert!(s.try_send(1).is_ok());
        assert_eq!(r.try_peek().unwrap(), 1);
        assert_eq!(r.try_peek().unwrap(), 1);
        assert_eq!(r.try_receive().unwrap(), 1);
    }

    #[futures_test::test]
    async fn receiver_receives_given_try_send_async() {
        let executor = ThreadPool::new().unwrap();

        static CHANNEL: StaticCell<Channel<CriticalSectionRawMutex, u32, 3>> = StaticCell::new();
        let c = &*CHANNEL.init(Channel::new());
        let c2 = c;
        assert!(
            executor
                .spawn(async move {
                    assert!(c2.try_send(1).is_ok());
                })
                .is_ok()
        );
        assert_eq!(c.receive().await, 1);
    }

    #[futures_test::test]
    async fn sender_send_completes_if_capacity() {
        let c = Channel::<CriticalSectionRawMutex, u32, 1>::new();
        c.send(1).await;
        assert_eq!(c.receive().await, 1);
    }

    #[futures_test::test]
    async fn senders_sends_wait_until_capacity() {
        let executor = ThreadPool::new().unwrap();

        static CHANNEL: StaticCell<Channel<CriticalSectionRawMutex, u32, 1>> = StaticCell::new();
        let c = &*CHANNEL.init(Channel::new());
        assert!(c.try_send(1).is_ok());

        let c2 = c;
        let send_task_1 = executor.spawn_with_handle(async move { c2.send(2).await });
        let c2 = c;
        let send_task_2 = executor.spawn_with_handle(async move { c2.send(3).await });
        // Wish I could think of a means of determining that the async send is waiting instead.
        // However, I've used the debugger to observe that the send does indeed wait.
        Delay::new(Duration::from_millis(500)).await;
        assert_eq!(c.receive().await, 1);
        assert!(
            executor
                .spawn(async move {
                    loop {
                        c.receive().await;
                    }
                })
                .is_ok()
        );
        send_task_1.unwrap().await;
        send_task_2.unwrap().await;
    }

    mod reentry {
        use core::cell::RefCell;
        use core::future::Future;
        use core::pin::pin;
        use core::sync::atomic::{AtomicUsize, Ordering};
        use core::task::{Context, Poll};
        use std::boxed::Box;
        use std::rc::Rc;

        use super::{Channel, TrySendError};
        use crate::blocking_mutex::raw::CriticalSectionRawMutex as Raw;
        use crate::waitqueue::hooks::{Event, on, ran, waker};

        fn channel<T: 'static, const N: usize>() -> &'static Channel<Raw, T, N> {
            Box::leak(Box::new(Channel::new()))
        }

        #[test]
        fn a_receive_from_a_wake_inside_registration_takes_the_message() {
            let channel = channel::<u32, 1>();
            channel.try_send(7).unwrap();
            let (first, _) = waker();
            let (second, _) = waker();
            assert!(
                channel
                    .poll_ready_to_send(&mut Context::from_waker(&first))
                    .is_pending()
            );
            let (receiver, _) = waker();
            static ANSWER: AtomicUsize = AtomicUsize::new(0);
            on(Event::Wake, move || {
                if let Poll::Ready(message) = channel.poll_receive(&mut Context::from_waker(&receiver)) {
                    ANSWER.store(message as usize, Ordering::SeqCst);
                }
            });
            assert!(
                channel
                    .poll_ready_to_send(&mut Context::from_waker(&second))
                    .is_pending()
            );
            assert!(ran(Event::Wake));
            assert_eq!(ANSWER.load(Ordering::SeqCst), 7);
            assert!(channel.is_empty());
        }

        #[test]
        fn a_send_from_a_wake_inside_registration_reaches_the_new_waiter() {
            let channel = channel::<u32, 1>();
            let (first, _) = waker();
            let (second, second_wakes) = waker();
            assert!(channel.poll_receive(&mut Context::from_waker(&first)).is_pending());
            on(Event::Wake, move || assert_eq!(channel.try_send(9), Ok(())));
            assert!(channel.poll_receive(&mut Context::from_waker(&second)).is_pending());
            assert!(ran(Event::Wake));
            assert_eq!(second_wakes.load(Ordering::SeqCst), 1, "the waiter is told");
            assert_eq!(channel.poll_receive(&mut Context::from_waker(&second)), Poll::Ready(9));
        }

        #[test]
        fn nested_reads_inside_registration_answer_the_truth() {
            let channel = channel::<u32, 2>();
            channel.try_send(1).unwrap();
            channel.try_send(2).unwrap();
            let (first, _) = waker();
            let (second, _) = waker();
            assert!(
                channel
                    .poll_ready_to_send(&mut Context::from_waker(&first))
                    .is_pending()
            );
            on(Event::Wake, move || {
                assert_eq!(channel.len(), 2);
                assert!(channel.is_full());
                assert!(!channel.is_empty());
                assert_eq!(channel.try_peek(), Ok(1));
                assert_eq!(channel.try_send(3), Err(TrySendError::Full(3)));
            });
            assert!(
                channel
                    .poll_ready_to_send(&mut Context::from_waker(&second))
                    .is_pending()
            );
            assert!(ran(Event::Wake));
        }

        #[test]
        fn a_message_sent_by_the_wakers_clone_answers_the_same_poll() {
            let channel = channel::<u32, 1>();
            let (first, _) = waker();
            on(Event::Clone, move || channel.try_send(5).unwrap());
            assert_eq!(channel.poll_receive(&mut Context::from_waker(&first)), Poll::Ready(5));
        }

        #[test]
        fn room_made_by_the_wakers_clone_lets_the_same_poll_send() {
            let channel = channel::<u32, 1>();
            channel.try_send(1).unwrap();
            let (first, _) = waker();
            on(Event::Clone, move || assert_eq!(channel.try_receive(), Ok(1)));
            let mut send = pin!(channel.send(2));
            assert!(send.as_mut().poll(&mut Context::from_waker(&first)).is_ready());
            assert_eq!(channel.try_receive(), Ok(2));
        }

        #[test]
        fn a_wait_survives_a_call_from_the_wakers_clone() {
            let channel = channel::<u32, 1>();
            let (first, first_wakes) = waker();
            on(Event::Clone, move || assert!(channel.is_empty()));
            assert!(channel.poll_receive(&mut Context::from_waker(&first)).is_pending());
            channel.try_send(4).unwrap();
            assert_eq!(first_wakes.load(Ordering::SeqCst), 1);
        }

        #[test]
        fn a_sender_stays_a_waiter_when_a_receive_wakes_only_the_other() {
            let channel = channel::<u32, 1>();
            channel.try_send(1).unwrap();
            let (first, first_wakes) = waker();
            let (second, second_wakes) = waker();
            let mut a = pin!(channel.send(2));
            assert!(a.as_mut().poll(&mut Context::from_waker(&first)).is_pending());
            let mut b = pin!(channel.send(3));
            assert!(b.as_mut().poll(&mut Context::from_waker(&second)).is_pending());
            assert_eq!(first_wakes.load(Ordering::SeqCst), 1, "displaced, to wait again");
            assert_eq!(channel.try_receive(), Ok(1));
            assert_eq!(second_wakes.load(Ordering::SeqCst), 1);
            assert!(b.as_mut().poll(&mut Context::from_waker(&second)).is_ready());
            assert!(a.as_mut().poll(&mut Context::from_waker(&first)).is_pending());
        }

        struct OnDrop(Option<Box<dyn FnOnce()>>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                if let Some(hook) = self.0.take() {
                    hook();
                }
            }
        }

        #[test]
        fn a_message_dropped_by_clear_may_call_the_channel() {
            let channel = channel::<OnDrop, 2>();
            let seen = Rc::new(RefCell::new(None));
            let told = seen.clone();
            let first = OnDrop(Some(Box::new(move || {
                *told.borrow_mut() = Some(channel.len());
            })));
            assert!(channel.try_send(first).is_ok());
            assert!(channel.try_send(OnDrop(None)).is_ok());
            let (sender, sender_wakes) = waker();
            assert!(
                channel
                    .poll_ready_to_send(&mut Context::from_waker(&sender))
                    .is_pending()
            );
            channel.clear();
            assert_eq!(*seen.borrow(), Some(0), "the portion left in one visit");
            assert!(channel.is_empty());
            assert_eq!(sender_wakes.load(Ordering::SeqCst), 1, "room reaches the sender");
        }

        #[test]
        fn clear_leaves_what_a_dropped_message_sent() {
            let channel = channel::<OnDrop, 3>();
            let seen = Rc::new(RefCell::new(None));
            let told = seen.clone();
            let first = OnDrop(Some(Box::new(move || {
                // Its neighbour already left with it; a new message stays.
                *told.borrow_mut() = Some(channel.len());
                drop(channel.try_receive());
                assert!(channel.try_send(OnDrop(None)).is_ok());
            })));
            assert!(channel.try_send(first).is_ok());
            assert!(channel.try_send(OnDrop(None)).is_ok());
            channel.clear();
            assert_eq!(*seen.borrow(), Some(0), "the portion left in one visit");
            assert_eq!(channel.len(), 1, "the message sent meanwhile stays");
        }

        #[test]
        fn a_channel_keeps_order_across_the_wrap() {
            let channel = channel::<u32, 3>();
            for round in 0..10 {
                channel.try_send(round).unwrap();
                channel.try_send(round + 100).unwrap();
                assert_eq!(channel.try_peek(), Ok(round));
                assert_eq!(channel.len(), 2);
                assert_eq!(channel.try_receive(), Ok(round));
                assert_eq!(channel.try_receive(), Ok(round + 100));
            }
            assert_eq!(channel.try_receive(), Err(super::TryReceiveError::Empty));
        }

        #[test]
        fn a_channel_without_room_refuses_and_returns_the_message() {
            let channel = channel::<u32, 0>();
            assert_eq!(channel.try_send(1), Err(TrySendError::Full(1)));
            assert_eq!(channel.try_receive(), Err(super::TryReceiveError::Empty));
        }
    }
}
