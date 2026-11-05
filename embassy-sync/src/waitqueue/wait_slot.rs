use core::cell::Cell;
use core::fmt;
use core::task::Waker;

/// The one waiter of a state kept inside a blocking mutex.
///
/// A waker's `clone`, `wake` and `drop` are foreign code, which may call the
/// same primitive again. None of them runs here: what must run one is handed
/// back to the caller, to run after it left the state.
pub(crate) struct WaitSlot(Cell<Option<Waker>>);

impl WaitSlot {
    pub(crate) const fn new() -> Self {
        Self(Cell::new(None))
    }

    /// Whether the waiter here is the one `waker` wakes. `will_wake` only
    /// compares, and the slot is whole again before this returns.
    pub(crate) fn holds(&self, waker: &Waker) -> bool {
        let held = self.0.take();
        let same = held.as_ref().is_some_and(|held| held.will_wake(waker));
        self.0.set(held);
        same
    }

    /// Make `waker` the waiter and return the one it displaced, which the
    /// caller wakes so that its task can wait again.
    pub(crate) fn replace(&self, waker: Waker) -> Option<Waker> {
        self.0.replace(Some(waker))
    }

    /// The waiter, for the caller to wake.
    pub(crate) fn take(&self) -> Option<Waker> {
        self.0.take()
    }
}

impl fmt::Debug for WaitSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WaitSlot")
    }
}

/// Wake a waiter taken out of a state. Kept out of line: inlined into the
/// end of a critical section, the wake loses its debug location, and the
/// image's stack analysis finds a waker call by it.
#[inline(never)]
pub(crate) fn wake(waiter: Option<Waker>) {
    if let Some(waiter) = waiter {
        waiter.wake();
    }
}

#[cfg(test)]
pub(crate) mod hooks {
    //! Wakers whose `clone`, `wake` and `drop` run a test's code, as the
    //! foreign code they are. A hook runs once, on the test's own thread.

    use core::sync::atomic::{AtomicUsize, Ordering};
    use core::task::{RawWaker, RawWakerVTable, Waker};
    use std::boxed::Box;
    use std::cell::RefCell;

    type Hook = Option<Box<dyn FnOnce()>>;

    #[derive(Clone, Copy)]
    pub(crate) enum Event {
        Clone = 0,
        Wake = 1,
        Drop = 2,
    }

    std::thread_local! {
        static HOOKS: RefCell<[Hook; 3]> = const { RefCell::new([None, None, None]) };
    }

    /// Run `hook` at the next `event` of any hooked waker on this thread.
    pub(crate) fn on(event: Event, hook: impl FnOnce() + 'static) {
        HOOKS.with(|hooks| hooks.borrow_mut()[event as usize] = Some(Box::new(hook)));
    }

    /// Whether the hook of `event` has run.
    pub(crate) fn ran(event: Event) -> bool {
        HOOKS.with(|hooks| hooks.borrow()[event as usize].is_none())
    }

    fn run(event: Event) {
        let hook = HOOKS.with(|hooks| hooks.borrow_mut()[event as usize].take());
        if let Some(hook) = hook {
            hook();
        }
    }

    static TABLE: RawWakerVTable = RawWakerVTable::new(
        |data| {
            run(Event::Clone);
            RawWaker::new(data, &TABLE)
        },
        |data| {
            woken(data);
            run(Event::Drop);
        },
        woken,
        |_| run(Event::Drop),
    );

    fn woken(data: *const ()) {
        // SAFETY: every waker of this table carries a leaked `AtomicUsize`.
        unsafe { &*data.cast::<AtomicUsize>() }.fetch_add(1, Ordering::SeqCst);
        run(Event::Wake);
    }

    /// A waker of its own identity, and the count of its wakes.
    pub(crate) fn waker() -> (Waker, &'static AtomicUsize) {
        let wakes: &'static AtomicUsize = Box::leak(Box::new(AtomicUsize::new(0)));
        let data = core::ptr::from_ref(wakes).cast();
        // SAFETY: the table's functions keep the contract for this data.
        (unsafe { Waker::new(data, &TABLE) }, wakes)
    }
}
