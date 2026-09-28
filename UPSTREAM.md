# Upstream tracking

This fork (`oer/main` at `github.com/ermacv/embassy`) follows
`github.com/embassy-rs/embassy` `main` by periodic merges into `oer/main`. It
is never rebased: every pinned revision stays reachable. `main` mirrors
upstream unchanged. Only `embassy-net` differs from upstream; every other
crate is upstream's.

## Last merge

- Upstream: `f9801df12012bf7cf7af80143b69e30e641a1841`.
- Previous merge base: `98d847be57f3ea022ce05fe9b95ab3639a1e0a93`.

To prepare the next merge, `git fetch upstream main` and review
`git log <last merged upstream>..upstream/main -- embassy-net`.

## embassy-net differences

`embassy-net` is upstream's multi-interface API over the fork of xarxa at
`github.com/ermacv/xarxa` (`oer/main`, see its `UPSTREAM.md`), with:

- `Stack::new(storage, random_seed, packet_allocator)`: every packet the
  stack creates comes from the given `PacketBufAllocator`. The stack claims
  the pool's unique `PacketPoolWaiter`; the runner registers it when a poll
  or send found the pool empty, so a freed buffer schedules the next poll.
- UDP and raw sends that find the pool empty wait on the socket's send waker,
  which xarxa wakes once a buffer is free. There is no busy yield.
- The runner polls with `xarxa::Stack::poll_bounded` and a `PollBudget`
  (default 32 ingress and 32 socket-egress frames per turn, set with
  `Runner::set_poll_budget`), and yields when the budget is exhausted.
- `StackStorage` holds the xarxa stack in its own slot, so construction
  builds the protocol state in place instead of moving it through locals.
