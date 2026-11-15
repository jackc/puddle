# Resource lifecycle contract

This document states the lifecycle guarantees used by Puddle's concurrency
tests and [bounded TLA+ model](model/README.md). It describes the current API,
including operations that overlap cancellation, reset, or shutdown. Changes to
these guarantees should update the implementation, tests, and model together.

## Ownership and capacity

- The pool's methods are safe to call concurrently. A checked-out `Resource`
  has one owner. The caller must arrange any transfer between goroutines and
  must stop using the handle after `Release`, `ReleaseUnused`, or `Destroy`.
  Calling those methods twice or concurrently on the same handle is outside
  the contract; status checks do not make such misuse safe.
- `Hijack` transfers cleanup responsibility to the caller. The pool must never
  acquire or destroy that value again. Its handle remains usable for accessing
  the value and its timestamps.
- `TotalResources` counts the entries in pool bookkeeping: constructing, idle,
  and acquired resources. It never exceeds `MaxSize`.
- A resource removed for asynchronous destruction can remain physically alive
  while the pool creates a replacement. `MaxSize` does not bound the number of
  external objects including values pending destruction or already hijacked.
- Semaphore permits account for construction, acquired resources, and callers
  that have reserved permission but have not yet claimed or created a resource.
  A permit reservation and the subsequent pool-mutex operation are separate
  steps. Idle resources do not retain permits.

The implementation has four resource status constants. The model distinguishes
additional phases that explain ownership of permits and cleanup obligations:

| Phase | In pool bookkeeping? | Holds a permit? | Pool owes cleanup? |
| --- | --- | --- | --- |
| Permit reserved, before admission | No resource yet | Yes | No |
| Constructing | Yes | Yes | Yes, unless construction fails |
| Successful construction awaiting Acquire handoff | Yes (acquired status) | Yes | Yes |
| Idle | Yes | No | Yes |
| Owned by caller | Yes | Yes | Yes |
| `Destroy` callback executing | Yes (acquired status) | Yes | Yes |
| `Destroy` callback finished, removal pending | Yes (acquired status) | Yes | No |
| Removed by reset/close/stale release, destructor pending | No | No | Yes |
| Hijacked | No | No | No |

For every construction attempt, the wait-group obligation ends exactly once:
on failure, on hijack, or after the destructor returns. Failed construction
does not call the destructor. Every successfully constructed value that remains
the pool's responsibility is destroyed exactly once when it is retired.

## Construction and cancellation

`Acquire` first rejects an already-canceled context. Otherwise it waits for a
permit, checks whether the pool is closed under the mutex, and claims an idle
resource or admits a new construction. This mutex check is the admission point.

When `Acquire` starts construction, the constructor receives the caller's
context values and the pool's cancellation signal. Canceling the caller's
context lets the caller stop waiting without canceling that construction. The
background goroutine still owns its permit and cleanup obligation. After
successful construction it either transfers the resource to the caller through
the unbuffered channel, or returns it to the pool if the caller abandoned it.
An abandoned resource is destroyed if the pool was closed or its generation
was reset; otherwise it becomes idle. Failure releases both the permit and
cleanup obligation, including when the original caller has stopped waiting.

Cancellation racing with availability or a successful handoff may produce
either success or a context error. Cancellation is not an unconditional promise
that a concurrently completed acquire will fail.

`TryAcquire` and `CreateResource` pass their supplied context directly to the
constructor. Closing the pool does not cancel those contexts. `TryAcquire`
returns `ErrNotAvailable` when it starts background construction; it does not
wait for or report that constructor's result. `CreateResource` waits for its
constructor and reports its error. Successful construction made invalid by
`Close` is destroyed and returns `ErrClosedPool` from `CreateResource`;
invalidation by `Reset` destroys it but returns nil. A constructor error takes
precedence over this post-construction close check.

## Reset

`Reset` increments the reset generation, removes all idle resources, and starts
their destruction. It leaves the pool open and does not wait for destructors.

Already acquired resources remain usable by their owners, but releasing them
destroys them instead of making them idle. Construction admitted before reset
is likewise from the old generation. `TryAcquire` and `CreateResource` must
destroy those successful results instead of publishing them as idle.

An `Acquire` admitted before reset may still deliver its resource afterward.
That resource is destroyed when returned. Reset is not a barrier that cancels
every operation already in progress. Repeated resets preserve invalidation:
an old resource must never become current again.

## Close

`Close` marks the pool closed under the mutex, cancels the pool's constructor
context, and removes idle resources for destruction. Every concurrent or
subsequent `Close` waits for the same outstanding cleanup obligations.

- No new resource construction is admitted after the closed flag is set.
- An `Acquire` admitted before that point may still succeed, even after the
  flag is set. Close waits for its resource to be relinquished and, unless
  hijacked, destroyed.
- A canceled acquire's background construction remains a cleanup obligation.
- Successful `TryAcquire`/`CreateResource` construction completing after closure
  must be destroyed, never added to the idle pool.
- Callers waiting for semaphore permits are not directly woken by closing the
  pool. They observe closure after obtaining a permit, or leave on context
  cancellation. `TryAcquire`/`CreateResource` can return `ErrNotAvailable`
  before reaching their closed check. `Acquire` can return a context error
  before reaching its closed check.
- `Close` returns only after the pool's destructor callbacks have returned or
  cleanup responsibility has been relinquished by hijacking.

`Destroy` calls the destructor and completes its wait-group obligation before
locking the pool to remove the resource and release its permit. Consequently,
`Close` returning does not guarantee that all internal removal bookkeeping, all
waiting API calls, or every goroutine has already finished. A concurrent `Stat`
can briefly still count such a resource as acquired. The model preserves this
ordering rather than strengthening the contract to require an empty list at
the instant Close returns.

## Progress assumptions and limits

Shutdown eventually completes if constructors and destructors eventually
return, clients eventually release/destroy/hijack every acquired resource, and
runnable goroutines eventually execute. Puddle cannot force a callback to
return or reclaim a resource retained indefinitely by a caller. Callbacks must
not synchronously wait for pool operations whose completion depends on that
same callback (for example, calling `Close` from a destructor).

Callback panics, invalid configurations other than the documented size check,
invalid contexts, handle misuse, and reset-counter integer overflow are outside
the verification scope. The model assumes Go synchronization primitives work
according to their contracts; the race-enabled implementation tests complement
that assumption.

`AcquireAllIdle` and the generational idle stack retain their documented
behavior and existing tests. They also participate in lifecycle stress tests,
but their batch selection and ordering algorithm is not in this first model.
Statistics durations and timestamp semantics are not modeled.
