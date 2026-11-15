# Bounded lifecycle verification

[Pool.tla](Pool.tla) models the lifecycle contract in
[LIFECYCLE.md](../LIFECYCLE.md). [AcquireAllIdle.tla](AcquireAllIdle.tla) checks
incremental batch acquisition and generational ordering. These are handwritten
TLA+ models, checked by
[TLC](https://github.com/tlaplus/tlaplus), and is not a source-level proof of Go.

## Run

Install Java 11 or newer, `curl`, and `sha256sum`, then run from the repository:

```sh
sh model/check.sh
```

The script downloads TLC v1.7.4, verifies its SHA-256, and keeps all downloads,
state graphs, and generated traces in a temporary directory. Java 17 is used in
CI. It checks lifecycle capacities one and two, capacity two with batch owners,
and the focused batch model at capacities three and four. It then requires five
intentional mutations to fail the expected invariants. The GitHub Actions model
job runs this independently of the Go version matrix.

For offline runs, supply an absolute path to the same pinned jar and optionally
the Java executable:

```sh
TLA2TOOLS_JAR=/absolute/path/to/tla2tools.jar \
TLC_JAVA=/absolute/path/to/java sh model/check.sh
```

To inspect a complete counterexample, copy `Pool.tla` and `Pool.cfg` to a scratch
directory, change `Bug` in the copied config to one of the mutation names below,
and run:

```sh
java -XX:+UseParallelGC -Xmx1g -jar /absolute/path/to/tla2tools.jar \
  -workers 1 -config Pool.cfg Pool.tla
```

## Bounds and abstraction

The lifecycle configurations have two clients and two distinct resource
identities. Clients may repeatedly acquire, release, create, cancel, or hijack;
resources may cycle between idle and acquired indefinitely. An identity is
allocated at most once, including failed constructions. Thus the model explores
at most two construction attempts per execution, not unlimited resource churn.
Increase `Resources` to explore more births, at the cost of a larger state graph.
An open pool can exhaust this artificial identity budget; the model makes no
claim about open-pool acquisition liveness in that situation.

Reset generations are represented by a monotonic set of stale identities.
Because identities are never recycled, this preserves whether a resource was
invalidated by any number of resets without an unbounded generation counter.
It does not model machine-integer overflow.

Mutex-protected bookkeeping is represented by atomic transitions. Semaphore
reservation happens separately, and callbacks and result handoff can interleave
with reset, close, and other operations. Constructors nondeterministically
succeed or fail. An acquire can abandon construction at any point before the
handoff. Destructors finish in independent transitions.

The lifecycle model does not represent semaphore waiter ordering, context values,
wall-clock deadlines, error-channel delivery after failed construction, mutex
implementation, Go memory ordering, scheduling fairness guarantees of the
runtime, statistics counters, or resource payloads. With `BatchEnabled = TRUE`,
`AcquireAllIdle` may give a client any nonempty subset of idle resources that
fits available permits. This deliberately overapproximates batch selection:
it checks ownership, partial relinquishment, and reset/close behavior without
assuming a particular idle ordering. Empty batch results are stuttering steps.
There is no proof that the Go implementation refines the model, nor an
unbounded-size or general deadlock-freedom proof. `Reset` may stutter, so TLC's
deadlock check by itself is not evidence of progress.

## Properties and their implementation counterparts

| Property / action | Meaning | Go implementation |
| --- | --- | --- |
| `CapacityBound` | Listed resources never exceed capacity | `createNewResource`, admission checks |
| `PermitAccounting` | Held permits equal reservations plus permit-owning resources | `acquireSem`, all release paths |
| `CleanupAccounting` | Wait-group count matches outstanding lifecycle obligations | `destructWG.Add/Done` |
| `ExclusiveOwnership` | Each acquired resource has exactly one client owner | idle removal, construction handoff |
| `IdleValid` | Idle resources are current and the pool is open | background completion, release, reset, close |
| `DestructionValid` | Completed destruction belongs only to retiring/destroyed values | destructor completion and removal |
| `CloseSafe` | Close never returns with cleanup obligations remaining | `Close`, `destructResourceValue` |
| `CloseCompletes` | Once closed, Close eventually returns under the assumptions below | callback completion, relinquishing resources, wait group |
| `Reserve` then `Admit` | Permit reservation precedes taking the pool mutex | `Acquire`, `TryAcquire`, `CreateResource` |
| `ConstructionSucceeded` then `Handoff` | Successful construction is not yet caller ownership | `initResourceValue`'s unbuffered channel |
| `FinishDestructor` then `RemoveDestroyed` | Destroy's wait-group completion precedes pool removal | `destroyAcquiredResource` |
| `AcquireAllIdle`, set-valued `held` | A client may own and individually relinquish multiple resources | `AcquireAllIdle`, Resource methods |

Weak fairness is assumed for admitted operations, constructor completion,
handoff/abandoned return, client relinquishment, destructor completion, resource
removal, and finishing Close. These are explicit assumptions that callbacks
terminate, clients give up their handles, and enabled work runs. Close itself
need not be invoked; the temporal property is conditional on it being invoked.
The model does not promise starvation freedom for acquiring a semaphore permit.

## Focused batch model

`AcquireAllIdle.tla` starts with a full, idle pool of three or four resources,
two ordinary clients, and one batch caller. It models each semaphore attempt
separately, including the initial full-size attempt and descending powers of
two. Ordinary callers can reserve permits between attempts, but claiming and
releasing resources require the pool mutex. The batch caller can start another
batch after releasing its previous batch. There is no construction, reset,
close, or destruction here; those are covered by the lifecycle model.

The concrete stack is modeled as two sequences and an alias flag, matching
`GenStack`'s shared/old/new stacks, empty-old fallback, and generation merging.
An independent specification stores resources in pop order plus the length of
the older-generation prefix. `StackRefinement` requires the concrete pop order
to equal that specification after every action. This check is stronger than
assuming that every stack pop respects generation order.

Additional invariants require distinct resource ownership, conservation of
resources and permits, sufficient idle entries for every acquired batch
permit, and completion of the binary acquisition loop with either the requested
count or no free permits remaining. `BatchCompletes` requires a started batch
to finish under weak fairness for its own steps; ordinary callers need not
release resources to enable the nonblocking batch operation to finish.

The semaphore abstraction relies on all blocking requests in Puddle having
weight one: pending waiters cannot coexist with available permits after the
semaphore's atomic bookkeeping. In `golang.org/x/sync` v0.1.0, cancellation after
a permit grant leaves the grant intact, so refunds cannot increase free permits
while the batch holds the pool mutex. Revisit this abstraction if the semaphore
dependency or Puddle's acquisition/cancellation protocol changes. The focused
model includes a reserved `CreateResource` rejecting the full pool, so it does
not assume that every outstanding reservation consumes an idle resource.

These are complementary bounded checks. There is no mechanically proved
refinement between the two models, or between either model and the Go source.
They do not establish a guarantee for every pool size or all numbers of callers.

## Counterexamples and Go regressions

The runner checks that the invariants reject these mutations, not merely that
TLC exits unsuccessfully. Parser errors and unrelated failures do not qualify.

| Mutation | Violated invariant | Representative schedule | Go regression |
| --- | --- | --- | --- |
| `publish-invalid` | `IdleValid` | TryAcquire starts construction; Close or Reset runs; successful construction is published as idle | `TestPoolConstructionLifecycle` |
| `leak-failure-permit` | `PermitAccounting` | Acquire reserves a permit; construction fails; its resource and cleanup obligation disappear but its permit remains held | `TestPoolConstructionFailureReleasesWaiter` |
| `skip-generation` | `StackRefinement` | A partial batch leaves older idle entries; a batch resource is released and incorrectly moves ahead of them | `TestAcquireAllIdlePreservesGenerationsAcrossReservations`, stack fuzz oracle |
| `reverse-merge` | `StackRefinement` | A second partial batch merges nonempty old and new stacks in the wrong order | `TestAcquireAllIdlePreservesGenerationsAcrossReservations`, stack fuzz oracle |
| `skip-low-bit` | `BatchCount` | Two of three permits are reserved; the batch skips its one-permit attempt and leaves capacity unused | `TestAcquireSemAllMatchesAvailablePermits` |

The first mutation restores the lifecycle mistake fixed in Puddle 2.2.3 (and
also applies it to CreateResource). The second deliberately omits a semaphore
release. These are validation probes; they do not claim new defects in the
current implementation. The three batch mutations deliberately break the
generation boundary, generation merge, or semaphore loop. To inspect those
traces, use `AcquireAllIdle.tla` and `AcquireAllIdle.cfg` in the scratch-directory
instructions above instead of `Pool.tla` and `Pool.cfg`.

`TestPoolConstructionLifecycle` uses channels and `synctest.Wait` to replay the
invalid-construction schedule across all three constructor entry points, reset
and close, constructor success and failure, canceled acquires, delayed
destructors, and two concurrent Close callers. The failure test queues a second
acquire behind a controlled failing constructor and requires it to proceed.
Additional tests cover a successful acquire admitted before close and Destroy
retaining capacity until its destructor returns.

The batch ordering regression reserves permits directly, then executes real
`AcquireAllIdle` calls and the same idle admission helper used by regular
acquires. This controls the semaphore-to-mutex interval without timing or
production hooks. It is a controlled replay of that interval, not a complete
concurrently executing Acquire call. `TestPoolAcquireAllIdleLifecycle` checks
batch statistics and mixed release/destroy/hijack during reset or concurrent
close with delayed destructors. `FuzzGenStackMatchesGenerations` compares the
real stack to a queue of literal generation stacks, exercising arbitrary
push/pop/generation sequences. Its seeds run in ordinary CI; a longer run is:

```sh
go test -run '^$' -fuzz '^FuzzGenStackMatchesGenerations$' -fuzztime 30s ./internal/genstack
```

`TestStressLifecycle` supplements these schedules with eight concurrent clients,
capacities one/two/four, reset, cancellation, all acquire APIs, constructor
failures, delayed destructors, and concurrent shutdown. An independent ledger
checks exclusive ownership, no reuse after hijack/destruction, and exactly one
pool destructor call per successful non-hijacked value. Its seeded operation
streams do not make goroutine scheduling reproducible. `STRESS_TEST_DURATION`
controls the duration per capacity, as for the existing stress tests.

When changing lifecycle code, update the contract and corresponding model
transitions, run `sh model/check.sh` and `go test -race ./...`, and turn any new
counterexample into a controlled Go regression. Passing both layers provides
bounded algorithm checks and implementation evidence; it is not an exhaustive
verification of every execution of the library.
