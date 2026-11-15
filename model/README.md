# Bounded lifecycle verification

[Pool.tla](Pool.tla) models the lifecycle contract in
[LIFECYCLE.md](../LIFECYCLE.md). It is handwritten TLA+, checked by
[TLC](https://github.com/tlaplus/tlaplus), and is not a source-level proof of Go.

## Run

Install Java 11 or newer, `curl`, and `sha256sum`, then run from the repository:

```sh
sh model/check.sh
```

The script downloads TLC v1.7.4, verifies its SHA-256, and keeps all downloads,
state graphs, and generated traces in a temporary directory. Java 17 is used in
CI. It checks capacities one and two and then requires two intentional model
mutations to fail the expected invariants. The GitHub Actions lifecycle-model
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

Both checked configurations have two clients and two distinct resource
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

The model does not represent semaphore waiter ordering, context values,
wall-clock deadlines, error-channel delivery after failed construction, mutex
implementation, Go memory ordering, scheduling fairness guarantees of the
runtime, statistics counters, or resource payloads. `AcquireAllIdle` and its
generational ordering are covered by implementation tests, not this model.
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

Weak fairness is assumed for admitted operations, constructor completion,
handoff/abandoned return, client relinquishment, destructor completion, resource
removal, and finishing Close. These are explicit assumptions that callbacks
terminate, clients give up their handles, and enabled work runs. Close itself
need not be invoked; the temporal property is conditional on it being invoked.
The model does not promise starvation freedom for acquiring a semaphore permit.

## Counterexamples and Go regressions

The runner checks that the invariants reject these mutations, not merely that
TLC exits unsuccessfully. Parser errors and unrelated failures do not qualify.

| Mutation | Violated invariant | Representative schedule | Go regression |
| --- | --- | --- | --- |
| `publish-invalid` | `IdleValid` | TryAcquire starts construction; Close or Reset runs; successful construction is published as idle | `TestPoolConstructionLifecycle` |
| `leak-failure-permit` | `PermitAccounting` | Acquire reserves a permit; construction fails; its resource and cleanup obligation disappear but its permit remains held | `TestPoolConstructionFailureReleasesWaiter` |

The first mutation restores the lifecycle mistake fixed in Puddle 2.2.3 (and
also applies it to CreateResource). The second deliberately omits a semaphore
release. These are validation probes; they do not claim new defects in the
current implementation.

`TestPoolConstructionLifecycle` uses channels and `synctest.Wait` to replay the
invalid-construction schedule across all three constructor entry points, reset
and close, constructor success and failure, canceled acquires, delayed
destructors, and two concurrent Close callers. The failure test queues a second
acquire behind a controlled failing constructor and requires it to proceed.
Additional tests cover a successful acquire admitted before close and Destroy
retaining capacity until its destructor returns.

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
