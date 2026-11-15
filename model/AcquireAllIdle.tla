------------------------- MODULE AcquireAllIdle -------------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Clients, Capacity, Bug
ASSUME /\ Capacity > 0 /\ IsFiniteSet(Clients) /\ Clients # {}
       /\ Bug \in {"none", "skip-generation", "reverse-merge", "skip-low-bit"}

\* A fixed, initially idle pool isolates batch selection from construction
\* and destruction. Pool.tla separately checks the lifecycle of batch owners.
Resources == 1..Capacity
VARIABLES stack, order, older, reserved, owned, batchOwned, permits,
          phase, requested, bit, taken
vars == <<stack, order, older, reserved, owned, batchOwned, permits,
          phase, requested, bit, taken>>

Elems(s) == {s[i] : i \in DOMAIN s}
Reverse(s) == [i \in 1..Len(s) |-> s[Len(s) - i + 1]]
DropLast(s) == SubSeq(s, 1, Len(s) - 1)
Min(a, b) == IF a < b THEN a ELSE b

\* Concrete GenStack representation. When shared, old and new represent
\* the same underlying stack; writes must update both copies in the model.
PopOrder(s) == IF s.shared THEN Reverse(s.old) ELSE Reverse(s.old) \o Reverse(s.new)
Push(s, r) == IF s.shared
             THEN [s EXCEPT !.old = Append(@, r), !.new = Append(@, r)]
             ELSE [s EXCEPT !.new = Append(@, r)]
Pop(s) ==
    IF s.shared THEN [s EXCEPT !.old = DropLast(@), !.new = DropLast(@)]
    ELSE IF Len(s.old) = 0
         THEN [old |-> DropLast(s.new), new |-> DropLast(s.new), shared |-> TRUE]
         ELSE [s EXCEPT !.old = DropLast(@)]
RECURSIVE PopMany(_, _)
PopMany(s, n) == IF n = 0 THEN s ELSE PopMany(Pop(s), n - 1)
NextGen(s) ==
    IF Bug = "skip-generation" THEN s
    ELSE IF s.shared THEN [s EXCEPT !.new = <<>>, !.shared = FALSE]
    ELSE [old |-> IF Bug = "reverse-merge" THEN s.old \o s.new ELSE s.new \o s.old,
          new |-> <<>>, shared |-> FALSE]

\* Independent specification: a sequence in pop order, with a prefix of
\* older-generation entries. Push inserts immediately after that prefix;
\* a generation boundary protects every remaining entry from later pushes.
RefPush(r) == SubSeq(order, 1, older) \o <<r>> \o SubSeq(order, older + 1, Len(order))
Owners == {c \in Clients : owned[c] # 0}
ClientResources == {owned[c] : c \in Owners}

Init ==
    /\ stack = [old |-> [i \in Resources |-> i],
                 new |-> [i \in Resources |-> i], shared |-> TRUE]
    /\ order = Reverse(stack.old) /\ older = 0
    /\ reserved = {} /\ owned = [c \in Clients |-> 0] /\ batchOwned = {}
    /\ permits = 0 /\ phase = "ready" /\ requested = 0 /\ bit = 0 /\ taken = 0

\* A caller can reserve a permit even while AcquireAllIdle holds p.mux.
\* Blocking semaphore requests in Puddle all have weight one. With the
\* pinned semaphore dependency, queued waiters imply zero available permits;
\* a granted reservation is not refunded on cancellation before p.mux.
Reserve(c) ==
    /\ c \notin reserved /\ owned[c] = 0 /\ permits < Capacity
    /\ reserved' = reserved \cup {c} /\ permits' = permits + 1
    /\ UNCHANGED <<stack, order, older, owned, batchOwned, phase, requested, bit, taken>>

Claim(c) ==
    /\ phase = "ready" /\ c \in reserved /\ Len(order) > 0
    /\ owned' = [owned EXCEPT ![c] = Head(PopOrder(stack))]
    /\ reserved' = reserved \ {c}
    /\ stack' = Pop(stack) /\ order' = Tail(order) /\ older' = older - Min(older, 1)
    /\ UNCHANGED <<batchOwned, permits, phase, requested, bit, taken>>

\* A concurrent CreateResource can reserve a permit and then reject the
\* full pool without consuming an idle resource. This is not a Claim.
RejectCreate(c) ==
    /\ phase = "ready" /\ c \in reserved
    /\ reserved' = reserved \ {c} /\ permits' = permits - 1
    /\ UNCHANGED <<stack, order, older, owned, batchOwned, phase, requested, bit, taken>>

Release(c) ==
    /\ phase = "ready" /\ owned[c] # 0
    /\ stack' = Push(stack, owned[c]) /\ order' = RefPush(owned[c])
    /\ owned' = [owned EXCEPT ![c] = 0] /\ permits' = permits - 1
    /\ UNCHANGED <<older, reserved, batchOwned, phase, requested, bit, taken>>

ReleaseBatch(r) ==
    /\ phase = "ready" /\ r \in batchOwned
    /\ stack' = Push(stack, r) /\ order' = RefPush(r)
    /\ batchOwned' = batchOwned \ {r} /\ permits' = permits - 1
    /\ UNCHANGED <<older, reserved, owned, phase, requested, bit, taken>>

BeginBatch ==
    /\ phase = "ready" /\ batchOwned = {} /\ Len(order) > 0
    /\ phase' = "fullTry" /\ requested' = Len(order)
    /\ bit' = 2 ^ (CHOOSE k \in 0..Capacity : 2^k <= Len(order) /\ Len(order) < 2^(k+1))
    /\ UNCHANGED <<stack, order, older, reserved, owned, batchOwned, permits, taken>>

\* Each semaphore TryAcquire is a separate step. Releases and claims are
\* excluded by p.mux, but Reserve can run between any two attempts.
TryFull ==
    /\ phase = "fullTry"
    /\ IF Capacity - permits >= requested
       THEN /\ taken' = requested /\ permits' = permits + requested /\ phase' = "commit"
       ELSE /\ UNCHANGED <<taken, permits>> /\ phase' = "bits"
    /\ UNCHANGED <<stack, order, older, reserved, owned, batchOwned, requested, bit>>

TryBit ==
    /\ phase = "bits" /\ bit > 0
    /\ IF Capacity - permits >= bit /\ ~(Bug = "skip-low-bit" /\ bit = 1)
       THEN /\ taken' = taken + bit /\ permits' = permits + bit
       ELSE UNCHANGED <<taken, permits>>
    /\ bit' = bit \div 2
    /\ phase' = IF bit = 1 THEN "commit" ELSE "bits"
    /\ UNCHANGED <<stack, order, older, reserved, owned, batchOwned, requested>>

CommitBatch ==
    /\ phase = "commit"
    /\ batchOwned' = Elems(SubSeq(PopOrder(stack), 1, taken))
    /\ stack' = NextGen(PopMany(stack, taken))
    /\ order' = SubSeq(order, taken + 1, Len(order)) /\ older' = Len(order')
    /\ phase' = "ready" /\ requested' = 0 /\ bit' = 0 /\ taken' = 0
    /\ UNCHANGED <<reserved, owned, permits>>

Advance == TryFull \/ TryBit \/ CommitBatch
Next == (\E c \in Clients : Reserve(c) \/ Claim(c) \/ RejectCreate(c) \/ Release(c))
        \/ (\E r \in Resources : ReleaseBatch(r)) \/ BeginBatch \/ Advance
Spec == Init /\ [][Next]_vars /\ WF_vars(Advance)

TypeOK ==
    /\ stack \in [old : Seq(Resources), new : Seq(Resources), shared : BOOLEAN]
    /\ order \in Seq(Resources) /\ older \in 0..Len(order)
    /\ reserved \subseteq Clients /\ owned \in [Clients -> Resources \cup {0}]
    /\ batchOwned \subseteq Resources /\ permits \in 0..Capacity
    /\ phase \in {"ready", "fullTry", "bits", "commit"}
    /\ requested \in 0..Capacity /\ taken \in 0..Capacity /\ bit \in 0..Capacity
    /\ stack.shared => stack.old = stack.new
StackRefinement == PopOrder(stack) = order
ExclusiveOwnership ==
    /\ Cardinality(Elems(order)) = Len(order)
    /\ Cardinality(ClientResources) = Cardinality(Owners)
    /\ reserved \intersect Owners = {}
    /\ Elems(order) \intersect ClientResources = {}
    /\ Elems(order) \intersect batchOwned = {}
    /\ ClientResources \intersect batchOwned = {}
    /\ Elems(order) \cup ClientResources \cup batchOwned = Resources
PermitAccounting == permits = Cardinality(reserved) + Cardinality(Owners)
                              + Cardinality(batchOwned) + taken
BatchBound == taken <= Len(order)
\* With no releases while p.mux is held, the loop must take the requested
\* amount or exhaust available permits. No lower bit may be skipped.
BatchCount == phase = "commit" => taken = requested \/ permits = Capacity
BatchCompletes == phase # "ready" ~> phase = "ready"

=============================================================================
