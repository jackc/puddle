------------------------------ MODULE Pool ------------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Clients, Resources, Capacity, Bug
ASSUME /\ Capacity > 0
       /\ Clients # {} /\ IsFiniteSet(Clients)
       /\ Resources # {} /\ IsFiniteSet(Resources)
       /\ 0 \notin Resources
       /\ Bug \in {"none", "publish-invalid", "leak-failure-permit"}

\* A bounded lifecycle model, not a translation or proof of the Go source.
\* Mutex-protected bookkeeping is atomic. Reserving a semaphore permit,
\* running callbacks, delivering a constructor result, and finishing Close
\* are separate transitions. See README.md for bounds and omissions.
VARIABLES phase, mode, held, state, caller, origin, stale, abandoned,
          permits, obligations, destroyed, closed, closeReturned

vars == <<phase, mode, held, state, caller, origin, stale, abandoned,
          permits, obligations, destroyed, closed, closeReturned>>

States == {"unused", "constructing", "handoff", "idle", "acquired",
           "destroying", "removing", "retiring", "dead", "failed", "hijacked"}
InState(states) == {r \in Resources : state[r] \in states}
Listed == InState({"constructing", "handoff", "idle", "acquired",
                   "destroying", "removing"})
Idle == InState({"idle"})
TokenResources == InState({"constructing", "handoff", "acquired",
                           "destroying", "removing"})
Outstanding == InState({"constructing", "handoff", "idle", "acquired",
                        "destroying", "retiring"})
Reservations == {c \in Clients : phase[c] = "reserved"}
Invalid(r) == closed \/ r \in stale

Init ==
    /\ phase = [c \in Clients |-> "ready"]
    /\ mode = [c \in Clients |-> "acquire"]
    /\ held = [c \in Clients |-> 0]
    /\ state = [r \in Resources |-> "unused"]
    /\ caller = [r \in Resources |-> CHOOSE c \in Clients : TRUE]
    /\ origin = [r \in Resources |-> "acquire"]
    /\ stale = {}
    /\ abandoned = {}
    /\ permits = 0
    /\ obligations = 0
    /\ destroyed = {}
    /\ closed = FALSE
    /\ closeReturned = FALSE

Begin(c, kind) ==
    /\ phase[c] = "ready"
    /\ phase' = [phase EXCEPT ![c] = "waitingPermit"]
    /\ mode' = [mode EXCEPT ![c] = kind]
    /\ UNCHANGED <<held, state, caller, origin, stale, abandoned, permits,
                    obligations, destroyed, closed, closeReturned>>

\* acquireSem is acquired BEFORE p.mux; do not merge with Admit.
Reserve(c) ==
    /\ phase[c] = "waitingPermit"
    /\ permits < Capacity
    /\ permits' = permits + 1
    /\ phase' = [phase EXCEPT ![c] = "reserved"]
    /\ UNCHANGED <<mode, held, state, caller, origin, stale, abandoned,
                    obligations, destroyed, closed, closeReturned>>

LeaveWait(c) ==
    /\ phase[c] = "waitingPermit"
    \* Acquire cancellation or an unsuccessful nonblocking call.
    /\ mode[c] = "acquire" \/ permits = Capacity
    /\ phase' = [phase EXCEPT ![c] = "ready"]
    /\ UNCHANGED <<mode, held, state, caller, origin, stale, abandoned,
                    permits, obligations, destroyed, closed, closeReturned>>

Reject(c) ==
    /\ phase[c] = "reserved"
    /\ closed \/ (mode[c] = "create" /\ Cardinality(Listed) >= Capacity)
    /\ phase' = [phase EXCEPT ![c] = "ready"]
    /\ permits' = permits - 1
    /\ UNCHANGED <<mode, held, state, caller, origin, stale, abandoned,
                    obligations, destroyed, closed, closeReturned>>

TakeIdle(c, r) ==
    /\ phase[c] = "reserved" /\ ~closed
    /\ mode[c] # "create" /\ r \in Idle
    /\ phase' = [phase EXCEPT ![c] = "holding"]
    /\ held' = [held EXCEPT ![c] = r]
    /\ state' = [state EXCEPT ![r] = "acquired"]
    /\ UNCHANGED <<mode, caller, origin, stale, abandoned, permits,
                    obligations, destroyed, closed, closeReturned>>

StartConstruction(c, r) ==
    /\ phase[c] = "reserved" /\ ~closed /\ state[r] = "unused"
    /\ IF mode[c] = "create" THEN Cardinality(Listed) < Capacity ELSE Idle = {}
    \* Deliberately no capacity guard for Acquire/TryAcquire: permit
    \* accounting must imply the unreachable overflow panic is impossible.
    /\ state' = [state EXCEPT ![r] = "constructing"]
    /\ caller' = [caller EXCEPT ![r] = c]
    /\ origin' = [origin EXCEPT ![r] = mode[c]]
    /\ phase' = [phase EXCEPT ![c] = IF mode[c] = "try" THEN "ready" ELSE "constructing"]
    /\ obligations' = obligations + 1
    /\ UNCHANGED <<mode, held, stale, abandoned, permits, destroyed, closed, closeReturned>>

Admit(c) == Reject(c) \/ (\E r \in Resources : TakeIdle(c, r) \/ StartConstruction(c, r))

\* Observing cancellation returns the caller immediately. The constructor
\* and its permit/WaitGroup obligation remain alive independently.
CancelConstruction(r) ==
    /\ state[r] \in {"constructing", "handoff"}
    /\ origin[r] = "acquire" /\ r \notin abandoned
    /\ abandoned' = abandoned \cup {r}
    /\ phase' = [phase EXCEPT ![caller[r]] = "ready"]
    /\ UNCHANGED <<mode, held, state, caller, origin, stale, permits,
                    obligations, destroyed, closed, closeReturned>>

ConstructionFailed(r) ==
    /\ state[r] = "constructing"
    /\ state' = [state EXCEPT ![r] = "failed"]
    /\ permits' = IF Bug = "leak-failure-permit" THEN permits ELSE permits - 1
    /\ obligations' = obligations - 1
    /\ phase' = IF origin[r] # "try" /\ r \notin abandoned
                 THEN [phase EXCEPT ![caller[r]] = "ready"] ELSE phase
    /\ UNCHANGED <<mode, held, caller, origin, stale, abandoned,
                    destroyed, closed, closeReturned>>

ConstructionSucceeded(r) ==
    /\ state[r] = "constructing"
    /\ IF origin[r] = "acquire"
       THEN /\ state' = [state EXCEPT ![r] = "handoff"]
            /\ UNCHANGED <<phase, permits>>
       ELSE /\ state' = [state EXCEPT ![r] =
                    IF Invalid(r) /\ Bug # "publish-invalid" THEN "retiring" ELSE "idle"]
            /\ permits' = permits - 1
            /\ phase' = IF origin[r] = "create"
                         THEN [phase EXCEPT ![caller[r]] = "ready"] ELSE phase
    /\ UNCHANGED <<mode, held, caller, origin, stale, abandoned,
                    obligations, destroyed, closed, closeReturned>>

CompleteConstruction(r) == ConstructionFailed(r) \/ ConstructionSucceeded(r)

\* The unbuffered channel transfers ownership, even if Close/Reset ran
\* after admission. Cancellation competes with this transition.
Deliver(r) ==
    /\ state[r] = "handoff" /\ r \notin abandoned
    /\ phase' = [phase EXCEPT ![caller[r]] = "holding"]
    /\ held' = [held EXCEPT ![caller[r]] = r]
    /\ state' = [state EXCEPT ![r] = "acquired"]
    /\ UNCHANGED <<mode, caller, origin, stale, abandoned, permits,
                    obligations, destroyed, closed, closeReturned>>

ReturnAbandoned(r) ==
    /\ state[r] = "handoff" /\ r \in abandoned
    /\ state' = [state EXCEPT ![r] = IF Invalid(r) THEN "retiring" ELSE "idle"]
    /\ permits' = permits - 1
    /\ UNCHANGED <<phase, mode, held, caller, origin, stale, abandoned,
                    obligations, destroyed, closed, closeReturned>>

Handoff(r) == Deliver(r) \/ ReturnAbandoned(r)

Release(c) ==
    /\ phase[c] = "holding"
    /\ state' = [state EXCEPT ![held[c]] = IF Invalid(held[c]) THEN "retiring" ELSE "idle"]
    /\ phase' = [phase EXCEPT ![c] = "ready"]
    /\ held' = [held EXCEPT ![c] = 0]
    /\ permits' = permits - 1
    /\ UNCHANGED <<mode, caller, origin, stale, abandoned,
                    obligations, destroyed, closed, closeReturned>>

Destroy(c) ==
    /\ phase[c] = "holding"
    /\ state' = [state EXCEPT ![held[c]] = "destroying"]
    /\ phase' = [phase EXCEPT ![c] = "ready"]
    /\ held' = [held EXCEPT ![c] = 0]
    \* Destroy retains both membership and its permit during the callback.
    /\ UNCHANGED <<mode, caller, origin, stale, abandoned, permits,
                    obligations, destroyed, closed, closeReturned>>

Hijack(c) ==
    /\ phase[c] = "holding"
    /\ state' = [state EXCEPT ![held[c]] = "hijacked"]
    /\ phase' = [phase EXCEPT ![c] = "ready"]
    /\ held' = [held EXCEPT ![c] = 0]
    /\ permits' = permits - 1
    /\ obligations' = obligations - 1
    /\ UNCHANGED <<mode, caller, origin, stale, abandoned, destroyed, closed, closeReturned>>

Relinquish(c) == Release(c) \/ Destroy(c) \/ Hijack(c)

FinishDestructor(r) ==
    /\ state[r] \in {"destroying", "retiring"}
    /\ state' = [state EXCEPT ![r] = IF @ = "destroying" THEN "removing" ELSE "dead"]
    /\ destroyed' = destroyed \cup {r}
    /\ obligations' = obligations - 1
    /\ UNCHANGED <<phase, mode, held, caller, origin, stale, abandoned,
                    permits, closed, closeReturned>>

\* destroyAcquiredResource calls destructWG.Done before locking p.mux to
\* remove the resource and release its permit. Close may return in between.
RemoveDestroyed(r) ==
    /\ state[r] = "removing"
    /\ state' = [state EXCEPT ![r] = "dead"]
    /\ permits' = permits - 1
    /\ UNCHANGED <<phase, mode, held, caller, origin, stale, abandoned,
                    obligations, destroyed, closed, closeReturned>>

Reset ==
    /\ stale' = stale \cup Listed
    /\ state' = [r \in Resources |-> IF r \in Idle THEN "retiring" ELSE state[r]]
    /\ UNCHANGED <<phase, mode, held, caller, origin, abandoned, permits,
                    obligations, destroyed, closed, closeReturned>>

Close ==
    /\ ~closed
    /\ closed' = TRUE
    /\ state' = [r \in Resources |-> IF r \in Idle THEN "retiring" ELSE state[r]]
    /\ UNCHANGED <<phase, mode, held, caller, origin, stale, abandoned,
                    permits, obligations, destroyed, closeReturned>>

FinishClose ==
    /\ closed /\ ~closeReturned /\ obligations = 0
    /\ closeReturned' = TRUE
    /\ UNCHANGED <<phase, mode, held, state, caller, origin, stale,
                    abandoned, permits, obligations, destroyed, closed>>

Next ==
    \/ \E c \in Clients :
          (\E kind \in {"acquire", "try", "create"} : Begin(c, kind))
          \/ Reserve(c) \/ LeaveWait(c) \/ Admit(c) \/ Relinquish(c)
    \/ \E r \in Resources : CancelConstruction(r) \/ CompleteConstruction(r)
          \/ Handoff(r) \/ FinishDestructor(r) \/ RemoveDestroyed(r)
    \/ Reset \/ Close \/ FinishClose

\* Fairness expresses terminating callbacks, clients relinquishing handles,
\* and runnable cleanup/admission steps eventually executing. It does not
\* promise that a caller wins contention or that Close is ever invoked.
Spec == Init /\ [][Next]_vars
        /\ (\A c \in Clients : WF_vars(Admit(c)) /\ WF_vars(Relinquish(c)))
        /\ (\A r \in Resources : WF_vars(CompleteConstruction(r))
              /\ WF_vars(Handoff(r)) /\ WF_vars(FinishDestructor(r))
              /\ WF_vars(RemoveDestroyed(r)))
        /\ WF_vars(FinishClose)

TypeOK ==
    /\ phase \in [Clients -> {"ready", "waitingPermit", "reserved", "constructing", "holding"}]
    /\ mode \in [Clients -> {"acquire", "try", "create"}]
    /\ held \in [Clients -> Resources \cup {0}]
    /\ state \in [Resources -> States]
    /\ caller \in [Resources -> Clients]
    /\ origin \in [Resources -> {"acquire", "try", "create"}]
    /\ stale \subseteq Resources /\ abandoned \subseteq Resources
    /\ destroyed \subseteq Resources
    /\ permits \in 0..Capacity /\ obligations \in 0..Cardinality(Resources)
    /\ closed \in BOOLEAN /\ closeReturned \in BOOLEAN

CapacityBound == Cardinality(Listed) <= Capacity
PermitAccounting == permits = Cardinality(Reservations) + Cardinality(TokenResources)
CleanupAccounting == obligations = Cardinality(Outstanding)
ExclusiveOwnership ==
    /\ \A c \in Clients : (phase[c] = "holding") <=> (held[c] # 0)
    /\ \A c \in Clients : held[c] # 0 => state[held[c]] = "acquired"
    /\ \A c, d \in Clients : (held[c] # 0 /\ held[c] = held[d]) => c = d
    /\ \A r \in Resources : state[r] = "acquired" => \E c \in Clients : held[c] = r
IdleValid == /\ Idle \intersect stale = {}
             /\ closed => Idle = {}
DestructionValid == destroyed = InState({"removing", "dead"})
CloseSafe == closeReturned => obligations = 0 /\ Outstanding = {}
CloseCompletes == closed ~> closeReturned

=============================================================================
