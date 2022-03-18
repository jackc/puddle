package puddle

import "github.com/jackc/puddle/puddleg"

var (
	// ErrClosedPool occurs on an attempt to acquire a connection from a closed pool
	// or a pool that is closed while the acquire is waiting.
	ErrClosedPool = puddleg.ErrClosedPool

	// ErrNotAvailable occurs on an attempt to acquire a resource from a pool
	// that is at maximum capacity and has no available resources.
	ErrNotAvailable = puddleg.ErrNotAvailable
)

type (
	// Constructor is a function called by the pool to construct a resource.
	Constructor = puddleg.Constructor[any]

	// Destructor is a function called by the pool to destroy a resource.
	Destructor = puddleg.Destructor[any]

	// Resource is the resource handle returned by acquiring from the pool.
	Resource = puddleg.Resource[any]

	// Pool is a concurrency-safe resource pool.
	Pool = puddleg.Pool[any]

	// Stat is a snapshot of Pool statistics.
	Stat = puddleg.Stat
)

// NewPool creates a new pool. Panics if maxSize is less than 1.
func NewPool(constructor Constructor, destructor Destructor, maxSize int32) *Pool {
	return puddleg.NewPool(constructor, destructor, maxSize)
}
