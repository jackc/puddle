package puddle_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/puddle/v2"
	"github.com/stretchr/testify/require"
)

// These schedules replay the invalid-construction counterexamples described
// in model/README.md, including construction abandoned by a canceled Acquire.
func TestPoolConstructionLifecycle(t *testing.T) {
	for _, method := range []string{"Acquire", "TryAcquire", "CreateResource"} {
		for _, interrupt := range []string{"Reset", "Close"} {
			for _, fail := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/fail=%t", method, interrupt, fail), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						constructorCtx := make(chan context.Context, 1)
						finishConstruction := make(chan struct{})
						finishDestruction := make(chan struct{})
						destroyed := make(chan int, 3)
						var constructionOnce, destructionOnce sync.Once
						defer constructionOnce.Do(func() { close(finishConstruction) })
						defer destructionOnce.Do(func() { close(finishDestruction) })
						var calls Counter
						constructErr := errors.New("controlled construction failure")
						pool, err := puddle.NewPool(&puddle.Config[int]{
							MaxSize: 1,
							Constructor: func(ctx context.Context) (int, error) {
								id := calls.Next()
								if id == 1 {
									constructorCtx <- ctx
									<-finishConstruction
									if fail {
										return 0, constructErr
									}
								}
								return id, nil
							},
							Destructor: func(id int) {
								if id == 1 {
									<-finishDestruction
								}
								destroyed <- id
							},
						})
						require.NoError(t, err)
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						result := make(chan error, 1)
						var workers sync.WaitGroup
						switch method {
						case "Acquire":
							workers.Go(func() {
								res, err := pool.Acquire(ctx)
								if res != nil {
									res.Release()
								}
								result <- err
							})
						case "TryAcquire":
							res, err := pool.TryAcquire(ctx)
							require.Nil(t, res)
							require.ErrorIs(t, err, puddle.ErrNotAvailable)
						case "CreateResource":
							workers.Go(func() { result <- pool.CreateResource(ctx) })
						}
						constructionContext := <-constructorCtx
						if method == "Acquire" {
							cancel()
							synctest.Wait()
							require.ErrorIs(t, <-result, context.Canceled)
							require.NoError(t, constructionContext.Err())
						}

						closeResults := make(chan struct{}, 2)
						startClose := func() {
							for range 2 {
								workers.Go(func() {
									pool.Close()
									closeResults <- struct{}{}
								})
							}
						}
						if interrupt == "Reset" {
							pool.Reset()
						} else {
							startClose()
							synctest.Wait()
							require.Empty(t, closeResults, "Close must wait for the constructor")
							if method == "Acquire" {
								require.ErrorIs(t, constructionContext.Err(), context.Canceled)
							} else {
								require.NoError(t, constructionContext.Err(), "Close does not cancel these constructors")
							}
						}
						constructionOnce.Do(func() { close(finishConstruction) })
						synctest.Wait()
						require.Zero(t, pool.Stat().TotalResources(), "invalid construction must not become idle")
						require.Empty(t, destroyed)
						if method == "CreateResource" {
							err := <-result
							switch {
							case fail:
								require.ErrorIs(t, err, constructErr)
							case interrupt == "Close":
								require.ErrorIs(t, err, puddle.ErrClosedPool)
							default:
								require.NoError(t, err)
							}
						}

						var wantDestroyed []int
						if !fail {
							wantDestroyed = append(wantDestroyed, 1)
						}
						if interrupt == "Reset" {
							// Reset retirement releases capacity before the destructor
							// finishes. A fresh resource must be usable in the meantime.
							res, err := pool.Acquire(context.Background())
							require.NoError(t, err)
							require.Equal(t, 2, res.Value())
							res.Release()
							wantDestroyed = append(wantDestroyed, 2)
							startClose()
							synctest.Wait()
						}
						if !fail {
							require.Empty(t, closeResults, "all Close callers must wait for destruction")
						}
						destructionOnce.Do(func() { close(finishDestruction) })
						workers.Wait()
						synctest.Wait()
						require.Len(t, closeResults, 2)
						close(destroyed)
						var gotDestroyed []int
						for id := range destroyed {
							gotDestroyed = append(gotDestroyed, id)
						}
						require.ElementsMatch(t, wantDestroyed, gotDestroyed)
						require.Zero(t, pool.Stat().TotalResources())
					})
				})
			}
		}
	}
}

func TestPoolConstructionFailureReleasesWaiter(t *testing.T) {
	for _, method := range []string{"Acquire", "TryAcquire", "CreateResource"} {
		t.Run(method, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				finishConstruction := make(chan struct{})
				var finishOnce sync.Once
				defer finishOnce.Do(func() { close(finishConstruction) })
				var calls, destructions Counter
				constructErr := errors.New("controlled construction failure")
				pool, err := puddle.NewPool(&puddle.Config[int]{
					MaxSize: 1,
					Constructor: func(context.Context) (int, error) {
						id := calls.Next()
						if id == 1 {
							<-finishConstruction
							return 0, constructErr
						}
						return id, nil
					},
					Destructor: func(int) { destructions.Next() },
				})
				require.NoError(t, err)
				firstResult := make(chan error, 1)
				var workers sync.WaitGroup
				workers.Go(func() {
					switch method {
					case "Acquire":
						_, err := pool.Acquire(context.Background())
						firstResult <- err
					case "TryAcquire":
						_, err := pool.TryAcquire(context.Background())
						firstResult <- err
					case "CreateResource":
						firstResult <- pool.CreateResource(context.Background())
					}
				})
				synctest.Wait()
				require.EqualValues(t, 1, pool.Stat().ConstructingResources())
				waiterResult := make(chan *puddle.Resource[int], 1)
				workers.Go(func() {
					res, err := pool.Acquire(context.Background())
					if err != nil {
						t.Errorf("waiting Acquire: %v", err)
					}
					waiterResult <- res
				})
				synctest.Wait()
				require.Empty(t, waiterResult, "the first constructor still owns the only permit")
				finishOnce.Do(func() { close(finishConstruction) })
				workers.Wait()
				if method == "TryAcquire" {
					require.ErrorIs(t, <-firstResult, puddle.ErrNotAvailable)
				} else {
					require.ErrorIs(t, <-firstResult, constructErr)
				}
				res := <-waiterResult
				require.NotNil(t, res)
				require.Equal(t, 2, res.Value())
				res.Release()
				pool.Close()
				require.Equal(t, 1, destructions.Value(), "failed construction must not call the destructor")
				require.Zero(t, pool.Stat().TotalResources())
			})
		})
	}
}

func TestPoolAcquireAdmittedBeforeClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		finishConstruction := make(chan struct{})
		var finishOnce sync.Once
		defer finishOnce.Do(func() { close(finishConstruction) })
		var destructions Counter
		pool, err := puddle.NewPool(&puddle.Config[int]{
			MaxSize: 1,
			Constructor: func(context.Context) (int, error) {
				<-finishConstruction
				return 1, nil
			},
			Destructor: func(int) { destructions.Next() },
		})
		require.NoError(t, err)
		result := make(chan *puddle.Resource[int], 1)
		var workers sync.WaitGroup
		workers.Go(func() {
			res, err := pool.Acquire(context.Background())
			if err != nil {
				t.Errorf("admitted Acquire: %v", err)
			}
			result <- res
		})
		synctest.Wait()
		require.EqualValues(t, 1, pool.Stat().ConstructingResources())
		closed := make(chan struct{})
		workers.Go(func() {
			pool.Close()
			close(closed)
		})
		synctest.Wait()
		finishOnce.Do(func() { close(finishConstruction) })
		synctest.Wait()
		res := <-result
		require.NotNil(t, res, "an admitted Acquire may succeed during Close")
		select {
		case <-closed:
			t.Fatal("Close returned while a resource was still acquired")
		default:
		}
		res.Release()
		workers.Wait()
		require.Equal(t, 1, destructions.Value())
		require.Zero(t, pool.Stat().TotalResources())
	})
}

func TestPoolDestroyRetainsCapacityUntilDestructorReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		finishDestruction := make(chan struct{})
		var finishOnce sync.Once
		defer finishOnce.Do(func() { close(finishDestruction) })
		constructor, calls := createConstructor()
		var destructions Counter
		pool, err := puddle.NewPool(&puddle.Config[int]{
			MaxSize:     1,
			Constructor: constructor,
			Destructor: func(id int) {
				if id == 1 {
					<-finishDestruction
				}
				destructions.Next()
			},
		})
		require.NoError(t, err)
		res, err := pool.Acquire(context.Background())
		require.NoError(t, err)
		res.Destroy()
		result := make(chan *puddle.Resource[int], 1)
		var workers sync.WaitGroup
		workers.Go(func() {
			res, err := pool.Acquire(context.Background())
			if err != nil {
				t.Errorf("Acquire after Destroy: %v", err)
			}
			result <- res
		})
		synctest.Wait()
		require.Empty(t, result)
		require.Equal(t, 1, calls.Value())
		require.EqualValues(t, 1, pool.Stat().AcquiredResources())
		finishOnce.Do(func() { close(finishDestruction) })
		workers.Wait()
		res = <-result
		require.NotNil(t, res)
		require.Equal(t, 2, res.Value())
		res.Release()
		pool.Close()
		require.Equal(t, 2, destructions.Value())
	})
}

// The ledger checks effects on actual values independently of pool statistics.
// A seeded operation stream is reproducible; the Go scheduler is not.
func TestStressLifecycle(t *testing.T) {
	for _, capacity := range []int32{1, 2, 4} {
		t.Run(fmt.Sprintf("capacity=%d", capacity), func(t *testing.T) {
			type value struct {
				id        int
				owned     atomic.Bool
				hijacked  atomic.Bool
				destroyed atomic.Int32
			}
			var ledgerMu sync.Mutex
			var ledger []*value
			var attempts Counter
			constructErr := errors.New("controlled construction failure")
			pool, err := puddle.NewPool(&puddle.Config[*value]{
				MaxSize: capacity,
				Constructor: func(context.Context) (*value, error) {
					id := attempts.Next()
					time.Sleep(time.Duration(id%3) * time.Microsecond)
					if id%7 == 0 {
						return nil, constructErr
					}
					v := &value{id: id}
					ledgerMu.Lock()
					ledger = append(ledger, v)
					ledgerMu.Unlock()
					return v, nil
				},
				Destructor: func(v *value) {
					if v.owned.Load() || v.hijacked.Load() {
						t.Errorf("destroying owned or hijacked resource %d", v.id)
					}
					if n := v.destroyed.Add(1); n != 1 {
						t.Errorf("resource %d destroyed %d times", v.id, n)
					}
					time.Sleep(10 * time.Microsecond)
				},
			})
			require.NoError(t, err)
			use := func(res *puddle.Resource[*value], rng *rand.Rand) {
				v := res.Value()
				if !v.owned.CompareAndSwap(false, true) {
					t.Errorf("resource %d acquired by multiple callers", v.id)
				}
				if v.destroyed.Load() != 0 || v.hijacked.Load() {
					t.Errorf("resource %d reused after destruction or hijack", v.id)
				}
				runtime.Gosched()
				v.owned.Store(false)
				switch rng.Intn(4) {
				case 0:
					res.Release()
				case 1:
					res.ReleaseUnused()
				case 2:
					res.Destroy()
				case 3:
					v.hijacked.Store(true)
					res.Hijack()
				}
			}
			checkError := func(err error) {
				if err != nil && !errors.Is(err, constructErr) && !errors.Is(err, puddle.ErrClosedPool) &&
					!errors.Is(err, puddle.ErrNotAvailable) && !errors.Is(err, context.Canceled) {
					t.Errorf("unexpected pool error: %v", err)
				}
			}
			stop := make(chan struct{})
			var workers sync.WaitGroup
			const seed = int64(20261114)
			t.Logf("operation seed: %d", seed)
			for worker := range 8 {
				workers.Go(func() {
					rng := rand.New(rand.NewSource(seed + int64(worker)))
					for {
						select {
						case <-stop:
							return
						default:
						}
						switch rng.Intn(7) {
						case 0, 1:
							ctx, cancel := context.WithCancel(context.Background())
							var timer *time.Timer
							if rng.Intn(2) == 0 {
								timer = time.AfterFunc(time.Microsecond, cancel)
							}
							res, err := pool.Acquire(ctx)
							cancel()
							if timer != nil {
								timer.Stop()
							}
							checkError(err)
							if res != nil {
								use(res, rng)
							}
						case 2:
							res, err := pool.TryAcquire(context.Background())
							checkError(err)
							if res != nil {
								use(res, rng)
							}
						case 3:
							checkError(pool.CreateResource(context.Background()))
						case 4:
							for _, res := range pool.AcquireAllIdle() {
								use(res, rng)
							}
						case 5:
							pool.Reset()
						case 6:
							stat := pool.Stat()
							if stat.TotalResources() > capacity || stat.TotalResources() < 0 {
								t.Errorf("invalid resource count: %d", stat.TotalResources())
							}
						}
					}
				})
			}
			time.Sleep(stressTestDur(t))
			// Keep workers active while concurrent Close calls drain construction,
			// checked-out values, and delayed destructors.
			var closers sync.WaitGroup
			for range 2 {
				closers.Go(pool.Close)
			}
			closers.Wait()
			close(stop)
			workers.Wait()
			ledgerMu.Lock()
			defer ledgerMu.Unlock()
			require.NotEmpty(t, ledger)
			for _, v := range ledger {
				require.False(t, v.owned.Load(), "resource %d still owned", v.id)
				wantDestructions := int32(1)
				if v.hijacked.Load() {
					wantDestructions = 0
				}
				require.Equal(t, wantDestructions, v.destroyed.Load(), "resource %d", v.id)
			}
		})
	}
}
