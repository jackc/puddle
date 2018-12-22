package pool_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPoolGet_CreatesResourceWhenNoneAvailable(t *testing.T) {
	createCalls := 0
	createFunc := func() (interface{}, error) {
		createCalls += 1
		return createCalls, nil
	}
	pool := pool.New(createFunc)

	res, err := pool.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res)

	pool.Return(res)
}

func TestPoolGet_ReturnsErrorFromFailedResourceCreate(t *testing.T) {
	errCreateFailed := errors.New("create failed")
	createFunc := func() (interface{}, error) {
		return nil, errCreateFailed
	}
	pool := pool.New(createFunc)

	res, err := pool.Get(context.Background())
	assert.Equal(t, errCreateFailed, err)
	assert.Nil(t, res)
}

func TestPoolGet_ReusesResources(t *testing.T) {
	createCalls := 0
	createFunc := func() (interface{}, error) {
		createCalls += 1
		return createCalls, nil
	}
	pool := pool.New(createFunc)

	res, err := pool.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res)

	pool.Return(res)

	res, err = pool.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res)

	pool.Return(res)

	assert.Equal(t, 1, createCalls)
}

func TestPoolGet_ContextAlreadyCanceled(t *testing.T) {
	createFunc := func() (interface{}, error) {
		panic("should never be called")
	}
	pool := pool.New(createFunc)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := pool.Get(ctx)
	assert.Equal(t, context.Canceled, err)
	assert.Nil(t, res)
}

func TestPoolGet_ContextCanceledDuringCreate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	createCalls := 0

	createFunc := func() (interface{}, error) {
		cancel()
		time.Sleep(1 * time.Second)
		createCalls += 1
		return createCalls, nil
	}
	pool := pool.New(createFunc)

	res, err := pool.Get(ctx)
	assert.Equal(t, context.Canceled, err)
	assert.Nil(t, res)
}

func TestPoolReturnPanicsIfResourceNotPartOfPool(t *testing.T) {
	createCalls := 0
	createFunc := func() (interface{}, error) {
		createCalls += 1
		return createCalls, nil
	}
	pool := pool.New(createFunc)

	assert.Panics(t, func() { pool.Return(42) })
}
