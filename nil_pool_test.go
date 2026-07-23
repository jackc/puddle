package puddle

import (
	"context"
	"testing"
)

func TestNilPoolAcquire(t *testing.T) {
	var p *Pool[int]
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked: %v", r)
		}
	}()
	_, err := p.Acquire(context.Background())
	if err == nil {
		t.Fatal("want error")
	}
}
