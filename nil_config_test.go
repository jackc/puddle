package puddle

import "testing"

func TestNewPoolNilConfig(t *testing.T) {
	p, err := NewPool[int](nil)
	if err == nil || p != nil {
		t.Fatalf("p=%v err=%v", p, err)
	}
}
