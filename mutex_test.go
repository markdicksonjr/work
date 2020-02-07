package work

import "testing"

func TestNewMutexMap(t *testing.T) {
	b1 := NewMutexMap()
	if b1 == nil {
		t.Fatal("new mutex map is nil")
	}
}
