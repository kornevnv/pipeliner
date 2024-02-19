package memQueue

import (
	"testing"
)

func TestQueueIsEmpty(t *testing.T) {
	// Make an empty queue
	q := New[string](2, "testQ")

	// Should be empty
	if !q.IsEmpty() {
		t.Fatal("The queue should be empty")
	}

	// Add something
	err := q.Push("Hello")
	if err != nil {
		t.Fatal("push error")
	}

	_ = q.Push("world")
	_ = q.Push(":)")

	// Should have 3 elements
	if q.IsEmpty() {
		t.Fatal("The queue should not be empty")
	}

	if v, _, _ := q.Next(); v != "Hello" {
		t.Fatal("The next returned incorrect value")
	}

	_, _, _ = q.Next()
	if v := q.Peek(); v != ":)" {
		t.Fatal("The peek returned incorrect value")
	}

	_, _, _ = q.Next()
	if _, ok, _ := q.Next(); ok {
		t.Fatal("The queue should be empty and returned empty type value and False")
	}

	_, _, _ = q.Next()

	// Should be empty because we used all its elements
	if !q.IsEmpty() {
		t.Fatal("The queue should be empty")
	}

}
