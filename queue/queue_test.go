package queue

import (
	"testing"
)

func TestQueueIsEmpty(t *testing.T) {
	// Make an empty queue
	q := NewQueue[string](2)

	// Should be empty
	if !q.IsEmpty() {
		t.Fatal("The queue should be empty")
	}

	// Add something
	q.Push("Hello")
	q.Push("world")
	q.Push(":)")

	// Should have 3 elements
	if q.IsEmpty() {
		t.Fatal("The queue should not be empty")
	}

	if v, _ := q.Next(); v != "Hello" {
		t.Fatal("The next returned incorrect value")
	}

	q.Next()
	if v := q.Peek(); v != ":)" {
		t.Fatal("The peek returned incorrect value")
	}

	q.Next()
	if _, ok := q.Next(); ok {
		t.Fatal("The queue should be empty and returned empty type value and False")
	}

	q.Next()

	// Should be empty because we used all its elements
	if !q.IsEmpty() {
		t.Fatal("The queue should be empty")
	}

}
