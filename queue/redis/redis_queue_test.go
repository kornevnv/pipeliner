package redis

import (
	"testing"
)

func TestQueueIsEmpty(t *testing.T) {
	qname := "testQ"
	q, err := New[string]("localhost:6379", "password", 0, qname)
	if err != nil {
		t.Fatal(err)
	}

	err = q.Clear()
	if err != nil {
		t.Fatal(err)
	}
	// Should be empty
	if q.Len() != 0 {
		t.Fatal("The queue should be empty")
	}

	// Add something
	q.Push("Hello")
	q.Push("world")
	q.Push(":)")

	// Should have 3 elements
	if q.Len() == 0 {
		t.Fatal("The queue should not be empty")
	}

	if v, _, _ := q.Next(); v != "Hello" {
		t.Fatal("The next returned incorrect value")
	}

	q.Next()
	q.Next()
	if _, ok, _ := q.Next(); ok {
		t.Fatal("The queue should be empty and returned empty type value and False")
	}

	q.Next()

	// Should be empty because we used all its elements
	if q.Len() != 0 {
		t.Fatal("The queue should be empty")
	}

}
