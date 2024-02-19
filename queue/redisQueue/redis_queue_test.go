package redisQueue

// TODO
// отключен специально, т.к. тестирование внешних компонентов упадет в раннере

// func TestQueueIsEmpty(t *testing.T) {
// 	qname := "testQ"
// 	q, err := New[string]("localhost:6379", "password", 0, qname)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
//
// 	err = q.Clear()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	// Should be empty
// 	if q.Len() != 0 {
// 		t.Fatal("The queue should be empty")
// 	}
//
// 	// Add something
// 	err = q.Push("Hello")
// 	if err != nil {
// 		t.Fatal("push error")
// 	}
// 	_ = q.Push("world")
// 	_ = q.Push(":)")
//
// 	// Should have 3 elements
// 	if q.Len() == 0 {
// 		t.Fatal("The queue should not be empty")
// 	}
//
// 	if v, _, _ := q.Next(); v != "Hello" {
// 		t.Fatal("The next returned incorrect value")
// 	}
//
// 	_, _, _ = q.Next()
// 	_, _, _ = q.Next()
// 	if _, ok, _ := q.Next(); ok {
// 		t.Fatal("The queue should be empty and returned empty type value and False")
// 	}
//
// 	_, _, _ = q.Next()
//
// 	// Should be empty because we used all its elements
// 	if q.Len() != 0 {
// 		t.Fatal("The queue should be empty")
// 	}
//
// }
