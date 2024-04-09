package main

import (
	"testing"
)

func TestExample(t *testing.T) {
	err := Run()

	if err != nil {
		t.Fatal(err)
	}
}
