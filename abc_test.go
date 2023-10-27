package abc

import (
	"github.com/google/go-cmp/cmp"
	"testing"
	"time"
)

var (
	TEST_KEY, TEST_SECRET, TEST_ENDPOINT, TEST_REGION, TEST_BUCKET string
)

type TestDummy struct {
	X   int
	Str string
	Dt  time.Time
	Arr []string
}

func TestABC(t *testing.T) {
	var (
		a    ABC
		x, y TestDummy
	)

	a.Init(TEST_KEY, TEST_SECRET, TEST_ENDPOINT, TEST_REGION)
	key := "test-abc.json"

	x.X = 1
	x.Str = "Hello"
	x.Dt = time.Now()
	x.Arr = []string{"a", "b", "c"}

	err := a.Put(TEST_BUCKET, key, x)
	if err != nil {
		t.Errorf("Put error: %v\n", err)
		return
	}

	err = a.Get(TEST_BUCKET, key, &y)
	if err != nil {
		t.Errorf("Get error: %v\n", err)
		return
	}

	if diff := cmp.Diff(x, y); diff != "" {
		t.Errorf("TestABC diff: %s\n", diff)
	}
}

func TestDefault(t *testing.T) {
	var (
		x, y TestDummy
	)

	Init(TEST_KEY, TEST_SECRET, TEST_ENDPOINT, TEST_REGION)
	key := "test-abc-default.json"

	x.X = 1
	x.Str = "Hello"
	x.Dt = time.Now()
	x.Arr = []string{"a", "b", "c"}

	err := Put(TEST_BUCKET, key, x)
	if err != nil {
		t.Errorf("Put error: %v\n", err)
		return
	}

	err = Get(TEST_BUCKET, key, &y)
	if err != nil {
		t.Errorf("Get error: %v\n", err)
		return
	}

	if diff := cmp.Diff(x, y); diff != "" {
		t.Errorf("TestABC diff: %s\n", diff)
	}
}
