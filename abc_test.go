package abc

import (
	"github.com/google/go-cmp/cmp"
	"sort"
	"strconv"
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
	key := "abc-test/test-abc.json"

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

func TestABCRaw(t *testing.T) {
	var (
		a    ABC
		x, y string
		tmp  []byte
	)

	a.Init(TEST_KEY, TEST_SECRET, TEST_ENDPOINT, TEST_REGION)
	key := "abc-test/test-abc-raw.json"

	x = "Hello World"

	err := a.PutRaw(TEST_BUCKET, key, []byte(x))
	if err != nil {
		t.Errorf("Put error: %v\n", err)
		return
	}

	tmp, err = a.GetRaw(TEST_BUCKET, key)
	if err != nil {
		t.Errorf("Get error: %v\n", err)
		return
	}
	y = string(tmp)

	if x != y {
		t.Errorf("TestABCRaw diff: [%s] [%s]\n", x, y)
	}
}

func TestList(t *testing.T) {
	var (
		a     ABC
		check []string
	)

	a.Init(TEST_KEY, TEST_SECRET, TEST_ENDPOINT, TEST_REGION)
	prefix := "abc-test/list/"

	for i := 0; i < 1100; i++ {
		body := []byte("Hello World " + strconv.Itoa(i))
		key := prefix + "hello-world-" + strconv.Itoa(i) + ".txt"
		check = append(check, key)
		a.PutRaw(TEST_BUCKET, key, body)
	}

	list, err := a.List(TEST_BUCKET, prefix)
	if err != nil {
		t.Errorf("List error: %v\n", err)
		return
	}

	if len(list) != len(check) {
		t.Errorf("List result len [%v] does not match expected [%v]\n", len(list), len(check))
		return
	}

	x := sort.StringSlice(list)
	y := sort.StringSlice(check)

	x.Sort()
	y.Sort()

	for i := 0; i < len(list); i++ {
		if x[i] != y[i] {
			t.Errorf("List result does not match at [%v]: %v | %v\n\n", i, x[i], y[i])
		}
	}
}

func TestExists(t *testing.T) {
	var (
		a ABC
	)

	a.Init(TEST_KEY, TEST_SECRET, TEST_ENDPOINT, TEST_REGION)
	existent := "abc-test/test-existent.txt"
	nonexistent := "abc-test/some-nonexistent-key.txt"

	err := a.PutRaw(TEST_BUCKET, existent, []byte("Hello World"))
	if err != nil {
		t.Errorf("Put error: %v\n", err)
		return
	}

	ex, err := a.Exists(TEST_BUCKET, existent)
	if err != nil {
		t.Errorf("Exists error: %v\n", err)
		return
	}

	if !ex {
		t.Errorf("Object should exist, but it doesn't")
		return
	}

	ex, err = a.Exists(TEST_BUCKET, nonexistent)
	if err != nil {
		t.Errorf("Exists2 error: %v\n", err)
		return
	}

	if ex {
		t.Errorf("Object should NOT exist, but it does")
		return
	}

}

func TestCursor(t *testing.T) {
	var (
		a           ABC
		check, list []string
	)

	a.Init(TEST_KEY, TEST_SECRET, TEST_ENDPOINT, TEST_REGION)
	prefix := "abc-test/cursor/"

	for i := 0; i < 1100; i++ {
		body := []byte("Hello Cursor " + strconv.Itoa(i))
		key := prefix + "hello-cursor-" + strconv.Itoa(i) + ".txt"
		check = append(check, key)
		a.PutRaw(TEST_BUCKET, key, body)
	}

	cur := a.Cursor(TEST_BUCKET, prefix)
	for {
		key, exists, err := cur.Next()

		if err != nil {
			t.Errorf("Cursor.Next error: %v\n", err)
			return
		}

		if !exists {
			break
		}

		list = append(list, key)
	}

	if len(list) != len(check) {
		t.Errorf("List result len [%v] does not match expected [%v]\n", len(list), len(check))
		return
	}

	x := sort.StringSlice(list)
	y := sort.StringSlice(check)

	x.Sort()
	y.Sort()

	for i := 0; i < len(list); i++ {
		if x[i] != y[i] {
			t.Errorf("List result does not match at [%v]: %v | %v\n\n", i, x[i], y[i])
		}
	}
}
