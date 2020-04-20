package requestcache

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type job struct {
	run func() (int, error)
	key string
}

type result struct {
	i int
}

// MarshalBinary marshals a the receiver to a byte array and fulfills
// the requirements for the Disk cache marshalFunc input.
func (r *result) MarshalBinary() ([]byte, error) {
	return []byte(strconv.Itoa(r.i)), nil
}

// UnmarshalBinary unmarshals the receiver from a byte array and fulfills
// the requirements for the Disk cache unmarshalFunc input.
func (r *result) UnmarshalBinary(b []byte) error {
	var err error
	r.i, err = strconv.Atoi(string(b))
	return err
}

func (j *job) Run(ctx context.Context, r Result) error {
	i, err := j.run()
	if err != nil {
		return err
	}
	rr := r.(*result)
	rr.i = i
	return nil
}

func (j *job) Key() string {
	return j.key
}

func TestDeDuplicate(t *testing.T) {
	j := &job{
		run: func() (int, error) {
			time.Sleep(10 * time.Millisecond)
			return 2, nil
		},
		key: "xxx",
	}

	c := NewCache(Deduplicate())

	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			r := c.NewRequest(context.Background(), j)
			var res result
			if err := r.Result(&res); err != nil {
				t.Error(err)
			}
			if res.i != 2 {
				t.Errorf("result should be 2 but is %d", res.i)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	requestsExpected := []int{10, 1}
	if !reflect.DeepEqual(c.Requests(), requestsExpected) {
		t.Errorf("number of requests expected be %v but was %v", requestsExpected, c.Requests())
	}
}

func TestMemory(t *testing.T) {
	j := &job{
		run: func() (int, error) {
			return 2, nil
		},
		key: "xxx",
	}

	c := NewCache(Memory(5))

	for i := 0; i < 10; i++ {
		r := c.NewRequest(context.Background(), j)
		var res result
		if err := r.Result(&res); err != nil {
			t.Error(err)
		}
		if res.i != 2 {
			t.Errorf("result should be 2 but is %d", res.i)
		}
	}
	requestsExpected := []int{10, 1}
	if !reflect.DeepEqual(c.Requests(), requestsExpected) {
		t.Errorf("number of requests expected be %v but was %v", requestsExpected, c.Requests())
	}
}

func TestDisk(t *testing.T) {
	j := &job{
		run: func() (int, error) {
			return 2, nil
		},
		key: "xxx",
	}

	c := NewCache(Disk("."))

	for i := 0; i < 10; i++ {
		r := c.NewRequest(context.Background(), j)
		var res result
		if err := r.Result(&res); err != nil {
			t.Error(err)
		}
		if res.i != 2 {
			t.Errorf("result should be 2 but is %d", res.i)
		}
	}
	requestsExpected := []int{10, 1}
	if !reflect.DeepEqual(c.Requests(), requestsExpected) {
		t.Errorf("number of requests expected be %v but was %v", requestsExpected, c.Requests())
	}
	// remove cached file.
	os.Remove("xxx.dat")
}

func TestSQLITE(t *testing.T) {
	j := &job{
		run: func() (int, error) {
			return 2, nil
		},
		key: "xxx",
	}

	db, err := sql.Open("sqlite3", "testdb.sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove("testdb.sqlite3")

	sqlCache, err := SQL(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}

	c := NewCache(sqlCache)

	for i := 0; i < 10; i++ {
		r := c.NewRequest(context.Background(), j)
		var res result
		if err := r.Result(&res); err != nil {
			t.Error(err)
		}
		if res.i != 2 {
			t.Errorf("result should be 2 but is %d", res.i)
		}
	}
	requestsExpected := []int{10, 1}
	if !reflect.DeepEqual(c.Requests(), requestsExpected) {
		t.Errorf("number of requests expected be %v but was %v", requestsExpected, c.Requests())
	}
	// remove cached file.
	os.Remove("xxx.dat")
}

func TestHTTP(t *testing.T) {
	j := &job{
		run: func() (int, error) {
			return 2, nil
		},
		key: "yyy",
	}

	// First, cache a result to disk.
	c := NewCache(Disk("."))
	r := c.NewRequest(context.Background(), j)
	var res result
	if err := r.Result(&res); err != nil {
		t.Fatal(err)
	}
	if res.i != 2 {
		t.Fatalf("disk cache result should be 2 but is %d", res.i)
	}

	// Create a local server for our saved result.
	s := httptest.NewServer(http.FileServer(http.Dir(".")))

	// Now, test our HTTP cache.
	c = NewCache(HTTP(s.URL))
	for i := 0; i < 10; i++ {
		r := c.NewRequest(context.Background(), j)
		var res result
		if err := r.Result(&res); err != nil {
			t.Fatal(err)
		}
		if res.i != 2 {
			t.Errorf("HTTP cache result should be 2 but is %d", res.i)
		}
	}

	// Make sure the HTTP cache caught all 10 requests.
	requestsExpected := []int{10, 0}
	if !reflect.DeepEqual(c.Requests(), requestsExpected) {
		t.Errorf("number of requests expected be %v but was %v", requestsExpected, c.Requests())
	}

	// Check what happens when we request a non-existent file
	j.key = "qqqq"
	req := c.NewRequest(context.Background(), j)
	if err := req.Result(&res); err != nil {
		t.Fatal(err)
	}
	requestsExpected = []int{11, 1} // The cache shouldn't catch this request.
	if !reflect.DeepEqual(c.Requests(), requestsExpected) {
		t.Errorf("number of requests expected be %v but was %v", requestsExpected, c.Requests())
	}

	// remove cached file.
	os.Remove("yyy.dat")
}

func TestCombined(t *testing.T) {
	j := &job{
		run: func() (int, error) {
			return 2, nil
		},
		key: "xxx",
	}

	c := NewCache(Memory(5), Disk("."))

	for i := 0; i < 10; i++ {
		r := c.NewRequest(context.Background(), j)
		var res result
		if err := r.Result(&res); err != nil {
			t.Error(err)
		}
		if res.i != 2 {
			t.Errorf("result should be 2 but is %d", res.i)
		}
	}
	requestsExpected := []int{10, 1, 1}
	if !reflect.DeepEqual(c.Requests(), requestsExpected) {
		t.Errorf("number of requests expected be %v but was %v", requestsExpected, c.Requests())
	}
	// remove cached file.
	os.Remove("xxx.dat")
}

func TestCombinedError(t *testing.T) {
	j := &job{
		run: func() (int, error) {
			return 2, fmt.Errorf("test error")
		},
		key: "xxx",
	}

	c := NewCache(Deduplicate(), Memory(5), Disk("."))

	for i := 0; i < 10; i++ {
		r := c.NewRequest(context.Background(), j)
		var res result
		err := r.Result(&res)
		if err == nil || err.Error() != "test error" {
			t.Errorf("try %d; error should be 'test error' but is instead %v", i, err)
		}
	}
	requestsExpected := []int{10, 10, 10, 10}
	if !reflect.DeepEqual(c.Requests(), requestsExpected) {
		t.Errorf("number of requests expected be %v but was %v", requestsExpected, c.Requests())
	}
	// remove cached file.
	os.Remove("xxx.dat")
}

type jobRecursive struct {
	i   int
	key string
}

func (j *jobRecursive) Run(ctx context.Context, c *Cache, r Result) error {
	res1 := new(result)
	res2 := new(result)
	if j.i < 5 {
		req1 := c.NewRequestRecursive(ctx, &jobRecursive{j.i + 1, fmt.Sprint(j.key, "-", j.i+1)})
		if err := req1.Result(res1); err != nil {
			return err
		}
		req2 := c.NewRequestRecursive(ctx, &jobRecursive{j.i + 1, fmt.Sprint(j.key, ";", j.i+1)})
		if err := req2.Result(res2); err != nil {
			return err
		}
	}

	rr := r.(*result)
	rr.i = j.i + res1.i + res2.i
	return nil
}

func (j *jobRecursive) Key() string {
	return j.key
}

func TestCombinedRecursive(t *testing.T) {
	dirName := "test_combined_recursive"
	if err := os.Mkdir(dirName, os.ModePerm); err != nil {
		t.Fatal(err)
	}
	j := &jobRecursive{
		i:   1,
		key: "1",
	}

	c := NewCache(Memory(2), Disk(dirName))

	for i := 0; i < 2; i++ {
		r := c.NewRequestRecursive(context.Background(), j)
		var res result
		if err := r.Result(&res); err != nil {
			t.Error(err)
		}
		if res.i != 129 {
			t.Errorf("result should be 129 but is %d", res.i)
		}
	}
	requestsExpected := []int{32, 31, 31}
	if !reflect.DeepEqual(c.Requests(), requestsExpected) {
		t.Errorf("number of requests expected be %v but was %v", requestsExpected, c.Requests())
	}
	// remove cached file.
	os.RemoveAll(dirName)
}
