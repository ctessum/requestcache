package requestcache

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestDeDuplicate(t *testing.T) {
	p := func(ctx context.Context, r interface{}) (interface{}, error) {
		time.Sleep(10 * time.Millisecond)
		return 2, nil
	}

	c := NewCache(p, 2, Deduplicate())

	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			r := c.NewRequest(context.Background(), 2, "xxx")
			result, err := r.Result()
			if err != nil {
				t.Error(err)
			}
			if result.(int) != 2 {
				t.Errorf("result should be 2 but is %d", result.(int))
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
	p := func(ctx context.Context, r interface{}) (interface{}, error) {
		return 2, nil
	}

	c := NewCache(p, 2, Memory(5))

	for i := 0; i < 10; i++ {
		r := c.NewRequest(context.Background(), 2, "xxx")
		result, err := r.Result()
		if err != nil {
			t.Error(err)
		}
		if result.(int) != 2 {
			t.Errorf("result should be 2 but is %d", result.(int))
		}
	}
	requestsExpected := []int{10, 1}
	if !reflect.DeepEqual(c.Requests(), requestsExpected) {
		t.Errorf("number of requests expected be %v but was %v", requestsExpected, c.Requests())
	}
}

func TestDisk(t *testing.T) {
	p := func(ctx context.Context, r interface{}) (interface{}, error) {
		return 2, nil
	}

	c := NewCache(p, 2, Disk(".", MarshalGob, UnmarshalGob))

	for i := 0; i < 10; i++ {
		r := c.NewRequest(context.Background(), 2, "xxx")
		result, err := r.Result()
		if err != nil {
			t.Error(err)
		}
		if result.(int) != 2 {
			t.Errorf("result should be 2 but is %d", result.(int))
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
	p := func(ctx context.Context, r interface{}) (interface{}, error) {
		return 2, nil
	}

	db, err := sql.Open("sqlite3", "testdb.sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove("testdb.sqlite3")

	sqlCache, err := SQL(context.Background(), db, MarshalGob, UnmarshalGob)
	if err != nil {
		t.Fatal(err)
	}

	c := NewCache(p, 2, sqlCache)

	for i := 0; i < 10; i++ {
		r := c.NewRequest(context.Background(), 2, "xxx")
		result, err := r.Result()
		if err != nil {
			t.Error(err)
		}
		if result.(int) != 2 {
			t.Errorf("result should be 2 but is %d", result.(int))
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
	p := func(ctx context.Context, r interface{}) (interface{}, error) {
		return 2, nil
	}

	// First, cache a result to disk.
	c := NewCache(p, 2, Disk(".", MarshalGob, UnmarshalGob))
	r := c.NewRequest(context.Background(), 2, "yyy")
	result, err := r.Result()
	if err != nil {
		t.Fatal(err)
	}
	if result.(int) != 2 {
		t.Fatalf("disk cache result should be 2 but is %d", result.(int))
	}

	// Create a local server for our saved result.
	s := httptest.NewServer(http.FileServer(http.Dir(".")))

	// Now, test our HTTP cache.
	c = NewCache(p, 2, HTTP(s.URL, UnmarshalGob))
	for i := 0; i < 10; i++ {
		r := c.NewRequest(context.Background(), 2, "yyy")
		result, err := r.Result()
		if err != nil {
			t.Fatal(err)
		}
		if result.(int) != 2 {
			t.Errorf("HTTP cache result should be 2 but is %d", result.(int))
		}
	}

	// Make sure the HTTP cache caught all 10 requests.
	requestsExpected := []int{10, 0}
	if !reflect.DeepEqual(c.Requests(), requestsExpected) {
		t.Errorf("number of requests expected be %v but was %v", requestsExpected, c.Requests())
	}

	// Check what happens when we request a non-existent file
	result, err = c.NewRequest(context.Background(), 2, "qqqq").Result()
	if err != nil {
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
	p := func(ctx context.Context, r interface{}) (interface{}, error) {
		return 2, nil
	}

	c := NewCache(p, 2, Memory(5), Disk(".", MarshalGob, UnmarshalGob))

	for i := 0; i < 10; i++ {
		r := c.NewRequest(context.Background(), 2, "xxx")
		result, err := r.Result()
		if err != nil {
			t.Error(err)
		}
		if result.(int) != 2 {
			t.Errorf("result should be 2 but is %d", result.(int))
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
	p := func(ctx context.Context, r interface{}) (interface{}, error) {
		return 2, fmt.Errorf("test error")
	}

	c := NewCache(p, 2, Deduplicate(), Memory(5), Disk(".", MarshalGob, UnmarshalGob))

	for i := 0; i < 10; i++ {
		r := c.NewRequest(context.Background(), 2, "xxx")
		_, err := r.Result()
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
