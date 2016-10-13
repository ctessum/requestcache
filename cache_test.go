package requestcache

import (
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"
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

	c := NewCache(p, 2, Memory(5), Disk(".", MarshalGob, UnmarshalGob))

	for i := 0; i < 10; i++ {
		r := c.NewRequest(context.Background(), 2, "xxx")
		_, err := r.Result()
		if err.Error() != "test error" {
			t.Errorf("error should be 'test error' but is instead %v", err)
		}
	}
	requestsExpected := []int{10, 10, 10}
	if !reflect.DeepEqual(c.Requests(), requestsExpected) {
		t.Errorf("number of requests expected be %v but was %v", requestsExpected, c.Requests())
	}
	// remove cached file.
	os.Remove("xxx.dat")
}
