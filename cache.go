// Package requestcache provides functions for caching on-demand generated data.
package requestcache

import (
	"context"
	"database/sql"
	"encoding"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/golang/groupcache/lru"
)

// Cache is a holder for one or multiple caches.
type Cache struct {
	// requestChan receives incoming requests.
	requestChan chan *Request

	// requests holds the number of requests each individual cache has received.
	requests    []int
	requestLock sync.RWMutex
}

// A job specifies a unit of work to be run and cached with a
// unique key.
type Job interface {
	// Run runs the job and fills the provided result.
	Run(context.Context, Result) error

	// Key returns a unique identifier for this job.
	Key() string
}

type Result interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

// NewCache creates a new set of caches for on-demand generated content, where
// processor numProcessors is the
// number of processors that will be working in parallel and cachefuncs are the
// caches to be used, listed in order of priority.
func NewCache(numProcessors int, cachefuncs ...CacheFunc) *Cache {
	c := &Cache{
		requestChan: make(chan *Request),
		requests:    make([]int, len(cachefuncs)+1),
	}

	in := c.requestChan
	out := in
	for i, cf := range cachefuncs {

		intermediate := make(chan *Request)
		go func(in chan *Request, i int) {
			// Track the number of requests received by this cache.
			for req := range in {
				c.requestLock.Lock()
				c.requests[i]++
				c.requestLock.Unlock()
				intermediate <- req
			}
		}(in, i)

		out = cf(intermediate)
		in = out
	}
	for i := 0; i < numProcessors; i++ {
		go func() {
			for req := range out {
				// Process the results
				c.requestLock.Lock()
				c.requests[len(cachefuncs)]++
				c.requestLock.Unlock()
				err := req.job.Run(req.ctx, req.resultPayload)
				if err != nil {
					req.errs = append(req.errs, err)
				}
				req.returnChan <- req
			}
		}()
	}

	return c
}

// Requests returns the number of requests that each cache has received. The
// last index in the output is the number of requests received by the processor.
// So, for example, the miss rate for the first cache in c is r[len(r)-1] / r[0],
// where r is the result of this function.
func (c *Cache) Requests() []int {
	c.requestLock.Lock()
	out := make([]int, len(c.requests))
	copy(out, c.requests)
	defer c.requestLock.Unlock()
	return out
}

// Request holds information about a request that is to be handled either by
// a cache or a ProcessFunc.
type Request struct {
	ctx           context.Context
	job           Job
	resultPayload Result
	requestChan   chan *Request
	returnChan    chan *Request
	errs          []error
	funcs         []func(*Request)
}

// NewRequest creates a new request where job is the job to be run.
func (c *Cache) NewRequest(ctx context.Context, job Job) *Request {
	return &Request{
		job:         job,
		returnChan:  make(chan *Request),
		requestChan: c.requestChan,
		ctx:         ctx,
	}
}

// Result sends the request for processing and fills the provided result
// variable.
func (r *Request) Result(result Result) error {
	r.resultPayload = result
	r.requestChan <- r
	rr := <-r.returnChan
	return rr.finalize()
}

// finalize runs any clean-up functions that need to be run after the results
// have been generated and returns the first of any errors have may have occurred.
func (r *Request) finalize() error {
	for len(r.funcs) > 0 {
		f := r.funcs[0]
		r.funcs = r.funcs[1:len(r.funcs)]
		f(r)
	}
	if len(r.errs) > 0 {
		return r.errs[0]
	}
	return nil
}

// setPayload fills the payload of the reciever with the specified
// result.
func (r *Request) setPayload(res Result) {
	reflect.ValueOf(r.resultPayload).Elem().Set(reflect.ValueOf(res).Elem())
}

// A CacheFunc can be used to store request results in a cache.
type CacheFunc func(in chan *Request) (out chan *Request)

// Deduplicate avoids duplicating requests.
func Deduplicate() CacheFunc {
	return func(in chan *Request) chan *Request {
		out := make(chan *Request)
		var dupLock sync.Mutex
		runningTasks := make(map[string][]*Request)

		dupFunc := func(req *Request) {
			dupLock.Lock()
			reqs := runningTasks[req.job.Key()]
			for i := 1; i < len(reqs); i++ {
				reqs[i].setPayload(req.resultPayload)
				reqs[i].returnChan <- reqs[i]
			}
			delete(runningTasks, req.job.Key())
			dupLock.Unlock()
		}

		go func() {
			for req := range in {
				dupLock.Lock()
				if _, ok := runningTasks[req.job.Key()]; ok {
					// This task is currently running, so add it to the queue.
					runningTasks[req.job.Key()] = append(runningTasks[req.job.Key()], req)
					dupLock.Unlock()
				} else {
					// This task is not currently running, so add it to the beginning of the
					// queue and pass it on.
					runningTasks[req.job.Key()] = []*Request{req}
					req.funcs = append(req.funcs, dupFunc)
					dupLock.Unlock()
					out <- req
				}
			}
		}()
		return out
	}
}

// Memory manages an in-memory cache of results, where maxEntries is the
// max number of items in the cache. If the results returned by this cache
// are modified by the caller, they may also be modified in the cache.
func Memory(maxEntries int) CacheFunc {
	return func(in chan *Request) chan *Request {
		out := make(chan *Request)
		cache := lru.New(maxEntries)
		var mx sync.RWMutex

		// cacheFunc adds the data to the cache and is sent along
		// with the request if the data is not in the cache
		cacheFunc := func(req *Request) {
			if len(req.errs) > 0 {
				return
			}
			mx.Lock()
			defer mx.Unlock()
			cache.Add(req.job.Key(), req.resultPayload)
		}

		go func() {
			for req := range in {
				mx.RLock()
				d, ok := cache.Get(req.job.Key())
				mx.RUnlock()
				if ok {
					// If the item is in the cache, return it
					req.setPayload(d.(Result))
					req.returnChan <- req
				} else {
					// otherwise, add the request to the cache and send the request along.
					req.funcs = append(req.funcs, cacheFunc)
					out <- req
				}
			}
		}()
		return out
	}
}

// FileExtension is appended to request key names to make
// up the names of files being written to disk.
var FileExtension = ".dat"

// Disk manages an on-disk cache of results, where dir is the
// directory in which to store results.
func Disk(dir string) CacheFunc {
	return func(in chan *Request) chan *Request {

		out := make(chan *Request)

		// This function writes the data to the disk after it is
		// created, and is sent along with the request if the data is
		// not in the cache.
		writeFunc := func(req *Request) {
			if len(req.errs) > 0 {
				return
			}
			fname := filepath.Join(dir, req.job.Key()+FileExtension)
			w, err := os.Create(fname)
			if err != nil {
				req.errs = append(req.errs, err)
				return
			}
			defer w.Close()
			b, err := req.resultPayload.MarshalBinary()
			if err != nil {
				req.errs = append(req.errs, err)
				return
			}
			if _, err = w.Write(b); err != nil {
				req.errs = append(req.errs, err)
				return
			}
		}

		go func() {
			for req := range in {
				fname := filepath.Join(dir, req.job.Key()+FileExtension)

				f, err := os.Open(fname)
				if err != nil {
					// If we can't open the file, assume that it doesn't exist and Pass
					// the request on.
					req.funcs = append(req.funcs, writeFunc)
					out <- req
					continue
				}
				b, err := ioutil.ReadAll(f)
				if err != nil {
					// We can't read the file.
					req.errs = append(req.errs, err)
					req.returnChan <- req
					continue
				}
				if err := req.resultPayload.UnmarshalBinary(b); err != nil {
					// There is some problem with the file.
					req.errs = append(req.errs, err)
					req.returnChan <- req
					continue
				}
				if err := f.Close(); err != nil {
					req.errs = append(req.errs, err)
					req.returnChan <- req
					continue
				}
				// Successfully retrieved the result. Now return it to the requester.
				req.returnChan <- req
			}
		}()
		return out
	}
}

// SQL manages a cache of results in an SQL database,
// where db is the database connection.
func SQL(ctx context.Context, db *sql.DB) (CacheFunc, error) {
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS cache (
	key TEXT PRIMARY KEY,
	data BLOB NOT NULL
);`)
	if err != nil {
		return nil, fmt.Errorf("requestcache: preparing database: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE UNIQUE INDEX cache_key ON cache(key);`); err != nil {
		return nil, fmt.Errorf("requestcache: preparing database index: %v", err)
	}

	writeStmt, err := db.PrepareContext(ctx, `INSERT INTO cache (key,data) VALUES(?,?);`)
	if err != nil {
		return nil, fmt.Errorf("requestcache: preparing database: %v", err)
	}
	readStmt, err := db.PrepareContext(ctx, `SELECT data from cache WHERE key = ?;`)
	if err != nil {
		return nil, fmt.Errorf("requestcache: preparing database: %v", err)
	}

	// This function writes the data to the disk after it is
	// created, and is sent along with the request if the data is
	// not in the cache.
	writeFunc := func(req *Request) {
		if len(req.errs) > 0 {
			return
		}
		b, err := req.resultPayload.MarshalBinary()
		if err != nil {
			req.errs = append(req.errs, err)
			return
		}
		_, err = writeStmt.ExecContext(req.ctx, req.job.Key(), b)
		if err != nil {
			req.errs = append(req.errs, fmt.Errorf("requestcache: writing data to SQL: %v", err))
			return
		}
	}

	return func(in chan *Request) chan *Request {
		out := make(chan *Request)

		go func() {
			for req := range in {
				row := readStmt.QueryRowContext(req.ctx, req.job.Key())
				var b []byte
				err := row.Scan(&b)
				if err == sql.ErrNoRows {
					// Data doesn't doesn't exist: pass
					// the request on.
					req.funcs = append(req.funcs, writeFunc)
					out <- req
					continue
				} else if err != nil {
					// We can't read the file.
					req.errs = append(req.errs, err)
					req.returnChan <- req
					continue
				}
				if err := req.resultPayload.UnmarshalBinary(b); err != nil {
					// There is some problem with the data.
					req.errs = append(req.errs, err)
					req.returnChan <- req
					continue
				}
				// Successfully retrieved the result. Now return it to the requester.
				req.returnChan <- req
			}
		}()
		return out
	}, nil
}

// HTTP retrieves cached requests over an HTTP connection, where addr is the
// address where results are stored.
// This function does not cache requests, it only retrieves previously cached
// requests.
func HTTP(addr string) CacheFunc {
	return func(in chan *Request) chan *Request {

		out := make(chan *Request)

		go func() {
			for req := range in {
				fname := addr + "/" + req.job.Key() + FileExtension

				response, err := http.Get(fname)
				if err != nil {
					// If we don't get a response from the server, return an error.
					req.errs = append(req.errs, fmt.Errorf(response.Status))
					req.returnChan <- req
					continue
				}

				if response.StatusCode != 200 { // Check if the status is 'ok'.
					if response.StatusCode == 404 {
						// If we get a "not found" error, pass the request on.
						out <- req
						continue
					} else {
						// If we get a different status, return an error.
						req.errs = append(req.errs, fmt.Errorf(response.Status))
						req.returnChan <- req
						continue
					}
				}
				b, err := ioutil.ReadAll(response.Body)
				if err != nil {
					// We can't read the file.
					req.errs = append(req.errs, err)
					req.returnChan <- req
					continue
				}
				if err := req.resultPayload.UnmarshalBinary(b); err != nil {
					// There is some problem with the file.
					req.errs = append(req.errs, err)
					req.returnChan <- req
					continue
				}
				if err := response.Body.Close(); err != nil {
					req.errs = append(req.errs, err)
					req.returnChan <- req
					continue
				}
				// Successfully retrieved the result. Now return it to the requester.
				req.returnChan <- req
			}
		}()
		return out
	}
}

// GoogleCloudStorage manages an cache of results in Google Cloud Storage,
// where bucket is the bucket in which to store results and subdir is the
// bucket subdirectory, if any, that should be used.
func GoogleCloudStorage(ctx context.Context, bucket, subdir string) (CacheFunc, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	bkt := client.Bucket(bucket)
	return func(in chan *Request) chan *Request {
		out := make(chan *Request)

		// This function writes the data to the disk after it is
		// created, and is sent along with the request if the data is
		// not in the cache.
		writeFunc := func(req *Request) {
			if len(req.errs) > 0 {
				return
			}
			obj := bkt.Object(subdir + "/" + req.job.Key() + FileExtension)
			w := obj.NewWriter(req.ctx)
			defer w.Close()
			b, err := req.resultPayload.MarshalBinary()
			if err != nil {
				req.errs = append(req.errs, err)
				return
			}
			if _, err = w.Write(b); err != nil {
				req.errs = append(req.errs, err)
				return
			}
		}

		go func() {
			for req := range in {
				obj := bkt.Object(subdir + "/" + req.job.Key() + FileExtension)
				f, err := obj.NewReader(req.ctx)
				if err != nil {
					if err == storage.ErrObjectNotExist {
						req.funcs = append(req.funcs, writeFunc)
						out <- req
						continue
					}
					req.errs = append(req.errs, err)
					req.returnChan <- req
					continue
				}
				b, err := ioutil.ReadAll(f)
				if err != nil {
					// We can't read the file.
					req.errs = append(req.errs, err)
					req.returnChan <- req
					continue
				}
				if err := req.resultPayload.UnmarshalBinary(b); err != nil {
					// There is some problem with the file.
					req.errs = append(req.errs, err)
					req.returnChan <- req
					continue
				}
				if err := f.Close(); err != nil {
					req.errs = append(req.errs, err)
					req.returnChan <- req
					continue
				}
				// Successfully retrieved the result. Now return it to the requester.
				req.returnChan <- req
			}
		}()
		return out
	}, nil
}
