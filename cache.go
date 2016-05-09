package slca

import (
	"crypto/md5"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"bitbucket.org/ctessum/slca/greet"
	"bitbucket.org/ctessum/sparse"

	"github.com/golang/groupcache/lru"
)

type request struct {
	edge       *greet.ResultEdge
	results    *greet.Results
	result     interface{}
	returnChan chan *request
	err        error
	funcs      []func(*request)
	key        string
}

func newRequest(r *greet.Results, e *greet.ResultEdge) (*request, error) {
	var key string
	p := r.GetFromNode(e).Process
	switch t := p.(type) {
	case *greet.StationaryProcess:
		sp := p.(*greet.StationaryProcess)
		if sp.SpatialRef == nil {
			return nil, fmt.Errorf("stationary process %s (id=%s) has no SpatialRef", sp.Name, sp.ID)
		}
		key = fmt.Sprintf("%x", md5.Sum([]byte(sp.SpatialRef.Key())))
	case *greet.TransportationProcess:
		key = "transportation"
	case *greet.Mix:
		key = "NoSpatial"
	default:
		return nil, fmt.Errorf("in slca.newRequest: can't make key for type %v", t)
	}

	return &request{
		edge:       e,
		results:    r,
		returnChan: make(chan *request),
		key:        key,
	}, nil
}

// finalize runs all of the functions that have been queued up for running
// after the request has finished processing.
func (r *request) finalize() error {
	if r.err != nil {
		return r.err
	}
	for _, f := range r.funcs {
		f(r)
		if r.err != nil {
			return r.err
		}
	}
	return r.err
}

// deDuplicate avoids duplicating requests.
func deDuplicate(in chan *request) <-chan *request {
	out := make(chan *request)
	var dupLock sync.Mutex
	runningTasks := make(map[string][]*request)

	dupFunc := func(req *request) {
		dupLock.Lock()
		reqs := runningTasks[req.key]
		firstReq := reqs[0]
		for i := 1; i < len(reqs); i++ {
			reqs[i].returnChan <- firstReq
		}
		delete(runningTasks, req.key)
		dupLock.Unlock()
	}

	go func() {
		for req := range in {
			if _, ok := runningTasks[req.key]; ok {
				// This task is currently running, so add it to the queue.
				runningTasks[req.key] = append(runningTasks[req.key], req)
			} else {
				// This task is not currently running, so add it to the beginning of the
				// queue and pass it on.
				dupLock.Lock()
				runningTasks[req.key] = []*request{req}
				dupLock.Unlock()
				req.funcs = append(req.funcs, dupFunc)
				out <- req
			}
		}
	}()
	return out
}

// memoryCache manages an in-memory cache of results.
func memoryCache(in <-chan *request) <-chan *request {
	out := make(chan *request)
	const maxEntries = 50 // max number of items in the cache
	cache := lru.New(maxEntries)

	// cacheFunc adds the data to the cache and is sent along
	// with the request if the data is not in the cache
	cacheFunc := func(req *request) {
		cache.Add(req.key, req.result)
	}

	go func() {
		for req := range in {
			if d, ok := cache.Get(req.key); ok {
				// If the item is in the cache, return it
				req.result = d
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

// diskCache manages an in-memory cache of results, where dir is the
// directory in which to store results
func diskCache(in <-chan *request, dir string) <-chan *request {

	// These are the data types that will be saved in the cache.
	gob.Register(sparse.SparseArray{})
	gob.Register(sparse.DenseArray{})
	gob.Register(map[string][]float64{})

	out := make(chan *request)

	// This function writes the data to the disk after it is
	// created, and is sent along with the request if the data is
	// not in the cache.
	writeFunc := func(req *request) {
		fname := filepath.Join(dir, req.key+".gob")
		w, err := os.Create(fname)
		if err != nil {
			req.err = err
			return
		}
		defer w.Close()
		enc := gob.NewEncoder(w)
		err = enc.Encode(&req.result)
		if err != nil {
			req.err = err
		}
	}

	go func() {
		for req := range in {
			fname := filepath.Join(dir, req.key+".gob")

			f, err := os.Open(fname)
			if err != nil {
				// If we can't open the file, assume that it doesn't exist and Pass
				// the request on.
				req.funcs = append(req.funcs, writeFunc)
				out <- req
				continue
			}
			var data interface{}
			dec := gob.NewDecoder(f)
			if err := dec.Decode(&data); err != nil {
				// There is some problem with the file. Pass the request on to
				// recreate it.
				log.Println("error decoding from disk cache: ", err)
				req.funcs = append(req.funcs, writeFunc)
				out <- req
				continue
			}
			if err := f.Close(); err != nil {
				req.err = err
			}
			// Successfully retrieved the result. Now add it to the request
			// so it is stored in the cache and return it to the requester.
			req.result = data
			req.returnChan <- req
		}
	}()
	return out
}
