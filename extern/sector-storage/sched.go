package sectorstorage

import (
	"context"
	//"fmt"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/filecoin-project/sector-storage/stores"
	"github.com/hashicorp/go-multierror"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type schedPrioCtxKey int

var SchedPriorityKey schedPrioCtxKey
var DefaultSchedPriority = 0
var SelectorTimeout = 5 * time.Second

func getPriority(ctx context.Context) int {
	sp := ctx.Value(SchedPriorityKey)
	if p, ok := sp.(int); ok {
		return p
	}

	return DefaultSchedPriority
}

func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, SchedPriorityKey, priority)
}

const mib = 1 << 20

type WorkerAction func(ctx context.Context, w Worker) error

type WorkerSelector interface {
	Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, a *workerHandle) (bool, error) // true if worker is acceptable for performing a task

	Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) // true if a is preferred over b
}

type scheduler struct {
	spt 				abi.RegisteredSealProof					// int64

	workersLk  			sync.RWMutex
	nextWorker 			WorkerID							// uint64
	workers    			map[WorkerID]*workerHandle		

	newWorkers 			chan *workerHandle

	watchClosing  		chan WorkerID
	workerClosing 		chan WorkerID

	schedule       		chan *workerRequest
	workerFree 			chan WorkerID
	info 				chan func(interface{})
	// owned by the sh.runSched goroutine
	schedQueue  		*requestQueue

	closing  			chan struct{}
	closed   			chan struct{}
	testSync 			chan struct{} // used for testing

	sectorGroupIds 		map[abi.SectorID]int
	mutualPathMap  		map[int]string
}

type workerHandle struct {
	w 					Worker

	info 				storiface.WorkerInfo

	preparing 			*activeResources
	active    			*activeResources

	// stats / tracking
	wt 					*workTracker
}

type activeResources struct {
	memUsedMin 			uint64
	memUsedMax 			uint64
	gpuUsed    			bool
	cpuUse     			uint64

	cond 				*sync.Cond
}

type workerRequest struct {
	sector   			abi.SectorID							// Miner(uint64) && SectorNumber(uint64)
	taskType 			sealtasks.TaskType						// string
	priority 			int // larger values more important
	sel     			WorkerSelector				

	prepare 			WorkerAction
	work    			WorkerAction
	
	start 				time.Time

	index 				int // The index of the item in the heap.

	ret       			chan<- workerResponse
	ctx       			context.Context
}

type workerResponse struct {
	err error
}

func newScheduler(spt abi.RegisteredSealProof) *scheduler {
	return &scheduler{
		spt: 			spt,

		nextWorker: 	0,
		workers:    	map[WorkerID]*workerHandle{},

		newWorkers: 	make(chan *workerHandle),

		watchClosing:  	make(chan WorkerID),
		workerClosing: 	make(chan WorkerID),

		schedule:       make(chan *workerRequest),
		workerFree:	 	make(chan WorkerID),
		schedQueue: 	&requestQueue{},

		info: 			make(chan func(interface{})),

		closing: 		make(chan struct{}),
		closed:  		make(chan struct{}),

		sectorGroupIds: map[abi.SectorID]int{},
		mutualPathMap:  map[int]string{},
	}
}

func (sh *scheduler) Schedule(ctx context.Context, sector abi.SectorID, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	case sh.schedule <- &workerRequest{
		sector:   		sector,
		taskType: 		taskType,
		priority: 		getPriority(ctx),
		sel:      		sel,

		prepare: 		prepare,
		work:    		work,

		start: 			time.Now(),

		ret: 			ret,
		ctx: 			ctx,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *workerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.ctx.Done():
		log.Warnf("request got cancelled before we could respond")
	}
}

type SchedDiagRequestInfo struct {
	Sector   			abi.SectorID
	TaskType 			sealtasks.TaskType
	Priority 			int
	GroupID  			int
}

type SchedDiagInfo struct {
	Requests    []SchedDiagRequestInfo
	OpenWindows []WorkerID
}

func (sh *scheduler) runSched() {
	defer close(sh.closed)

	go sh.runWorkerWatcher()

	for {
		select {
		case w := <-sh.newWorkers:
			sh.schedNewWorker(w)
		case wid := <-sh.workerClosing:
			sh.schedDropWorker(wid)
		case req := <-sh.schedule:
			if sh.testSync != nil {
				sh.testSync <- struct{}{}
			}
			scheduled, err := sh.maybeSchedRequest(req)
			if err != nil {
				req.respond(err)
				continue
			}
			if scheduled {
				continue
			}

			sh.schedQueue.Push(req)
		case ireq := <-sh.info:
			ireq(sh.diag())
		case wid := <-sh.workerFree:
			sh.onWorkerFreed(wid)
		case <-sh.closing:
			sh.schedClose()
			return
		}
	}
}

func (sh *scheduler) diag() SchedDiagInfo {
	var out SchedDiagInfo

	log.Infof("DECENTRAL: in sector-storage, sched diag info")

	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {
		task := (*sh.schedQueue)[sqi]

		id, err := sh.findSectorGroupId(task.sector)

		if err != nil {
			id = -1
		}

		out.Requests = append(out.Requests, SchedDiagRequestInfo{
			Sector:   task.sector,
			TaskType: task.taskType,
			Priority: task.priority,
			GroupID:  id,
		})
	}

	return out
}

func (sh *scheduler) onWorkerFreed(wid WorkerID) {
	sh.workersLk.Lock()
	w, ok := sh.workers[wid]
	sh.workersLk.Unlock()
	if !ok {
		log.Warnf("onWorkerFreed on invalid worker %d", wid)
		return
	}

	for i := 0; i < sh.schedQueue.Len(); i++ {
		req := (*sh.schedQueue)[i]

		ok, err := req.sel.Ok(req.ctx, req.taskType, sh.spt, w)
		if err != nil {
			log.Errorf("onWorkerFreed req.sel.Ok error: %+v", err)
			continue
		}

		if !ok {
			continue
		}

		scheduled, err := sh.maybeSchedRequest(req)
		if err != nil {
			req.respond(err)
			continue
		}

		if scheduled {
			sh.schedQueue.Remove(i)
			i--
			continue
		}
	}
}

func (sh *scheduler) maybeSchedRequest(req *workerRequest) (bool, error) {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()

	tried := 0
	var acceptable []WorkerID

	needRes := ResourceTable[req.taskType][sh.spt]

	for wid, worker := range sh.workers {
		if req.taskType == sealtasks.TTPreCommit1 || req.taskType == sealtasks.TTPreCommit2 || req.taskType == sealtasks.TTCommit1 {
			if id, err := sh.findSectorGroupId(req.sector); err != nil {
				log.Errorf("sector %d did not have group: %+v", req.sector.Number, err)
				return false, err
			} else if id != worker.info.WorkerGroupsId {
				continue
			}
		}

		rpcCtx, cancel := context.WithTimeout(req.ctx, SelectorTimeout)
		ok, err := req.sel.Ok(rpcCtx, req.taskType, sh.spt, worker)
		cancel()

		if err != nil {
			return false, err
		}

		if !ok {
			continue
		}
		tried++

		if !worker.preparing.canHandleRequest(needRes, wid, worker.info.Resources) {
			continue
		}

		acceptable = append(acceptable, wid)
	}

	if len(acceptable) > 0 {
		{
			var serr error

			sort.SliceStable(acceptable, func(i, j int) bool {
				rpcCtx, cancel := context.WithTimeout(req.ctx, SelectorTimeout)
				defer cancel()
				r, err := req.sel.Cmp(rpcCtx, req.taskType, sh.workers[acceptable[i]], sh.workers[acceptable[j]])

				if err != nil {
					serr = multierror.Append(serr, err)
				}
				return r
			})

			if serr != nil {
				return false, xerrors.Errorf("error(s) selecting best worker: %w", serr)
			}
		}

		return true, sh.assignWorker(acceptable[0], sh.workers[acceptable[0]], req)
	}

	if tried == 0 {
		return false, xerrors.New("maybeSchedRequest didn't find any good workers")
	}

	return false, nil // put in waiting queue
}

func (sh *scheduler) assignWorker(wid WorkerID, w *workerHandle, req *workerRequest) error {
	needRes := ResourceTable[req.taskType][sh.spt]

	w.preparing.add(w.info.Resources, needRes)

	go func() {
		err := req.prepare(req.ctx, w.wt.worker(w.w))
		sh.workersLk.Lock()

		if err != nil {
			w.preparing.free(w.info.Resources, needRes)
			sh.workersLk.Unlock()

			select {
			case sh.workerFree <- wid:
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond (prepare error: %+v)", err)
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}
			return
		}

		err = w.active.withResources(wid, w.info.Resources, needRes, &sh.workersLk, func() error {
			w.preparing.free(w.info.Resources, needRes)
			sh.workersLk.Unlock()
			defer sh.workersLk.Lock() // we MUST return locked from this function

			select {
			case sh.workerFree <- wid:
			case <-sh.closing:
			}

			err = req.work(req.ctx, w.w)

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond")
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response")
			}

			return nil
		})

		sh.workersLk.Unlock()

		// This error should always be nil, since nothing is setting it, but just to be safe:
		if err != nil {
			log.Errorf("error executing worker (withResources): %+v", err)
		}
	}()

	return nil
}

func (sh *scheduler) Info(ctx context.Context) (interface{}, error) {
	ch := make(chan interface{}, 1)

	sh.info <- func(res interface{}) {
		ch <- res
	}

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sh *scheduler) schedNewWorker(w *workerHandle) {
	sh.workersLk.Lock()

	id := sh.nextWorker
	sh.workers[id] = w
	sh.nextWorker++

	sh.workersLk.Unlock()

	select {
	case sh.watchClosing <- id:
	case <-sh.closing:
		return
	}

	sh.onWorkerFreed(id)
}

func (sh *scheduler) schedDropWorker(wid WorkerID) {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()

	w := sh.workers[wid]
	delete(sh.workers, wid)

	go func() {
		if err := w.w.Close(); err != nil {
			log.Warnf("closing worker %d: %+v", err)
		}
	}()
}

func (sh *scheduler) schedClose() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()

	for i, w := range sh.workers {
		if err := w.w.Close(); err != nil {
			log.Errorf("closing worker %d: %+v", i, err)
		}
	}
}

func (sh *scheduler) Close(ctx context.Context) error {
	close(sh.closing)
	select {
	case <-sh.closed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (sh *scheduler) findSectorGroupId(sector abi.SectorID) (int, error) {
	path, err := readPathJson()
	if err != nil {
		return -2, xerrors.Errorf("read pathConfig.json: %w", err)
	}

	idFile := filepath.Join(path.StorageRepoPath, stores.FTCache.String(), stores.SectorName(sector), "sectorGroupId")
	b, err := ioutil.ReadFile(idFile)
	if err != nil {
		return -2, xerrors.Errorf("read sectorGroupId %s: %w", idFile, err)
	}

	id, err := strconv.Atoi(string(b))
	if err != nil {
		return -2, xerrors.Errorf("convert string to int: %w", err)
	}

	return id, nil
}
