package sectorstorage

import (
	"context"
	"io"
	"net/http"

	logging "github.com/ipfs/go-log/v2"
	"encoding/json"
	"errors"
	"github.com/filecoin-project/sector-storage/fsutil"
	"github.com/ipfs/go-cid"
	"github.com/mitchellh/go-homedir"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/sector-storage/ffiwrapper"
	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/filecoin-project/sector-storage/stores"
	"github.com/filecoin-project/sector-storage/storiface"
)

var log = logging.Logger("advmgr")

var ErrNoWorkers = errors.New("no suitable workers found")

type URLs []string

type Worker interface {
	ffiwrapper.StorageSealer

	MoveStorage(ctx context.Context, sector abi.SectorID) error

	Fetch(ctx context.Context, s abi.SectorID, ft stores.SectorFileType, ptype stores.PathType, am stores.AcquireMode) error
	UnsealPiece(context.Context, abi.SectorID, storiface.UnpaddedByteIndex, abi.UnpaddedPieceSize, abi.SealRandomness, cid.Cid) error
	ReadPiece(context.Context, io.Writer, abi.SectorID, storiface.UnpaddedByteIndex, abi.UnpaddedPieceSize) (bool, error)

	TaskTypes(context.Context) (map[sealtasks.TaskType]struct{}, error)

	// Returns paths accessible to the worker
	Paths(context.Context) ([]stores.StoragePath, error)

	Info(context.Context) (storiface.WorkerInfo, error)

	// returns channel signalling worker shutdown
	Closing(context.Context) (<-chan struct{}, error)

	Close() error
}

type SectorManager interface {
	SectorSize() abi.SectorSize

	ReadPiece(context.Context, io.Writer, abi.SectorID, storiface.UnpaddedByteIndex, abi.UnpaddedPieceSize, abi.SealRandomness, cid.Cid) error

	ffiwrapper.StorageSealer
	storage.Prover
	FaultTracker
}

type WorkerID uint64

type Manager struct {
	scfg *ffiwrapper.Config

	ls         stores.LocalStorage
	storage    *stores.Remote
	localStore *stores.Local
	remoteHnd  *stores.FetchHandler
	index      stores.SectorIndex

	sched *scheduler

	storage.Prover
}

type SealerConfig struct {
	ParallelFetchLimit int

	// Local worker config
	AllowPreCommit1 bool
	AllowPreCommit2 bool
	AllowCommit     bool
	AllowUnseal     bool
}

type StorageAuth http.Header

func New(ctx context.Context, ls stores.LocalStorage, si stores.SectorIndex, cfg *ffiwrapper.Config, sc SealerConfig, urls URLs, sa StorageAuth) (*Manager, error) {
	lstor, err := stores.NewLocal(ctx, ls, si, urls)
	if err != nil {
		return nil, err
	}

	prover, err := ffiwrapper.New(&readonlyProvider{stor: lstor, index: si}, cfg)
	if err != nil {
		return nil, xerrors.Errorf("creating prover instance: %w", err)
	}

	stor := stores.NewRemote(lstor, si, http.Header(sa), sc.ParallelFetchLimit)

	m := &Manager{
		scfg: cfg,

		ls:         ls,
		storage:    stor,
		localStore: lstor,
		remoteHnd:  &stores.FetchHandler{Local: lstor},
		index:      si,

		sched: newScheduler(cfg.SealProofType),

		Prover: prover,
	}

	go m.sched.runSched()

	localTasks_finalize := []sealtasks.TaskType{
		sealtasks.TTCommit1, sealtasks.TTFinalize, sealtasks.TTReadUnsealed,
	}
	localTasks_addpiece := []sealtasks.TaskType{
		sealtasks.TTAddPiece, sealtasks.TTCommit1, sealtasks.TTFinalize, sealtasks.TTReadUnsealed,
	}
	localTasks_fetch := []sealtasks.TaskType{
		sealtasks.TTCommit1, sealtasks.TTFetch, sealtasks.TTReadUnsealed,
	}
	/*
	if sc.AllowPreCommit1 {
		localTasks = append(localTasks, sealtasks.TTPreCommit1)
	}
	if sc.AllowPreCommit2 {
		localTasks = append(localTasks, sealtasks.TTPreCommit2)
	}
	if sc.AllowCommit {
		localTasks = append(localTasks, sealtasks.TTCommit2)
	}
	if sc.AllowUnseal {
		localTasks = append(localTasks, sealtasks.TTUnseal)
	*/
	if sc.AllowUnseal {
	//	localTasks = append(localTasks, sealtasks.TTUnseal)
		localTasks_finalize = append(localTasks_finalize, sealtasks.TTUnseal)
		localTasks_addpiece = append(localTasks_addpiece, sealtasks.TTUnseal)
		localTasks_fetch = append(localTasks_fetch, sealtasks.TTUnseal)
	}

	err = m.AddWorker(ctx, NewLocalWorker(WorkerConfig{
		SealProof: cfg.SealProofType,
		TaskTypes: localTasks_finalize,
	}, stor, lstor, si, -1, "NoUse", "NoUse"))

	err = m.AddWorker(ctx, NewLocalWorker(WorkerConfig{
		SealProof: cfg.SealProofType,
		TaskTypes: localTasks_addpiece,
	}, stor, lstor, si, -1, "NoUse", "NoUse"))

	err = m.AddWorker(ctx, NewLocalWorker(WorkerConfig{
		SealProof: cfg.SealProofType,
		TaskTypes: localTasks_fetch,
	}, stor, lstor, si, -1, "NoUse", "NoUse"))

	if err != nil {
		return nil, xerrors.Errorf("adding local worker: %w", err)
	}

	return m, nil
}

func (m *Manager) AddLocalStorage(ctx context.Context, path string) error {
	path, err := homedir.Expand(path)
	if err != nil {
		return xerrors.Errorf("expanding local path: %w", err)
	}

	if err := m.localStore.OpenPath(ctx, path); err != nil {
		return xerrors.Errorf("opening local path: %w", err)
	}

	if err := m.ls.SetStorage(func(sc *stores.StorageConfig) {
		sc.StoragePaths = append(sc.StoragePaths, stores.LocalPath{Path: path})
	}); err != nil {
		return xerrors.Errorf("get storage config: %w", err)
	}
	return nil
}

func (m *Manager) AddWorker(ctx context.Context, w Worker) error {
	info, err := w.Info(ctx)
	if err != nil {
		return xerrors.Errorf("getting worker info: %w", err)
	}

	// update task selector in sched queue
	// since the newExistingSelector now seeks worker everytime, the following code is not necessary

	/*
	my_tasktypes, err := w.TaskTypes(ctx)

	if err == nil {

		for sqi := 0; sqi < m.sched.schedQueue.Len(); sqi ++ {
			task := (*m.sched.schedQueue)[sqi]
			_, supported := my_tasktypes[task.taskType]

			if (task.taskType == sealtasks.TTPreCommit2 || task.taskType == sealtasks.TTCommit1) && (supported) {
				//log.Debug("DECENTRAL: AddWorker - new selector for task %d", task.sector.Number)
				//selector := newExistingSelector(m.index, task.sector, stores.FTCache|stores.FTSealed, true)
				selector, err := newExistingSelector(ctx, m.index, task.sector, stores.FTCache|stores.FTSealed, true)
				if err != nil {
					log.Debug("DECENTRAL: Cannot update task %d selector", task.sector.Number)
				}
				task.sel = selector
				//log.Debugf("DECENTRAL: AddWorker - task selector is %v", task.sel)
			}
		}
		log.Debugf("DECENTRAL: task updated and worker added")
	}

	*/

	m.sched.newWorkers <- &workerHandle{
		w: w,
		wt: &workTracker{
			running: map[uint64]storiface.WorkerJob{},
		},
		info:      info,
		preparing: &activeResources{},
		active:    &activeResources{},
	}

	return nil
}

func (m *Manager) AddMutualPath(ctx context.Context, groupsId int, path string) error {
	value, ok := m.sched.mutualPathMap[groupsId]
	if ok {
		if value != path {
			return xerrors.Errorf("groupsId exists, but value is not same. %s vs %s", value, path)
		}
		return nil
	}
	m.sched.mutualPathMap[groupsId] = path
	return nil
}

func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.remoteHnd.ServeHTTP(w, r)
}

func (m *Manager) SectorSize() abi.SectorSize {
	sz, _ := m.scfg.SealProofType.SectorSize()
	return sz
}

func schedNop(context.Context, Worker) error {
	return nil
}

func schedFetch(sector abi.SectorID, ft stores.SectorFileType, ptype stores.PathType, am stores.AcquireMode) func(context.Context, Worker) error {
	return func(ctx context.Context, worker Worker) error {
		return worker.Fetch(ctx, sector, ft, ptype, am)
	}
}

func (m *Manager) ReadPiece(ctx context.Context, sink io.Writer, sector abi.SectorID, offset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize, ticket abi.SealRandomness, unsealed cid.Cid) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTSealed|stores.FTCache, stores.FTUnsealed); err != nil {
		return xerrors.Errorf("acquiring sector lock: %w", err)
	}

	best, err := m.index.StorageFindSector(ctx, sector, stores.FTUnsealed, 0, false)
	if err != nil {
		return xerrors.Errorf("read piece: checking for already existing unsealed sector: %w", err)
	}

	var selector WorkerSelector
	if len(best) == 0 { // new
		selector = newAllocSelector(ctx, m.index, stores.FTUnsealed, stores.PathSealing)
	} else { // append to existing
		selector = newExistingSelector(m.index, sector, stores.FTUnsealed, false)
	}

	var readOk bool

	if len(best) > 0 {
		// There is unsealed sector, see if we can read from it

		selector = newExistingSelector(m.index, sector, stores.FTUnsealed, false)

		err = m.sched.Schedule(ctx, sector, sealtasks.TTReadUnsealed, selector, schedFetch(sector, stores.FTUnsealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
			readOk, err = w.ReadPiece(ctx, sink, sector, offset, size)
			return err
		})
		if err != nil {
			return xerrors.Errorf("reading piece from sealed sector: %w", err)
		}

		if readOk {
			return nil
		}
	}

	unsealFetch := func(ctx context.Context, worker Worker) error {
		if err := worker.Fetch(ctx, sector, stores.FTSealed|stores.FTCache, stores.PathSealing, stores.AcquireCopy); err != nil {
			return xerrors.Errorf("copy sealed/cache sector data: %w", err)
		}

		if len(best) > 0 {
			if err := worker.Fetch(ctx, sector, stores.FTUnsealed, stores.PathSealing, stores.AcquireMove); err != nil {
				return xerrors.Errorf("copy unsealed sector data: %w", err)
			}
		}
		return nil
	}

	err = m.sched.Schedule(ctx, sector, sealtasks.TTUnseal, selector, unsealFetch, func(ctx context.Context, w Worker) error {
		return w.UnsealPiece(ctx, sector, offset, size, ticket, unsealed)
	})
	if err != nil {
		return err
	}

	selector = newExistingSelector(m.index, sector, stores.FTUnsealed, false)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTReadUnsealed, selector, schedFetch(sector, stores.FTUnsealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
		readOk, err = w.ReadPiece(ctx, sink, sector, offset, size)
		return err
	})
	if err != nil {
		return xerrors.Errorf("reading piece from sealed sector: %w", err)
	}

	if readOk {
		return xerrors.Errorf("failed to read unsealed piece")
	}

	return nil
}

func (m *Manager) NewSector(ctx context.Context, sector abi.SectorID) error {
	log.Warnf("stub NewSector")
	return nil
}

func (m *Manager) AddPiece(ctx context.Context, sector abi.SectorID, existingPieces []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, r io.Reader) (abi.PieceInfo, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	log.Infof("DECENTRAL: manager add piece - storageLock")
	if err := m.index.StorageLock(ctx, sector, stores.FTNone, stores.FTUnsealed); err != nil {
		return abi.PieceInfo{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	var selector WorkerSelector
	var err error
	if len(existingPieces) == 0 { // new
		/*
		log.Infof("DECENTRAL: manager add piece - newAllocSelector")
		selector = newAllocSelector(m.index, stores.FTUnsealed, stores.PathSealing)
	} else { // use existing
		log.Infof("DECENTRAL: manager add piece - newExistingSelector")
		selector = newExistingSelector(m.index, sector, stores.FTUnsealed, false)
		*/
		selector = newAllocSelector(ctx, m.index, stores.FTUnsealed, stores.PathSealing)
	} else { // use existing
		selector = newExistingSelector(m.index, sector, stores.FTUnsealed, false)
	}
	if err != nil {
		return abi.PieceInfo{}, xerrors.Errorf("creating path selector: %w", err)
	}

	log.Infof("DECENTRAL: manager add piece - schedule")
	var out abi.PieceInfo
	err = m.sched.Schedule(ctx, sector, sealtasks.TTAddPiece, selector, schedNop, func(ctx context.Context, w Worker) error {
		p, err := w.AddPiece(ctx, sector, existingPieces, sz, r)
		if err != nil {
			return err
		}
		out = p
		return nil
	})
	log.Infof("DECENTRAL: manager add piece - scheduled ")

	return out, err
}

func (m *Manager) SealPreCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, pieces []abi.PieceInfo) (out storage.PreCommit1Out, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTUnsealed, stores.FTSealed|stores.FTCache); err != nil {
		return nil, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	// TODO: also consider where the unsealed data sits

	selector := newAllocSelector(ctx, m.index, stores.FTCache|stores.FTSealed, stores.PathSealing)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit1, selector, schedFetch(sector, stores.FTUnsealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
		info, err := w.Info(ctx)
		if err != nil {
			return xerrors.Errorf("get worker info: %w", err)
		}
		// m.sched.sectorGroupIds[sector] = info.WorkerGroupsId

		p, err := w.SealPreCommit1(ctx, sector, ticket, pieces)
		if err != nil {
			return err
		}

		if err := handleStoragePath(ctx, sector, w, stores.FTSealed); err != nil {
			return xerrors.Errorf("handleStoragePath %w", err)
		}
		if err := handleStoragePath(ctx, sector, w, stores.FTCache); err != nil {
			return xerrors.Errorf("handleStoragePath %w", err)
		}
		if info.MutualPath != "NoUse" {
			sectorGroupId := []byte(strconv.Itoa(info.WorkerGroupsId))
			idFile := filepath.Join(info.MutualPath, stores.FTCache.String(), stores.SectorName(sector), "sectorGroupId")
			if err := ioutil.WriteFile(idFile, sectorGroupId, 0777); err != nil {
				return xerrors.Errorf("save sectorGroupId to %s: %w", idFile, err)
			}
			log.Infof("Sector %d is assigned to Group %d", sector.Number, info.WorkerGroupsId)
		} else {
			sectorGroupId := []byte(strconv.Itoa(info.WorkerGroupsId))
			path, err := readPathJson()
			if err != nil {
				return err
			}
			idFile := filepath.Join(path.StorageRepoPath, stores.FTCache.String(), stores.SectorName(sector), "sectorGroupId")
			if err := ioutil.WriteFile(idFile, sectorGroupId, 0777); err != nil {
				return xerrors.Errorf("save sectorGroupId to %s: %w", idFile, err)
			}
			log.Warn("Miner is computing. Sector %d is assigned to Group %d", sector.Number, info.WorkerGroupsId)
		}

		out = p
		return nil
	})

	return out, err
}

func (m *Manager) SealPreCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage.PreCommit1Out) (out storage.SectorCids, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTSealed, stores.FTCache); err != nil {
		return storage.SectorCids{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	selector := newExistingSelector(m.index, sector, stores.FTCache|stores.FTSealed, true)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit2, selector, schedFetch(sector, stores.FTCache|stores.FTSealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
		p, err := w.SealPreCommit2(ctx, sector, phase1Out)
		if err != nil {
			return err
		}

		if err := handleStoragePath(ctx, sector, w, stores.FTSealed); err != nil {
			return xerrors.Errorf("handleStoragePath %w", err)
		}
		if err := handleStoragePath(ctx, sector, w, stores.FTCache); err != nil {
			return xerrors.Errorf("handleStoragePath %w", err)
		}

		out = p
		return nil
	})
	return out, err
}

func (m *Manager) SealCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (out storage.Commit1Out, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTSealed, stores.FTCache); err != nil {
		return storage.Commit1Out{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	// NOTE: We set allowFetch to false in so that we always execute on a worker
	// with direct access to the data. We want to do that because this step is
	// generally very cheap / fast, and transferring data is not worth the effort
	selector := newExistingSelector(m.index, sector, stores.FTCache|stores.FTSealed, false)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTCommit1, selector, schedFetch(sector, stores.FTCache|stores.FTSealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
		p, err := w.SealCommit1(ctx, sector, ticket, seed, pieces, cids)
		if err != nil {
			return err
		}

		if err := handleStoragePath(ctx, sector, w, stores.FTSealed); err != nil {
			return xerrors.Errorf("handleStoragePath %w", err)
		}
		if err := handleStoragePath(ctx, sector, w, stores.FTCache); err != nil {
			return xerrors.Errorf("handleStoragePath %w", err)
		}

		out = p
		return nil
	})
	return out, err
}

func (m *Manager) SealCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage.Commit1Out) (out storage.Proof, err error) {
	selector := newTaskSelector()

	err = m.sched.Schedule(ctx, sector, sealtasks.TTCommit2, selector, schedNop, func(ctx context.Context, w Worker) error {
		p, err := w.SealCommit2(ctx, sector, phase1Out)
		if err != nil {
			return err
		}

		if err := handleStoragePath(ctx, sector, w, stores.FTSealed); err != nil {
			return xerrors.Errorf("handleStoragePath %w", err)
		}
		if err := handleStoragePath(ctx, sector, w, stores.FTCache); err != nil {
			return xerrors.Errorf("handleStoragePath %w", err)
		}

		out = p
		return nil
	})

	return out, err
}

func (m *Manager) FinalizeSector(ctx context.Context, sector abi.SectorID, keepUnsealed []storage.Range) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	log.Infof("DECENTRAL: manager finalize sector - storageLock")
	if err := m.index.StorageLock(ctx, sector, stores.FTNone, stores.FTSealed|stores.FTUnsealed|stores.FTCache); err != nil {
		return xerrors.Errorf("acquiring sector lock: %w", err)
	}

	unsealed := stores.FTUnsealed
	{
		log.Infof("DECENTRAL: manager finalize sector - storage find sector")
		unsealedStores, err := m.index.StorageFindSector(ctx, sector, stores.FTUnsealed, 0, false)
		if err != nil {
			return xerrors.Errorf("finding unsealed sector: %w", err)
		}

		if len(unsealedStores) == 0 { // Is some edge-cases unsealed sector may not exist already, that's fine
			unsealed = stores.FTNone
		}
	}

	/*
	log.Infof("DECENTRAL: manager finalize sector - newExistingSelector")
	selector := newExistingSelector(m.index, sector, stores.FTCache|stores.FTSealed, false)

	log.Infof("DECENTRAL: manager finalize sector - schedule finalize")
	err := m.sched.Schedule(ctx, sector, sealtasks.TTFinalize, selector,
	*/
	selector := newExistingSelector(m.index, sector, stores.FTCache|stores.FTSealed, false)

	err := m.sched.Schedule(ctx, sector, sealtasks.TTFinalize, selector,
		schedFetch(sector, stores.FTCache|stores.FTSealed|unsealed, stores.PathSealing, stores.AcquireMove),
		func(ctx context.Context, w Worker) error {
			return w.FinalizeSector(ctx, sector, keepUnsealed)
		})
	if err != nil {
		return err
	}

	log.Infof("DECENTRAL: manager finalize sector - finalize scheduled ")
	/*
	log.Infof("DECENTRAL: manager finalize sector - new alloc selector")
	fetchSel := newAllocSelector(m.index, stores.FTCache|stores.FTSealed, stores.PathStorage)
	*/
	fetchSel := newAllocSelector(ctx, m.index, stores.FTCache|stores.FTSealed, stores.PathStorage)
	moveUnsealed := unsealed
	{
		if len(keepUnsealed) == 0 {
			moveUnsealed = stores.FTNone
		}
	}

	log.Infof("DECENTRAL: manager finalize sector - new schedule fetch")
	err = m.sched.Schedule(ctx, sector, sealtasks.TTFetch, fetchSel,
		schedFetch(sector, stores.FTCache|stores.FTSealed|moveUnsealed, stores.PathStorage, stores.AcquireMove),
		func(ctx context.Context, w Worker) error {
			return w.MoveStorage(ctx, sector)
		})
	if err != nil {
		return xerrors.Errorf("moving sector to storage: %w", err)
	}
	log.Infof("DECENTRAL: manager finalize sector - fetch scheduled ")

	return nil
}

func (m *Manager) ReleaseUnsealed(ctx context.Context, sector abi.SectorID, safeToFree []storage.Range) error {
	log.Warnw("ReleaseUnsealed todo")
	return nil
}

func (m *Manager) Remove(ctx context.Context, sector abi.SectorID) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	log.Infof("DECENTRAL: manager remove - storageLock")
	if err := m.index.StorageLock(ctx, sector, stores.FTNone, stores.FTSealed|stores.FTUnsealed|stores.FTCache); err != nil {
		return xerrors.Errorf("acquiring sector lock: %w", err)
	}

	unsealed := stores.FTUnsealed
	{
		log.Infof("DECENTRAL: manager remove - storageFindSector")
		unsealedStores, err := m.index.StorageFindSector(ctx, sector, stores.FTUnsealed, 0, false)
		if err != nil {
			return xerrors.Errorf("finding unsealed sector: %w", err)
		}

		if len(unsealedStores) == 0 { // can be already removed
			unsealed = stores.FTNone
		}
	}

	selector := newExistingSelector(m.index, sector, stores.FTCache|stores.FTSealed, false)

	log.Infof("DECENTRAL: manager remove - schedule finalize")
	return m.sched.Schedule(ctx, sector, sealtasks.TTFinalize, selector,
		schedFetch(sector, stores.FTCache|stores.FTSealed|unsealed, stores.PathStorage, stores.AcquireMove),
		func(ctx context.Context, w Worker) error {
			return w.Remove(ctx, sector)
		})
}

func (m *Manager) StorageLocal(ctx context.Context) (map[stores.ID]string, error) {
	l, err := m.localStore.Local(ctx)
	if err != nil {
		return nil, err
	}

	out := map[stores.ID]string{}
	for _, st := range l {
		out[st.ID] = st.LocalPath
	}

	return out, nil
}

func (m *Manager) FsStat(ctx context.Context, id stores.ID) (fsutil.FsStat, error) {
	return m.storage.FsStat(ctx, id)
}

func (m *Manager) SchedDiag(ctx context.Context) (interface{}, error) {
	return m.sched.Info(ctx)
}

func (m *Manager) Close(ctx context.Context) error {
	return m.sched.Close(ctx)
}

var _ SectorManager = &Manager{}

func handleStoragePath(ctx context.Context, sector abi.SectorID, w Worker, sectorFileType stores.SectorFileType) error {
	info, err := w.Info(ctx)
	if err != nil {
		return err
	}

	if info.MutualPath == "NoUse" {
		return nil
	}

	path, err := readPathJson()
	if err != nil {
		return err
	}

	actualPath := filepath.Join(info.MutualPath, sectorFileType.String(), stores.SectorName(sector))
	symlinkPath := filepath.Join(path.StorageRepoPath, sectorFileType.String(), stores.SectorName(sector))

	if _, err := os.Stat(actualPath); err != nil {
		return xerrors.Errorf("os.Stat actualPath %s, %w", actualPath, err)
	}

	_, err = os.Stat(symlinkPath)
	if err != nil && !os.IsNotExist(err) {
		return xerrors.Errorf("os.Stat symlinkPath %s, %w", symlinkPath, err)
	}

	if err != nil && os.IsNotExist(err) {
		if err := os.Symlink(actualPath, symlinkPath); err != nil {
			return xerrors.Errorf("os.Symlink symlinkPath %s to actualPath %s", symlinkPath, actualPath, err)
		}
		return nil
	}

	if p, err := os.Readlink(symlinkPath); err != nil || p != actualPath {
		if err := os.Remove(symlinkPath); err != nil {
			return xerrors.Errorf("os.Remove symlinkPath %s", symlinkPath, err)
		}
		if err := os.Symlink(actualPath, symlinkPath); err != nil {
			return xerrors.Errorf("os.Symlink symlinkPath %s to actualPath %s", symlinkPath, actualPath, err)
		}
	}

	return nil
}

func readPathJson() (pathConfig, error) {
	pathFile, err := homedir.Expand("~/pathConfig.json")
	if err != nil {
		log.Errorf("%+v", err)
		return pathConfig{}, err
	}

	pathData, err := os.Open(pathFile)
	if err != nil {
		log.Errorf("%+v", err)
		return pathConfig{}, err
	}
	defer pathData.Close()

	data := make([]byte, 2000)
	n, err := pathData.Read(data)
	if err != nil {
		log.Errorf("%+v", err)
		return pathConfig{}, err
	}

	var path pathConfig
	if err := json.Unmarshal(data[:n], &path); err != nil {
		log.Errorf("%+v", err)
		return pathConfig{}, err
	}

	return path, nil
}

/*
func (m *Manager) ReturnIndex() stores.SectorIndex {

	return m.index
}
*/

type pathConfig struct {
	MinerId         string
	StorageRepoPath string
}
