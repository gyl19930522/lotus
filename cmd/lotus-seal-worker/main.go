package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
//	"syscall"
//	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"
	paramfetch "github.com/filecoin-project/go-paramfetch"
	"github.com/filecoin-project/sector-storage/ffiwrapper"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/apistruct"
	"github.com/filecoin-project/lotus/build"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/lotuslog"
	"github.com/filecoin-project/lotus/node/repo"
	sectorstorage "github.com/filecoin-project/sector-storage"
	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/filecoin-project/sector-storage/stores"
	"github.com/mitchellh/go-homedir"
)

var log = logging.Logger("main")

const FlagWorkerRepo = "worker-repo"

// TODO remove after deprecation period
const FlagWorkerRepoDeprecation = "workerrepo"

func main() {
	lotuslog.SetupLogLevels()

	log.Info("Starting lotus worker")

	local := []*cli.Command{
		runCmd,
	}

	app := &cli.App{
		Name:    "lotus-worker",
		Usage:   "Remote miner worker",
		Version: build.UserVersion(),
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    FlagWorkerRepo,
				Aliases: []string{FlagWorkerRepoDeprecation},
				EnvVars: []string{"LOTUS_WORKER_PATH", "WORKER_PATH"},
				Value:   "~/.lotusworker", // TODO: Consider XDG_DATA_HOME
				Usage:   fmt.Sprintf("Specify worker repo path. flag %s and env WORKER_PATH are DEPRECATION, will REMOVE SOON", FlagWorkerRepoDeprecation),
			},
			&cli.StringFlag{
				Name:    "miner-repo",
				Aliases: []string{"storagerepo"},
				EnvVars: []string{"LOTUS_MINER_PATH", "LOTUS_STORAGE_PATH"},
				Value:   "~/.lotusminer", // TODO: Consider XDG_DATA_HOME
				Usage:   fmt.Sprintf("Specify miner repo path. flag storagerepo and env LOTUS_STORAGE_PATH are DEPRECATION, will REMOVE SOON"),
			},
			&cli.BoolFlag{
				Name:  "enable-gpu-proving",
				Usage: "enable use of GPU for mining operations",
				Value: true,
			},
			&cli.StringFlag{
				Name:  "mutualpath",
				Usage: "mutual path for miner and workers",
			},
			&cli.StringFlag{
				Name:  "workerGroupsId",
				Usage: "worker Groups Id",
			},
			&cli.StringFlag{
				Name:  "minerActualRepoPath",
				Usage: "actual storagerepo of miner",
			},
		},

		Commands: local,
	}
	app.Setup()
	app.Metadata["repoType"] = repo.Worker

	if err := app.Run(os.Args); err != nil {
		log.Warnf("%+v", err)
		return
	}
}

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start lotus worker",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "address",
			Usage: "Locally reachable address",
		},
		&cli.BoolFlag{
			Name:  "no-local-storage",
			Usage: "don't use storageminer repo for sector storage",
		},
		&cli.BoolFlag{
			Name:  "precommit1",
			Usage: "enable precommit1 (32G sectors: 1 core, 128GiB Memory)",
			Value: true,
		},
		&cli.BoolFlag{
			Name:  "precommit2",
			Usage: "enable precommit2 (32G sectors: all cores, 96GiB Memory)",
			Value: true,
		},
		&cli.BoolFlag{
			Name:  "commit",
			Usage: "enable commit (32G sectors: all cores or GPUs, 128GiB Memory + 64GiB swap)",
			Value: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		if !cctx.Bool("enable-gpu-proving") {
			if err := os.Setenv("BELLMAN_NO_GPU", "true"); err != nil {
				return xerrors.Errorf("could not set no-gpu env: %+v", err)
			}
		}

		if cctx.String("address") == "" {
			return xerrors.Errorf("--address flag is required")
		}

		if cctx.String("mutualpath") == "" {
			return xerrors.Errorf("--mutualpath is required")
		}

		if cctx.String("workerGroupsId") == "" {
			return xerrors.Errorf("--workerGroupsId is required")
		}

		if cctx.String("minerActualRepoPath") == "" {
			return xerrors.Errorf("--minerActualRepoPath is required")
		}

		// Connect to storage-miner

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return xerrors.Errorf("getting miner api: %w", err)
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		v, err := nodeApi.Version(ctx)
		if err != nil {
			return err
		}
		if v.APIVersion != build.APIVersion {
			return xerrors.Errorf("lotus-miner API version doesn't match: local: ", api.Version{APIVersion: build.APIVersion})
		}
		log.Infof("Remote version %s", v)

		// Check params

		act, err := nodeApi.ActorAddress(ctx)
		if err != nil {
			return err
		}
		ssize, err := nodeApi.ActorSectorSize(ctx, act)
		if err != nil {
			return err
		}

		if cctx.Bool("commit") {
			if err := paramfetch.GetParams(ctx, build.ParametersJSON(), uint64(ssize)); err != nil {
				return xerrors.Errorf("get params: %w", err)
			}
		}

		var taskTypes []sealtasks.TaskType

		taskTypes = append(taskTypes, sealtasks.TTFetch, sealtasks.TTCommit1, sealtasks.TTFinalize)

		if cctx.Bool("precommit1") {
			taskTypes = append(taskTypes, sealtasks.TTPreCommit1)
		}
		if cctx.Bool("precommit2") {
			taskTypes = append(taskTypes, sealtasks.TTPreCommit2)
		}
		if cctx.Bool("commit") {
			taskTypes = append(taskTypes, sealtasks.TTCommit2)
		}

		if len(taskTypes) == 0 {
			return xerrors.Errorf("no task types specified")
		}

		// Open repo

		repoPath := cctx.String(FlagWorkerRepo)
		r, err := repo.NewFS(repoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			if err := r.Init(repo.Worker); err != nil {
				return err
			}

			lr, err := r.Lock(repo.Worker)
			if err != nil {
				return err
			}

			var localPaths []stores.LocalPath

			if !cctx.Bool("no-local-storage") {
				b, err := json.MarshalIndent(&stores.LocalStorageMeta{
					ID:       stores.ID(uuid.New().String()),
					Weight:   10,
					CanSeal:  true,
					CanStore: false,
				}, "", "  ")
				if err != nil {
					return xerrors.Errorf("marshaling storage config: %w", err)
				}

				if err := ioutil.WriteFile(filepath.Join(lr.Path(), "sectorstore.json"), b, 0644); err != nil {
					return xerrors.Errorf("persisting storage metadata (%s): %w", filepath.Join(lr.Path(), "sectorstore.json"), err)
				}

				localPaths = append(localPaths, stores.LocalPath{
					Path: lr.Path(),
				})
			}

			if err := lr.SetStorage(func(sc *stores.StorageConfig) {
				sc.StoragePaths = append(sc.StoragePaths, localPaths...)
			}); err != nil {
				return xerrors.Errorf("set storage config: %w", err)
			}

			{
				// init datastore for r.Exists
				_, err := lr.Datastore("/metadata")
				if err != nil {
					return err
				}
			}
			if err := lr.Close(); err != nil {
				return xerrors.Errorf("close repo: %w", err)
			}
		}

		lr, err := r.Lock(repo.Worker)
		if err != nil {
			return err
		}

		log.Info("Opening local storage; connecting to master")

		localStore, err := stores.NewLocal(ctx, lr, nodeApi, []string{"http://" + cctx.String("address") + "/remote"})
		if err != nil {
			return err
		}

		p, err := homedir.Expand(repoPath)
		if err != nil {
			xerrors.Errorf("could not expand home dir (%s): %w", repoPath, err)
		}

		workerGroupsId, err := strconv.Atoi(cctx.String("workerGroupsId"))
		if err != nil {
			return err
		}

		mutualPath := cctx.String("mutualpath")
		if _, err := os.Stat(mutualPath); err != nil {
			return err
		}

		if err := nodeApi.AddMutualPath(ctx, workerGroupsId, mutualPath); err != nil {
			return err
		}

		mutualUnsealPath := mutualPath + "/" + stores.FTUnsealed.String()
		if _, err := os.Stat(mutualUnsealPath); err != nil {
			if !os.IsNotExist(err) {
				return nil
			}
			if err := os.MkdirAll(mutualUnsealPath, 0777); err != nil {
				return err
			}
		}
		unsealPath := filepath.Join(p, stores.FTUnsealed.String())
		if err := os.Remove(unsealPath); err != nil && !os.IsNotExist(err) {
			return xerrors.Errorf("remove '%s': %w", unsealPath, err)
		}
		if err := os.Symlink(mutualUnsealPath, unsealPath); err != nil {
			return xerrors.Errorf("symlink '%s' to '%s': %w", unsealPath, mutualUnsealPath, err)
		}

		mutualSealedPath := mutualPath + "/" + stores.FTSealed.String()
		if _, err := os.Stat(mutualSealedPath); err != nil {
			if !os.IsNotExist(err) {
				return nil
			}
			if err := os.MkdirAll(mutualSealedPath, 0777); err != nil {
				return err
			}
		}
		sealedPath := filepath.Join(p, stores.FTSealed.String())
		if err := os.Remove(sealedPath); err != nil && !os.IsNotExist(err) {
			return xerrors.Errorf("remove '%s': %w", sealedPath, err)
		}
		if err := os.Symlink(mutualSealedPath, sealedPath); err != nil {
			return xerrors.Errorf("symlink '%s' to '%s': %w", sealedPath, mutualSealedPath, err)
		}

		mutualCachePath := mutualPath + "/" + stores.FTCache.String()
		if _, err := os.Stat(mutualCachePath); err != nil {
			if !os.IsNotExist(err) {
				return nil
			}
			if err := os.MkdirAll(mutualCachePath, 0777); err != nil {
				return err
			}
		}
		cachePath := filepath.Join(p, stores.FTCache.String())
		if err := os.Remove(cachePath); err != nil && !os.IsNotExist(err) {
			return xerrors.Errorf("remove '%s': %w", cachePath, err)
		}
		if err := os.Symlink(mutualCachePath, cachePath); err != nil {
			return xerrors.Errorf("symlink '%s' to '%s': %w", cachePath, mutualCachePath, err)
		}

		mutualSectorPath := filepath.Join(cctx.String("minerActualRepoPath"), "mutual-sector")
		if _, err := os.Stat(mutualSectorPath); err == nil {
			localPath, err := homedir.Expand("~/lotus_local_data")
			if err != nil {
				return err
			}
			localStagedPath := filepath.Join(localPath, "/mutual-sector")
			if _, err := os.Stat(localStagedPath); err != nil {
				if !os.IsNotExist(err) {
					return xerrors.Errorf("stat mutual sector: %w", err)
				}
				cmd := exec.Command("cp", "-rf", mutualSectorPath, localStagedPath)
				log.Infof("copping staged sector: cp -rf %s %s", mutualSectorPath, localStagedPath)
				if err := cmd.Run(); err != nil {
					return xerrors.Errorf("copy staged sector: %w", err)
				}
			}
		}

		// Setup remote sector store
		spt, err := ffiwrapper.SealProofTypeFromSectorSize(ssize)
		if err != nil {
			return xerrors.Errorf("getting proof type: %w", err)
		}

		sminfo, err := lcli.GetAPIInfo(cctx, repo.StorageMiner)
		if err != nil {
			return xerrors.Errorf("could not get api info: %w", err)
		}

		remote := stores.NewRemote(localStore, nodeApi, sminfo.AuthHeader())

		// Create / expose the worker

		workerApi := &worker{
			LocalWorker: sectorstorage.NewLocalWorker(sectorstorage.WorkerConfig{
				SealProof: spt,
				TaskTypes: taskTypes,
			}, remote, localStore, nodeApi, workerGroupsId, mutualPath, mutualSectorPath),
		}

		mux := mux.NewRouter()

		log.Info("Setting up control endpoint at " + cctx.String("address"))

		rpcServer := jsonrpc.NewServer()
		rpcServer.Register("Filecoin", apistruct.PermissionedWorkerAPI(workerApi))

		mux.Handle("/rpc/v0", rpcServer)
		mux.PathPrefix("/remote").HandlerFunc((&stores.FetchHandler{Local: localStore}).ServeHTTP)
		mux.PathPrefix("/").Handler(http.DefaultServeMux) // pprof

		ah := &auth.Handler{
			Verify: nodeApi.AuthVerify,
			Next:   mux.ServeHTTP,
		}

		srv := &http.Server{
			Handler: ah,
			BaseContext: func(listener net.Listener) context.Context {
				return ctx
			},
		}

		go func() {
			<-ctx.Done()
			log.Warn("Shutting down...")
			if err := srv.Shutdown(context.TODO()); err != nil {
				log.Errorf("shutting down RPC server failed: %s", err)
			}
			log.Warn("Graceful shutdown successful")
		}()

		nl, err := net.Listen("tcp", cctx.String("address"))
		if err != nil {
			return err
		}

		log.Info("Waiting for tasks")

		go func() {
			if err := nodeApi.WorkerConnect(ctx, "ws://"+cctx.String("address")+"/rpc/v0"); err != nil {
				log.Errorf("Registering worker failed: %+v", err)
				cancel()
				return
			}
		}()

		return srv.Serve(nl)
	},
}
