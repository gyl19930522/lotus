package sealing

import (
	"context"

	"golang.org/x/xerrors"

	"encoding/json"
	"github.com/filecoin-project/sector-storage/stores"
	"github.com/filecoin-project/specs-actors/actors/abi"
	//nr "github.com/filecoin-project/storage-fsm/lib/nullreader"
	"github.com/mitchellh/go-homedir"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"fmt"
)

func (m *Sealing) pledgeSector(ctx context.Context, sectorID abi.SectorID, existingPieceSizes []abi.UnpaddedPieceSize, sizes ...abi.UnpaddedPieceSize) ([]abi.PieceInfo, error) {
	if len(sizes) == 0 {
		return nil, nil
	}

	out := make([]abi.PieceInfo, len(sizes))
	for i, size := range sizes {
		//ppi, err := m.sealer.AddPiece(ctx, sectorID, existingPieceSizes, size, m.pledgeReader(size))
		ppi, err := m.sealer.AddPiece(ctx, sectorID, existingPieceSizes, size, NewNullReader(size))
		if err != nil {
			return nil, xerrors.Errorf("add piece: %w", err)
		}

		existingPieceSizes = append(existingPieceSizes, size)

		out[i] = ppi
	}

	return out, nil
}

func (m *Sealing) PledgeSector() error {
	cfg, err := m.getConfig()
	if err != nil {
		return xerrors.Errorf("getting config: %w", err)
	}

	if cfg.MaxSealingSectors > 0 {
		if m.stats.curSealing() > cfg.MaxSealingSectors {
			return xerrors.Errorf("too many sectors sealing (curSealing: %d, max: %d)", m.stats.curSealing(), cfg.MaxSealingSectors)
		}
	}

	go func() {
		ctx := context.TODO() // we can't use the context from command which invokes
		// this, as we run everything here async, and it's cancelled when the
		// command exits

		size := abi.PaddedPieceSize(m.sealer.SectorSize()).Unpadded()

		sid, err := m.sc.Next()
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		// DECENTRAL: I modified NewSector and implement AcquireSector in NewSector
		err = m.sealer.NewSector(ctx, m.minerSector(sid))
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		//pieces, err := m.pledgeSector(ctx, m.minerSector(sid), []abi.UnpaddedPieceSize{}, size)
		log.Infof("DECENTRAL: calling readPiecesJson")
		pieces, err := m.readPiecesJson(ctx, m.minerSector(sid), size)
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		ps := make([]Piece, len(pieces))
		for idx := range ps {
			ps[idx] = Piece{
				Piece:    pieces[idx],
				DealInfo: nil,
			}
		}

		log.Infof("DECENTRAL: calling newSectorCC")
		if err := m.newSectorCC(sid, ps); err != nil {
			log.Errorf("%+v", err)
			return
		}
	}()
	return nil
}

func (m *Sealing) MutualSector(storageRepoPath string) error {
	go func() {
		ctx := context.TODO() // we can't use the context from command which invokes
		// this, as we run everything here async, and it's cancelled when the
		// command exits

		size := abi.PaddedPieceSize(m.sealer.SectorSize()).Unpadded()

		sid := abi.SectorNumber(uint64(math.MaxUint64))

		pieces, err := m.pledgeSector(ctx, m.minerSector(sid), []abi.UnpaddedPieceSize{}, size)
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		p, err := homedir.Expand(storageRepoPath)
		if err != nil {
			xerrors.Errorf("could not expand home dir (%s): %w", storageRepoPath, err)
			return
		}

		stagedPath_ := filepath.Join(p, stores.FTUnsealed.String(), stores.SectorName(m.minerSector(sid)))
		stagedPath := filepath.Join(p, "mutual-sector")

		if err := os.Rename(stagedPath_, stagedPath); err != nil {
			log.Errorf("%+v", err)
			return
		}

		localPath, err := homedir.Expand("~/lotus_local_data")
		if err != nil {
			xerrors.Errorf("could not expand home dir (%s): %w", "~/lotus_local_data", err)
			return
		}
		if err := os.MkdirAll(localPath, 0777); err != nil && !os.IsExist(err) {
			xerrors.Errorf("mkdir '%s': %w", localPath, err)
			return
		}
		localStagedPath := filepath.Join(localPath, "mutual-sector")
		cmd := exec.Command("cp", "-rf", stagedPath, localStagedPath)
		log.Infof("copping staged sector: cp -rf %s %s", stagedPath, localStagedPath)
		if err := cmd.Run(); err != nil {
			xerrors.Errorf("copy staged sector: %w", err)
			return
		}

		data, err := json.Marshal(pieces[0])
		if err != nil {
			log.Errorf("%+v", err)
			log.Errorf("%+v", err)
			return
		}
		sectorInfoPath := filepath.Join(p, "sectorInfo.json")
		if err := saveJson(data, sectorInfoPath); err != nil {
			log.Errorf("%+v", err)
			return
		}

		pathJson := pathConfig{
			//MinerId:         m.Address().String(),
			StorageRepoPath: p,
		}

		data_, err := json.Marshal(pathJson)
		if err != nil {
			log.Errorf("%+v", err)
			return
		}
		pathFile, err := homedir.Expand("~/pathConfig.json")
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		if err := saveJson(data_, pathFile); err != nil {
			log.Errorf("%+v", err)
			return
		}
	}()
	return nil
}

func (m *Sealing) readPiecesJson(ctx context.Context, sectorID abi.SectorID, size abi.UnpaddedPieceSize) ([]abi.PieceInfo, error) {
	path, err := readPathJson()
	if err != nil {
		log.Errorf("%+v", err)
		return nil, err
	}
	
	unsealPath := filepath.Join(path.StorageRepoPath, stores.FTUnsealed.String(), stores.SectorName(sectorID))

	localPath, err := homedir.Expand("~/lotus_local_data")
	if err != nil {
		return nil, err
	}
	localStagedPath := localPath + "/mutual-sector"
	if _, err := os.Stat(localStagedPath); err != nil {
		return nil, err
	}

	if err := os.Symlink(localStagedPath, unsealPath); err != nil {
		log.Errorf("%+v", err)
		return nil, err
	}

	pieceSize, ok := os.LookupEnv("LOTUS_MUTUAL_PIECE_SIZE")
	if !ok {
		return nil, xerrors.New("LOTUS_MUTUAL_PIECE_SIZE is not set env")
	}
	pieceCid, ok := os.LookupEnv("LOTUS_MUTUAL_PIECE_CID")
	if !ok {
		return nil, xerrors.New("LOTUS_MUTUAL_PIECE_CID is not set env")
	}	
	var piece abi.PieceInfo
	if err := json.Unmarshal([]byte(fmt.Sprintf(`{"Size":%s,"PieceCID":{"/":"%s"}}`, pieceSize, pieceCid)), &piece); err != nil {
		return nil, err
	}

	return []abi.PieceInfo{piece}, nil
}

func readPathJson() (pathConfig, error) {
	storageRepoPath, ok := os.LookupEnv("LOTUS_STORAGE_REPO_PATH")
	if !ok {
		return pathConfig{}, xerrors.New("LOTUS_STORAGE_REPO_PATH is not set env")
	}
	return pathConfig{StorageRepoPath: storageRepoPath}, nil
}

func saveJson(data []byte, path string) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Warnf("pathConfig.json ile d failed: %w", err)
		}
	}()

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	return nil
}

type pathConfig struct {
	//MinerId         string
	StorageRepoPath string
}
