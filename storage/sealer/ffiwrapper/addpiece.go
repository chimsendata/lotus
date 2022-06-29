package ffiwrapper

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/ipfs/go-cid"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

var once sync.Once

func (sb Sealer) GetCCUnsealedPath() (string, string) {
	datapath, isExist := os.LookupEnv("FFI_PIECE_PATH")
	if !isExist {
		panic("FFI_PIECE_X env not exist")
	}
	if datapath == "" {
		panic("FFI_PIECE_X path is empty")
	}
	return path.Join(datapath, "data"), path.Join(datapath, "cid")
}

func (sb *Sealer) CCUnsealedIsExist() (bool, error) {
	data, cid := sb.GetCCUnsealedPath()
	_, err := os.Stat(data)
	_, err = os.Stat(cid)
	if err == nil {
		return true, nil
	}
	err = os.Remove(data)
	err = os.Remove(cid)
	return false, err
}

func (sb *Sealer) MakeUnsealed(pieceCID cid.Cid, srcPath string) error {

	unsealedIsExist, err := sb.CCUnsealedIsExist()
	if err != nil {
		return err
	}

	if unsealedIsExist {
		return nil
	}

	data, pcid := sb.GetCCUnsealedPath()

	srcFile, err := os.OpenFile(srcPath, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}
	dstFile, err := os.OpenFile(data, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	defer dstFile.Close()

	if _, err = io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	text, err := pieceCID.MarshalText()
	if err != nil {
		return err
	}

	return ioutil.WriteFile(pcid, text, 0666)
}

func (sb *Sealer) GetUnsealed(ctx context.Context, sector storiface.SectorRef, pieceSize abi.UnpaddedPieceSize) (abi.PieceInfo, error) {
	var done func()
	defer func() {
		if done != nil {
			done()
		}
	}()
	stagedPath, done, err := sb.sectors.AcquireSector(ctx, sector, 0, storiface.FTUnsealed, storiface.PathSealing)
	if err != nil {
		return abi.PieceInfo{}, err
	}

	data, pcid := sb.GetCCUnsealedPath()

	bs, err := os.ReadFile(pcid)
	if err != nil {
		return abi.PieceInfo{}, err
	}

	var id cid.Cid
	if err = id.UnmarshalText(bs); err != nil {
		return abi.PieceInfo{}, err
	}

	srcFile, err := os.OpenFile(data, os.O_RDONLY, 0666)
	if err != nil {
		return abi.PieceInfo{}, err
	}
	dstFile, err := os.OpenFile(stagedPath.Unsealed, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return abi.PieceInfo{}, err
	}
	defer srcFile.Close()
	defer dstFile.Close()

	if _, err = io.Copy(dstFile, srcFile); err != nil {
		return abi.PieceInfo{}, err
	}

	return abi.PieceInfo{
		Size:     pieceSize.Padded(),
		PieceCID: id,
	}, err
}
