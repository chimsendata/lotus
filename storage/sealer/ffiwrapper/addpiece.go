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

func (sb *Sealer) MakeUnsealed(pieceCID cid.Cid, srcPath string) error {
	dir, _ := path.Split(srcPath)
	dir = path.Join(dir, "../")
	dir = path.Join(dir, "../")
	dir = path.Join(dir, "addpiece")

	srcFile, err := os.OpenFile(srcPath, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}
	dstFile, err := os.OpenFile(path.Join(dir, "unsealed"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
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

	return ioutil.WriteFile(path.Join(dir, "cid"), text, 0666)
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
	dir, _ := path.Split(stagedPath.Unsealed)
	dir = path.Join(dir, "../")
	dir = path.Join(dir, "../")
	dir = path.Join(dir, "addpiece")

	bs, err := os.ReadFile(path.Join(dir, "cid"))
	if err != nil {
		return abi.PieceInfo{}, err
	}

	var id cid.Cid
	if err = id.UnmarshalText(bs); err != nil {
		return abi.PieceInfo{}, err
	}

	srcFile, err := os.OpenFile(path.Join(dir, "unsealed"), os.O_RDONLY, 0666)
	if err != nil {
		return abi.PieceInfo{}, err
	}
	dstFile, err := os.OpenFile(stagedPath.Unsealed, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return abi.PieceInfo{}, err
	}
	defer srcFile.Close()
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)

	return abi.PieceInfo{
		Size:     pieceSize.Padded(),
		PieceCID: id,
	}, err
}
