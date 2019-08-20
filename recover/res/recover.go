package res

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/zssky/log"

	"github.com/mysql-binlog/common/meta"
)

// RecoverMode
type RecoverMode string

const (
	separated RecoverMode = "separated"

	// integer means all binlog file into one whole style
	integer RecoverMode = "integer"
)

// Recover for tables or integer
type Recover interface {
	ID() string
	Recover()
	ExecutedGTID() string
}

// Recovering
func Recovering(mode RecoverMode, tbs []string, clusterPath string, time int64, ctx context.Context, o *meta.Offset, user, pass string, port int, errs chan error) ([]Recover, error) {
	switch mode {
	case separated:
		cwg := &sync.WaitGroup{}
		co, err := NewCoordinator(user, pass, port, time, o, clusterPath, tbs, cwg, ctx, errs)
		if err != nil {
			log.Errorf("create coordinator error {%v}", err)
		}

		// init wait group
		size := len(tbs)
		wg := &sync.WaitGroup{}
		wg.Add(size)

		var trs []Recover
		for _, tb := range tbs {
			tr, err := NewTable(tb, clusterPath, time, ctx, o, user, pass, port, wg, errs, co.SyncCh)
			if err != nil {
				// error occur then exit
				os.Exit(1)
			}

			go tr.Recover()

			trs = append(trs, tr)

			log.Infof("start table {%s} recover ", tr.ID())
		}

		log.Infof("wait for all recorded table to finish")
		wg.Wait()

		// 在 wg 未结束之前 cwg 还不到wait 不存在wait panic的状态
		log.Infof("wait for coordinate table to recover")
		cwg.Wait()

		return trs, nil
	case integer:
		wg := &sync.WaitGroup{}
		wg.Add(1)

		it, err := NewInteger(clusterPath, time, ctx, o, user, pass, port, wg, errs)
		if err != nil {
			os.Exit(1)
		}

		go it.Recover()

		log.Infof("start table {%s} recover ", it.ID())
		wg.Wait()
		return []Recover{it}, nil
	}
	panic(fmt.Errorf("recover mode %s no supported {integer, separated}", mode))
}
