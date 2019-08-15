package log

import (
	"testing"

	"github.com/zssky/log"
)

func TestBinlogWriter_WriteDDL(t *testing.T) {
	f := uint16(1)
	v := f | RowEventNoForeignKeyChecks
	log.Info(v)
}
