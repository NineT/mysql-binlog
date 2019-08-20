package conf

import (
	"testing"

	"github.com/zssky/log"
)

func TestIsCoordinateSQL(t *testing.T) {
	sql := []byte("drop database pengan")

	b, reg, err := IsCoordinateSQL(sql)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("is coordinate sql ? %v, table regular is %s", b, reg)
}
