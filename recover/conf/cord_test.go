package conf

import (
	"regexp"
	"testing"

	"github.com/zssky/log"
)

func TestIsCoordinateSQL(t *testing.T) {
	sql := []byte("drop database pengan")

	reg := regexp.MustCompile("(?i)^DROP\\s+DATABASE\\s+.*$")
	log.Infof("%v", reg.Match(sql))
}
