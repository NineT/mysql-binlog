package inter

import (
	"fmt"
	"testing"
)

func TestExists(t *testing.T) {
	f := `/export/backup/1/test.test02/1561346137.log`
	fmt.Println(Exists(f))
}
