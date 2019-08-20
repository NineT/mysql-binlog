package conf

import (
	"testing"

	"github.com/zssky/log"
)

func TestIsFilteredSQL(t *testing.T) {
	sql := "CREATE DEFINER=`root`@`%` TRIGGER `pt_osc_test_test_type_del` AFTER DELETE ON `test`.`test_type` FOR EACH ROW DELETE IGNORE FROM `test`.`_test_type_new` WHERE `test`.`_test_type_new`.`id` <=> OLD.`id`"

	b, err := IsFilteredSQL([]byte(sql))
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("is sql filtered ? %v", b)
}
