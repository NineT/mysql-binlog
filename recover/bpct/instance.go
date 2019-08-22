package bpct

/**
* binlog plugin checkpoint
*/

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	// mysql
	_ "github.com/go-sql-driver/mysql"
	"github.com/zssky/log"
)

type GtidMode string

const (
	gtidModeOFF           GtidMode = "OFF"
	gtidModeOffPermissive GtidMode = "OFF_PERMISSIVE"
	gtidModeOnPermissive  GtidMode = "ON_PERMISSIVE"
	gtidModeON            GtidMode = "ON"
)

// Instance MySQL server
type Instance struct {
	user string  // user
	pass string  // password
	db   *sql.DB // db
	tx   *sql.Tx // transaction
	rst  int     // transaction rst flag
}

// NewInstance MySQL db connection pool
func NewInstance(user, pass string, port int) (*Instance, error) {
	url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?maxAllowedPacket=0", user, pass, "127.0.0.1", port, "mysql")
	db, err := sql.Open("mysql", url)
	if err != nil {
		log.Errorf("open MySQL connection error{%v}", err)
		return nil, err
	}

	i := &Instance{
		user: user,
		pass: pass,
		db:   db,
		rst:  0,
	}

	if err := i.InitConn(); err != nil {
		log.Errorf("init connection error{%v}", err)
		return nil, err
	}

	return i, nil
}

// Check MySQL status
func (i *Instance) Check() error {
	rst, err := i.db.Query("select 1")
	if err != nil {
		log.Errorf("execute sql{select 1} error{%v}", err)
		return err
	}
	defer rst.Close()

	return nil
}

// Flush data
func (i *Instance) Flush() error {
	// open gtid
	sqls := []string{
		"SET @@GLOBAL.GTID_MODE = OFF_PERMISSIVE",
		"SET @@GLOBAL.GTID_MODE = ON_PERMISSIVE",
		"SET @@GLOBAL.GTID_MODE = ON",
		"RESET MASTER",
	}

	for _, s := range sqls {
		if _, err := i.db.Exec(s); err != nil {
			log.Errorf("execute query{%s} error{%v}", s, err)
			return err
		}
	}
	return nil
}

// InitConn for set
func (i *Instance) InitConn() error {
	var sqls []string
	switch i.GtidMode() {
	case gtidModeOFF:
		sqls = []string{
			"SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
			"SET @@session.foreign_key_checks=0, @@session.sql_auto_is_null=0, @@session.unique_checks=0, @@session.autocommit=0",
		}
	case gtidModeOffPermissive:
		sqls = []string{
			"SET @@GLOBAL.GTID_MODE = OFF",
			"SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
			"SET @@session.foreign_key_checks=0, @@session.sql_auto_is_null=0, @@session.unique_checks=0, @@session.autocommit=0",
		}
	case gtidModeOnPermissive:
		sqls = []string{
			"SET @@GLOBAL.GTID_MODE = OFF_PERMISSIVE",
			"SET @@GLOBAL.GTID_MODE = OFF",
			"SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
			"SET @@session.foreign_key_checks=0, @@session.sql_auto_is_null=0, @@session.unique_checks=0, @@session.autocommit=0",
		}
	case gtidModeON:
		sqls = []string{
			"SET @@GLOBAL.GTID_MODE = ON_PERMISSIVE",
			"SET @@GLOBAL.GTID_MODE = OFF_PERMISSIVE",
			"SET @@GLOBAL.GTID_MODE = OFF",
			"SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
			"SET @@session.foreign_key_checks=0, @@session.sql_auto_is_null=0, @@session.unique_checks=0, @@session.autocommit=0",
		}
	}

	for _, s := range sqls {
		if _, err := i.db.Exec(s); err != nil {
			log.Errorf("execute query{%s} error{%v}", s, err)
			return err
		}
	}
	return nil
}

// Close all connections
func (i *Instance) Close() {
	if err := i.db.Close(); err != nil {
		log.Warnf("close connection pool error{%v}", err)
	}
}

// Begin
func (i *Instance) Begin() error {
	log.Debugf("begin rst signal flag %d", i.rst)
	if i.rst != 0 {
		if err := i.Commit(); err != nil {
			log.Warn("rst signal is not == 0")
			return err
		}
	}

	tx, err := i.db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		log.Error(err)
		return err
	}
	i.tx = tx
	i.rst ++
	return nil
}

// Execute bins for binlog statement
func (i *Instance) Execute(bins []byte) error {
	log.Debug("execute binlog statement ", " exeucte size ", len(bins))

	if _, err := i.tx.Exec(string(bins)); err != nil {
		log.Error(err)
		return err
	}
	return nil
}

// Commit commit transaction
func (i *Instance) Commit() error {
	log.Debug("commit")
	i.rst --
	return i.tx.Commit()
}

// GtidMode
func (i *Instance) GtidMode() GtidMode {
	sql := "SELECT @@GLOBAL.GTID_MODE"
	rs, err := i.db.Query(sql)
	if err != nil {
		log.Errorf("query sql {%s} error {%v}", sql, err)
		return gtidModeOFF
	}
	defer rs.Close()

	for rs.Next() {
		var m string
		if err := rs.Scan(&m); err != nil {
			log.Errorf("scan rows error{%v}", err)
			return gtidModeOFF
		}

		m = strings.ToUpper(m)
		return GtidMode(m)
	}

	return gtidModeOFF
}
