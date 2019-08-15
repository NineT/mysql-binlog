package bpct

/**
* binlog plugin checkpoint
*/

import (
	"context"
	"database/sql"
	"fmt"
	// mysql
	_ "github.com/go-sql-driver/mysql"
	"github.com/zssky/log"
)

// Instance MySQL server
type Instance struct {
	user string  // user
	pass string  // password
	db   *sql.DB // db
	tx   *sql.Tx // transaction
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
	tx, err := i.db.Begin()
	if err != nil {
		log.Errorf("start transaction error{%v}", err)
		return err
	}

	if _, err := tx.Exec("flush tables with read lock"); err != nil {
		log.Errorf("execute sql{flush tables with read lock} error{%v}", err)
		return err
	}

	if _, err := tx.Exec("flush logs"); err != nil {
		log.Errorf("execute sql{flush logs} error{%v}", err)
		return err
	}

	return tx.Commit()
}

// InitConn for set
func (i *Instance) InitConn() error {
	sqls := []string{
		"set @@GLOBAL.GTID_MODE = ON_PERMISSIVE",
		"set @@GLOBAL.GTID_MODE = OFF_PERMISSIVE",
		"SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
		"SET @@session.foreign_key_checks=0, @@session.sql_auto_is_null=0, @@session.unique_checks=0, @@session.autocommit=0",
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
	log.Debug("begin")
	tx, err := i.db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		log.Error(err)
		return err
	}
	i.tx = tx
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
	return i.tx.Commit()
}
