package bpct

/**
* binlog plugin checkpoint
*/

import (
	"database/sql"
	"fmt"
	"sync"

	// mysql
	_ "github.com/go-sql-driver/mysql"
	"github.com/zssky/log"
)

// Instance MySQL server
type Instance struct {
	user string  // user
	pass string  // password
	db   *sql.DB // db

	lock *sync.Mutex        // map locks
	trxs map[string]*sql.Tx // transactions
}

// NewInstance MySQL db connection pool
func NewInstance(user, pass string, port int) (*Instance, error) {
	url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?maxAllowedPacket=0", user, pass, "127.0.0.1", port, "mysql")
	db, err := sql.Open("mysql", url)
	if err != nil {
		log.Errorf("open MySQL connection error{%v}", err)
		return nil, err
	}

	return &Instance{
		user: user,
		pass: pass,
		db:   db,
		lock: &sync.Mutex{},
		trxs: make(map[string]*sql.Tx),
	}, nil
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

// Begin binlog syntax statement
func (i *Instance) Begin(table string) error {
	log.Debug("execute binlog statement for begin")
	if _, ok := i.trxs[table]; ok {
		// already transaction already begin
		return nil
	}

	tx, err := i.db.Begin()
	if err != nil {
		log.Errorf("db begin open transaction error{%v}", err)
		return err
	}

	// one table have one transaction pool
	i.lock.Lock()
	defer i.lock.Unlock()
	i.trxs[table] = tx

	return nil
}

// Execute binlog statements under transaction
func (i *Instance) Execute(table string, bins []byte) error {
	tx, ok := i.trxs[table]
	if !ok {
		err := fmt.Errorf("no begin for table{%s} on transation", table)
		log.Error(err)
		return err
	}

	if _, err := tx.Exec(string(bins)); err != nil {
		log.Error(err)
		return err
	}
	return nil
}

// Commit transaction data
func (i *Instance) Commit(table string) error {
	tx, ok := i.trxs[table]
	if !ok {
		err := fmt.Errorf("no begin for table{%s} on transation", table)
		log.Error(err)
		return err
	}

	// commit error
	if err := tx.Commit(); err != nil {
		log.Errorf("table {%s} commit error{%v}", table, err)
		return err
	}

	// no need to clear
	//i.lock.Lock()
	//defer i.lock.Unlock()
	//delete(i.trxs, table)

	log.Debugf("table {%s} commit success", table)
	return nil
}

// Close
func (i *Instance) Close() {
	for t, tx := range i.trxs {
		log.Warnf("transaction on table{%s} no closed now to rollback", t)
		if err := tx.Rollback(); err != nil {
		}
	}

	if err := i.db.Close(); err != nil {
		log.Errorf("close db error{%v}", err)
	}
}
