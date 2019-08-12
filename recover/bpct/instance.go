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
	user string               // user
	pass string               // password
	db   *sql.DB              // db
	cons map[string]*sql.Conn // connection map
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
		cons: make(map[string]*sql.Conn),
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

// GetConn for table means one table one fixed connection
func (i *Instance) GetConn(ctx context.Context, table string) (*sql.Conn, error) {
	if con, ok := i.cons[table]; ok {
		if err := con.Close(); err != nil {
			log.Warnf("recycle connection for table{%s} error{%v}", table, err)
		}
	}

	c, err := i.db.Conn(ctx)
	if err != nil {
		log.Errorf("open connection for table{%s} error{%v}", table, err)
		return nil, err
	}
	i.cons[table] = c
	return c, nil
}

// Close all connections
func (i *Instance) Close() {
	for t, c := range i.cons {
		log.Infof("close connection for table{%s} ", t)
		if err := c.Close(); err != nil {
			log.Warnf("close table {%s} connection error{%v}", t, err)
		}
	}

	if err := i.db.Close(); err != nil {
		log.Warnf("close connection pool error{%v}", err)
	}
}
