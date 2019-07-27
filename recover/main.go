package main

import (
	"context"
	"flag"
	"github.com/mysql-binlog/recover/res"
	"os"
	"sync"

	"github.com/zssky/log"

	"github.com/mysql-binlog/common/inter"
	"github.com/mysql-binlog/recover/bpct"
	"github.com/mysql-binlog/recover/ss"
)

var (
	// base cfs存储路径
	base = flag.String("base", "/export/backup/127.0.0.1", "cfs 远程存储路径")

	// clusterid
	clusterID = flag.Int64("clusterid", 0, "集群ID")

	// time
	time = flag.String("time", "2999-12-30 23:59:59", "截止时间")

	// db
	db = flag.String("dbreg", "", "需要恢复的库名正则")

	// tb
	tb = flag.String("tbreg", "", "需要恢复的表名正则")

	// user
	user = flag.String("user", "root", "恢复目标 MySQL user")

	// password
	passwd = flag.String("password", "secret", "恢复目标 MySQL password")

	// log level
	level = flag.String("level", "debug", "日志级别log level {debug/info/warn/error}")
)

// logger 初始化logger
func logger() {
	// 日志输出到标准输出
	log.SetOutput(os.Stdout)

	// 设置日志级别
	log.SetLevelByString(*level)
}

func main() {
	// 解析导入参数
	flag.Parse()

	// init logger
	logger()

	t := inter.ParseTime(*time)

	c := ss.NewCluster(*base, *clusterID)
	tbs, err := c.SelectTables(*db, *tb)
	if err != nil {
		os.Exit(1)
	}

	// take the 1st offset
	s := ss.NewSnapshot(*base, *clusterID, t)

	// take newly offset
	o, err := s.Offset()
	if err != nil {
		os.Exit(1)
	}

	// copy
	if err := s.Copy(); err != nil {
		os.Exit(1)
	}

	// modify conf
	if err := s.ModifyConf(); err != nil {
		os.Exit(1)
	}

	// start MySQL
	if err := s.StartMySQL(); err != nil {
		os.Exit(1)
	}

	// New local MySQL connection POOl
	l, err := bpct.NewLocal(*user, *passwd)
	if err != nil {
		os.Exit(1)
	}
	defer l.Close()

	// MySQL check
	if err := l.Check(); err != nil {
		os.Exit(1)
	}

	// newly context
	ctx, cancel := context.WithCancel(context.Background())

	// init wait group
	size := len(tbs)
	wg := &sync.WaitGroup{}
	wg.Add(size)

	// init error channels
	errs := make(chan error, 64)
	defer close(errs)

	for _, tb := range tbs {
		tr, err := res.NewTable(tb, c.GetClusterPath(), t, ctx, o, l, wg, errs)
		if err != nil {
			// error occur then exit
			os.Exit(1)
		}

		go tr.Recover()

		log.Infof("start table {%s} recover ", tr.ID())
	}

	go func() {
		select {
		case err := <-errs:
			log.Errorf("get error from routine{%v}", err)
			cancel()
		}
	}()

	log.Infof("wait for all to finish")
	wg.Wait()

}
