package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/mysql-binlog/recover/bpct"
	"io"
	"os"
	"regexp"
	"sync"

	"github.com/zssky/log"

	"github.com/mysql-binlog/common/client"
	"github.com/mysql-binlog/common/db"
	"github.com/mysql-binlog/common/final"
	"github.com/mysql-binlog/common/inter"
	"github.com/mysql-binlog/recover/res"
)

var (
	// cfsPath cfs存储路径
	cfsPath = flag.String("cfspath", "/export/backup/127.0.0.1", "cfs 远程存储路径")

	// stop time
	stopTime = flag.String("stoptime", "2999-12-30 23:59:59", "截止时间")

	// database
	database = flag.String("dbreg", "", "需要恢复的库名正则")

	// table
	table = flag.String("tbreg", "", "需要恢复的表名正则")

	// host
	host = flag.String("host", "127.0.0.1", "恢复目标MySQL host")

	// port
	port = flag.Int("port", 3358, "恢复目标MySQL port")

	// user
	user = flag.String("user", "root", "恢复目标 MySQL user")

	// password
	passwd = flag.String("password", "secret", "恢复目标 MySQL password")

	// temporary path for level db data
	tmpPath = flag.String("tmppath", "/tmp/backup", "临时文件存储路径")

	// compress 數據是否被压缩
	compress = flag.Bool("compress", true, "数据是否被压缩")

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

	logger()

	log.Info("cfs path ", *cfsPath, ", database reg ", *database, ", table reg ", *table, ",compress ", *compress, ",temperate path ", *tmpPath)

	// new cfs client
	c := &client.CFSClient{
		Path:     inter.StdPath(*cfsPath),
		Compress: *compress,
		DbReg:    regexp.MustCompile(*database),
		TbReg:    regexp.MustCompile(*table),
	}

	// selected dirs
	dirs := c.SelectTableDirs()
	if len(dirs) == 0 {
		msg := fmt.Sprintf("%s %s", "no suitable table selected from path", c.Path)
		log.Fatal(msg)
	}

	log.Debug("%v", dirs)

	end := inter.ParseTime(*stopTime)

	// multi routines contract
	wg := &sync.WaitGroup{}

	// read save point
	d := "conf/"
	inter.CreateLocalDir(d)

	// checkpoint maps
	cms, err := bpct.ReadCheckPoint("conf/check.properties")
	if err != nil {
		log.Fatal(err)
		return
	}

	f, err := os.Create("conf/check.properties")
	if err != nil {
		log.Fatal(err)
		return
	}
	defer f.Close()

	// context
	ctx, cancel := context.WithCancel(context.Background())

	// errors channels
	errs := make(chan error, 64)
	a := &final.After{
		Errs:   make(chan interface{}, 4),
		Ctx:    ctx,
		Cancel: cancel,
		F:      nil,
	}

	// cancel if error occur
	go func(a *final.After) {
		for {
			select {
			case e, _ := <-errs:
				log.Error(e)
				a.Cancel()
				return
			}
		}
	}(a)

	// recover public path
	public(f, cms, a, wg, c, end, inter.StdPath(*tmpPath))

	tables(f, cms, a, wg, c, dirs, end, inter.StdPath(*tmpPath))

	log.Info("recover over close error channels")
	close(errs)
}

// public recover public data
func public(w io.Writer, cms map[string]*bpct.CheckPoint, a *final.After, wg *sync.WaitGroup, c *client.CFSClient, end int64, tmpPath string) {
	log.Debug("recover public data")
	wg.Add(1)
	m := &db.MetaConf{
		IP:       *host,
		Host:     *host,
		Port:     *port,
		Db:       "information_schema",
		User:     *user,
		Password: *passwd,
	}

	p := fmt.Sprintf("%s%s", tmpPath, inter.Public)

	inter.CreateLocalDir(p)

	r := &res.Recover{
		Writer:      w,
		Table:       inter.Public,
		After:       a,
		TempPath:    inter.StdPath(p),
		TaskCounter: wg,
		End:         end,
	}

	if c, ok := cms[inter.Public]; ok {
		log.Debug("last check ", c)
		r.LastCheck = c
	}

	r.StartPubRecover(c.PublicPath(), end, c, m)

	wg.Wait()
}

// tables recover table data
func tables(w io.Writer, cms map[string]*bpct.CheckPoint, a *final.After, wg *sync.WaitGroup, c *client.CFSClient, dirs inter.FileNames, end int64, tmpPath string) {
	// add len(dirs)
	wg.Add(len(dirs))

	for _, d := range dirs {
		m := &db.MetaConf{
			IP:       *host,
			Host:     *host,
			Port:     *port,
			Db:       "information_schema",
			User:     *user,
			Password: *passwd,
		}
		p := fmt.Sprintf("%s%s", tmpPath, d.Name)

		inter.CreateLocalDir(p)

		r := &res.Recover{
			Writer:      w,
			Table:       d.Name,
			After:       a,
			TempPath:    inter.StdPath(p),
			TaskCounter: wg,
			End:         end,
		}

		if c, ok := cms[d.Name]; ok {
			log.Debug("last check ", c)
			r.LastCheck = c
		}

		go r.StartTableRecover(d, end, c, m)
	}

	wg.Wait()
}
