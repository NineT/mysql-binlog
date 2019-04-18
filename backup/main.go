package main

import (
	"context"
	_ "crypto/md5"
	"flag"
	"fmt"
	"github.com/zssky/log"
	"net"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"sync"

	"github.com/mysql-binlog/common/client"
	"github.com/mysql-binlog/common/db"
	"github.com/mysql-binlog/common/final"
	"github.com/mysql-binlog/common/inter"

	"github.com/mysql-binlog/backup/binlog"
	"github.com/mysql-binlog/backup/handler"
)

/**
@author: pengan
作用： 合并binlog 并生成binlog
*/

var mc *handler.MergeConfig

var (
	// dump information
	dumpHost   = flag.String("dumphost", "127.0.0.1", "dump MySQL 域名")
	dumpPort   = flag.Int("dumpport", 3306, "dump MySQL 端口")
	dumpUser   = flag.String("dumpuser", "root", "dump MySQL 用户名")
	dumpPasswd = flag.String("dumppasswd", "secret", "dump MySQL 密码")
	dumpMode   = flag.String("dumpmode", "remote", "dump 模式 {local, remote}")
	stopTime   = flag.String("stoptime", "2999-12-30 23:59:59", "dump 结束时间")

	// meta information
	metaHost   = flag.String("metahost", "127.0.0.1", "元数据库域名")
	metaPort   = flag.Int("metaport", 3306, "元数据库端口")
	metaUser   = flag.String("metauser", "root", "元数据库用户名")
	metaPasswd = flag.String("metapasswd", "secret", "元数据库密码")
	metaDb     = flag.String("metadb", "binlog_backup", "元数据库名")

	// temporary path for level db data
	tmpPath = flag.String("tmppath", "/export/backup/", "临时文件存储路径")

	// cfs storage path for binlog data
	cfsPath = flag.String("cfspath", "/export/backup/", "cfs 数据存储目录")

	// compress 是否压缩数据
	compress = flag.Bool("compress", true, "是否压缩数据")

	// backupperiod 备份周期 : day, hour, minute, second
	period = flag.String("period", "sec", "备份周期 {day, hour, min, sec}")

	// log level
	level = flag.String("level", "debug", "日志级别log level {debug/info/warn/error}")
)

// 初始化
func initiate() {
	// print input parameter
	log.Info("dump host ", *dumpHost, ", dump port ", *dumpPort, ", dump user ", *dumpUser, ", dump password *****", ", dump stop time ", *stopTime)

	log.Info("meta host ", *metaHost, ", meta port ", *metaPort, ", meta user ", *metaUser, ", meta password *****", ", meta db ", *metaDb)

	log.Info("temporary path ", *tmpPath, ", log level ", *level)

	finalTime := inter.ParseTime(*stopTime)

	meta := &db.MetaConf{
		Host:     *metaHost,
		Port:     *metaPort,
		Db:       *metaDb,
		User:     *metaUser,
		Password: *metaPasswd,
	}

	dump := &db.MetaConf{
		Host:     *dumpHost,
		Port:     *dumpPort,
		Db:       "test",
		User:     *dumpUser,
		Password: *dumpPasswd,
	}

	startPos, err := meta.GetPosition(*dumpHost)
	if err != nil {
		log.Fatal(err)
	}
	log.Info("start binlog position ", startPos)

	dumpMode := selectDumpMode(*dumpHost, dump)

	// data temporary path
	dtp := fmt.Sprintf("%s%s", inter.StdPath(*tmpPath), inter.DataPath)

	// data snapshot path
	dsp := fmt.Sprintf("%s%s", inter.StdPath(*tmpPath), inter.SnapPath)

	// data storage path
	sp := fmt.Sprintf("%s%s", inter.StdPath(*cfsPath), inter.StorePath)

	// 创建目录
	inter.CreateLocalDir(dtp)

	inter.CreateLocalDir(dsp)

	// init merge config
	mc = &handler.MergeConfig{
		FinalTime:    finalTime,
		TempPath:     inter.StdPath(dtp),
		SnapshotPath: inter.StdPath(dsp),
		Client: &client.CFSClient{
			Path:     inter.StdPath(sp),
			Compress: *compress,
		},
		Period:        inter.FileType(*period),
		DumpMode:      dumpMode,
		StartPosition: startPos, // 共享同一个地址 后续更新采用同一地址

		// initiate >= start
		LatestPosition:  startPos,
		TableHandlers:   make(map[string]*binlog.TableEventHandler),
		DumpMySQLConfig: dump,
		MetaMySQLConfig: meta,
		Wgs:             make(map[string]*sync.WaitGroup),
	}

	// init after math
	errs := make(chan interface{}, 4)
	ctx, cancel := context.WithCancel(context.Background())
	am := &final.After{
		Errs:   errs,
		Ctx:    ctx,
		Cancel: cancel,
	}

	mc.After = am
}

// selectDumpMode 判断dump的模式remote / local
func selectDumpMode(domain string, backup *db.MetaConf) handler.DumpMode {
	if strings.EqualFold(*dumpMode, "remote") {
		// dumpmode == remote
		return handler.DumpModeRemote
	}

	// 如果域名对应的ip 是本机的ip 则采用本地备份
	ns, err := net.LookupHost(domain)
	if err != nil {
		log.Fatal(err)
	}

	// 内网当中一个域名只对应一个ip
	var ip2Domain string
	for _, n := range ns {
		ip2Domain = n
	}
	backup.IP = ip2Domain // 设置dump 域名对应的ip

	netIps := make(map[string]string)

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Fatal(err)
	}

	for _, addr := range addrs {
		// 检查ip地址判断是否回环地址
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				netIps[ipNet.IP.String()] = ipNet.IP.String()
			}
		}
	}

	if _, ok := netIps[ip2Domain]; ok {
		return handler.DumpModeLocal
	}

	return handler.DumpModeRemote
}

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

	initiate()

	f, _ := os.Create("/tmp/cpu.prof")
	pprof.StartCPUProfile(f)

	c := make(chan os.Signal)
	go func() {
		signal.Notify(c)
		s := <-c
		pprof.StopCPUProfile()

		f.Close()

		fmt.Println("退出信号", s)
		os.Exit(0)
	}()

	mc.Start()
}
