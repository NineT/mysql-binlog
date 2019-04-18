package main

import (
	"context"
	_ "crypto/md5"
	"flag"
	"fmt"
	"github.com/mysql-binlog/common/final"
	"github.com/mysql-binlog/siddontang/go-mysql/mysql"
	"net"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/zssky/log"

	"github.com/mysql-binlog/common/client"
	"github.com/mysql-binlog/common/db"
	"github.com/mysql-binlog/common/inter"

	"github.com/mysql-binlog/merge/binlog"
	"github.com/mysql-binlog/merge/handler"
)

/**
@author: pengan
作用： 合并binlog 并生成binlog

选择本地或者远程 dump, 都可以将多个binlog文件合并成一个或者一段binlog合并生成一个文件

@attention: 只生成一个文件
*/

var mc *handler.MergeConfig

var (
	// dump information
	dumpHost   = flag.String("dumphost", "127.0.0.1", "dump MySQL 域名")
	dumpPort   = flag.Int("dumpport", 3306, "dump MySQL 端口")
	dumpUser   = flag.String("dumpuser", "root", "dump MySQL 用户名")
	dumpPasswd = flag.String("dumppasswd", "secret", "dump MySQL 密码")

	// start configuration
	startTime       = flag.String("starttime", "2018-12-31 23:59:59", "合并binlog 的时间戳")
	startBinlogFile = flag.String("startblogfile", "mysql-bin.000001", "启动开始合并binlog文件名")
	startBinlogPos  = flag.Int64("startblogpos", 4, "启动开始合并binlog position")
	startGTID       = flag.String("startgtid", "", "启动开始合并binlog 的gtid")

	// stop configuration
	stopTime       = flag.String("stoptime", "2999-12-30 23:59:59", "结束合并binlog 的时间戳")
	stopBinlogFile = flag.String("stopblogfile", "mysql-bin.000001", "结束合并binlog文件名")
	stopBinlogPos  = flag.Int64("stopblogpos", 25374, "结束合并binlog position")
	stopGTID       = flag.String("stopgtid", "", "结束合并binlog的gtid")

	// temperate path
	tmpPath = flag.String("tmppath", "/export/backup", "dump 的数据文件临时存储路径")

	// file name
	targetFile = flag.String("targetfile", "/export/backup/out.log", "合并生成的文件名全称包含路径")

	// compress
	compress = flag.Bool("compress", true, "是否压缩 true表示压缩， false表示不进行压缩")

	// log level
	level = flag.String("level", "debug", "日志级别log level {debug/info/warn/error}")

	// dump binlog mode
	mode = flag.String("mode", "remote", "dump binlog 模式")
)

// 初始化
func initiate() {
	// print input parameter
	log.Info("dump host ", *dumpHost, ", dump port ", *dumpPort, ", dump user ", *dumpUser, ", start time ", *startTime, ", start binlog  file ", *startBinlogFile, ", start binlog position ", *startBinlogPos, ", start gtid ", *startGTID, ", stop time ", *stopTime, ", stop binlog file ", *stopBinlogFile, ", stop binlog position ", *stopBinlogPos, ", stop gtid ", *stopGTID)

	log.Info("temporary path ", *tmpPath, ", target file ", *targetFile, ", log level ", *level, ", compress ", *compress)

	timeStart := int64(0)
	if !strings.EqualFold(*startTime, "") {
		timeStart = parseTime(*startTime)
	}

	timeStop := int64(0)
	if !strings.EqualFold(*stopTime, "") {
		timeStop = parseTime(*stopTime)
	}

	// gtid start
	var gstart *mysql.GTIDSet
	if !strings.EqualFold(*startGTID, "") {
		s, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, *startGTID)
		if err != nil {
			log.Fatal(err)
		}
		gstart = &s
	}

	var gstop *mysql.GTIDSet
	if !strings.EqualFold(*stopGTID, "") {
		s, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, *stopGTID)
		if err != nil {
			log.Fatal(err)
		}
		gstop = &s
	}

	if timeStart == 0 && gstart == nil && (strings.EqualFold(*startBinlogFile, "") && *startBinlogPos == 0) {
		// not specify the correct start offset for dump binlog
		log.Fatal("not specify the correct start offset for dump binlog")
	}

	if timeStop == 0 && gstop == nil && (strings.EqualFold(*stopBinlogFile, "") && *stopBinlogPos == 0) {
		// not specify the correct stop offset for dump binlog
		log.Fatal("not specify the correct stop offset for dump binlog")
	}

	dump := &db.MetaConf{
		Host:     *dumpHost,
		Port:     *dumpPort,
		Db:       "test",
		User:     *dumpUser,
		Password: *dumpPasswd,
	}

	// can decide which dump mode whatever you want
	var dumpMode = handler.DumpModeRemote // default dump mode
	if strings.EqualFold(*mode, "") {
		dumpMode = selectDumpMode(*dumpHost, dump)
	} else if strings.EqualFold(strings.ToLower(*mode), "local") {
		dumpMode = handler.DumpModeLocal
	} else if strings.EqualFold(strings.ToLower(*mode), "remote") {
		dumpMode = handler.DumpModeRemote
	}

	// position start
	posStart := &inter.BinlogPosition{
		Domain:     *dumpHost,
		IP:         dump.IP,
		BinlogFile: *startBinlogFile,
		BinlogPos:  uint32(*startBinlogPos),
		Timestamp:  timeStart,
	}
	if gstart != nil {
		posStart.GTIDSet = *gstart
	}

	// position stop
	posStop := &inter.BinlogPosition{
		Domain:     *dumpHost,
		IP:         dump.IP,
		BinlogFile: *stopBinlogFile,
		BinlogPos:  uint32(*stopBinlogPos),
		Timestamp:  timeStop,
	}
	if gstop != nil {
		posStop.GTIDSet = *gstop
	}

	// data temporary path
	dtp := *tmpPath + inter.DataPath

	// data snapshot path
	dsp := *tmpPath + inter.SnapPath

	if !strings.HasSuffix(*tmpPath, "/") {
		dtp = *tmpPath + "/" + inter.DataPath
		dsp = *tmpPath + "/" + inter.SnapPath
	}

	// 创建目录
	createDir(dtp, dsp)

	// path, file name
	p, fn := (*targetFile)[:strings.LastIndex(*targetFile, "/")], (*targetFile)[strings.LastIndex(*targetFile, "/"):]

	c := &client.CFSClient{
		Path:     p,
		Compress: *compress,
	}

	w, err := c.CreateFile("", fn)
	if err != nil {
		log.Fatal(err)
	}

	mc = &handler.MergeConfig{
		RWg:           &sync.WaitGroup{},
		TempPath:      dtp,
		SnapshotPath:  dsp,
		Writer:        w,
		DumpMode:      dumpMode,
		StartPosition: posStart, // 共享同一个地址 后续更新采用同一地址
		StopPosition:  posStop,

		TableHandlers:   make(map[string]*binlog.TableEventHandler),
		DumpMySQLConfig: dump,
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

// createDir 创建路径
func createDir(ps ...string) {
	for _, p := range ps {
		os.MkdirAll(p, os.ModePerm)
	}
}

func parseTime(end string) int64 {
	ts, _ := time.Parse("2006-01-02 15:04:05", end)
	return ts.Unix()
}

// selectDumpMode 判断dump的模式remote / local
func selectDumpMode(domain string, backup *db.MetaConf) handler.DumpMode {
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
