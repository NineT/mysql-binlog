package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/zssky/log"

	"github.com/mysql-binlog/backup/server"
)

/**
@author: pengan
作用： 合并binlog 并生成binlog 备份不参与合并数据
*/

var (
	// port using for http port
	port = flag.Int("port", 8888, "http服务端口")

	// dump for MySQL using separated mode or integrated mode
	mode = flag.String("mode", "integrated", "separated or integrated 表示是否将每个表的binlog事件独立而不往一个binlog文件写")

	// cfs storage path for binlog data
	cfsPath = flag.String("cfspath", "/export/backup/", "cfs 数据存储目录")

	// etcd url http://localhost:2379
	etcd = flag.String("etcd", "", "etcd 请求地址")

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

	//f, _ := os.Create("/tmp/cpu.prof")
	//if err := pprof.StartCPUProfile(f); err != nil {
	//	log.Fatal(err)
	//}

	c := make(chan os.Signal)
	go func() {
		signal.Notify(c)
		s := <-c
		//pprof.StopCPUProfile()

		//if err := f.Close(); err != nil {
		//	log.Fatal(err)
		//}

		fmt.Println("退出信号", s)
		os.Exit(0)
	}()

	server.NewHttpServer(*etcd, *cfsPath, *mode, *port).Start()
}
