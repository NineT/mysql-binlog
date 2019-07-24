package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"

	"github.com/zssky/log"

	"github.com/mysql-binlog/common/client"
	"github.com/mysql-binlog/common/final"
	"github.com/mysql-binlog/common/inter"
	"github.com/mysql-binlog/common/meta"

	"github.com/mysql-binlog/backup/handler"
)

var (
	// store parameters
	para = &Paras{}
)

// Paras for dumps
type Paras struct {
	etcd    string
	cfsPath string
	mode    string
}

// HttpServer
type HttpServer struct {
	port  int                            // port used for http server
	mutex *sync.Mutex                    // mc map mutex
	mcs   map[int64]*handler.MergeConfig // merge config
}

// NewHttpServer
func NewHttpServer(etcd, cfsPath, mode string, port int) *HttpServer {
	para.etcd = etcd
	para.cfsPath = cfsPath
	para.mode = mode

	return &HttpServer{
		port:  port,
		mutex: &sync.Mutex{},
		mcs:   make(map[int64]*handler.MergeConfig),
	}
}

// Response for request
type Response struct {
	Code    int         `json:"code"`    // code: 0 means normal others means error
	Message string      `json:"message"` // message
	Data    interface{} `json:"data"`    // cluster-id map to offset that current for current dump
}

// initUsingHttp for merge config init
func initUsingHttp(m *meta.DbMeta) (*handler.MergeConfig, error) {
	log.Infof("path{%s}", para.cfsPath)

	// data storage path clusterID
	sp := fmt.Sprintf("%s%d", inter.StdPath(para.cfsPath), m.Inst.CID)

	// 创建目录
	inter.CreateLocalDir(sp)

	// has gtid check
	if has := m.Inst.HasGTID(); !has {
		err := errors.New("only support gtid opened MySQL instances")
		log.Error(err)
		return nil, err
	}

	// get master status
	off := m.Off
	if off == nil {
		pos, err := m.Inst.MasterStatus()
		if err != nil || pos.TrxGtid == "" {
			log.Errorf("transaction id is empty or err {%v}", err)
			return nil, err
		}
		log.Info("start binlog position ", string(pos.TrxGtid))
		off = pos

		off.CID = m.Inst.CID
	}

	log.Debugf("start binlog gtid{%s}, binlog file{%s}, binlog position{%d}", string(off.ExedGtid), off.BinFile, off.BinPos)

	// init merge config
	mc, err := handler.NewMergeConfig(sp, off, m.Inst, para.mode)
	if err != nil {
		log.Error(err)
		return nil, err
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

	return mc, nil
}

// initUsingEtcd 初始化 etcd
func initUsingEtcd(cid int64) (*handler.MergeConfig, error) {
	log.Infof("etcd url{%s}, path{%s}", para.etcd, para.cfsPath)

	// data storage path clusterID
	sp := fmt.Sprintf("%s%d", inter.StdPath(para.cfsPath), cid)

	// 创建目录
	inter.CreateLocalDir(sp)

	etc, err := client.NewEtcdMeta(para.etcd, "v1")
	if err != nil {
		log.Errorf("get meta error{%v}", err)
		return nil, err
	}

	i, err := etc.ReadInstance(cid)
	if err != nil {
		log.Errorf("read MySQL instance{according to %d} from etcd{%s} error {%s}", cid, etc.Url, err)
		return nil, err
	}

	if i == nil {
		err := fmt.Errorf("MySQL instance is nil for{%d}", cid)
		log.Error(err)
		return nil, err
	}

	// has gtid check
	if has := i.HasGTID(); !has {
		log.Error(errors.New("only support gtid opened MySQL instances"))
		return nil, err
	}

	// read meta offset using path
	log.Debug("etcd offset path ", cid)

	o, err := etc.ReadOffset(cid)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	// get master status
	off := o
	if o == nil {
		pos, err := i.MasterStatus()
		if err != nil || pos.TrxGtid == "" {
			log.Errorf("transaction id is empty or err {%v}", err)
			return nil, err
		}
		log.Info("start binlog position ", string(pos.TrxGtid))
		off = pos

		off.CID = cid
		// save newly get offset to etcd as well
		if err := etc.SaveOffset(off); err != nil {
			log.Fatalf("save offset{%v} to etcd error %v", off, err)
			return nil, err
		}
	}

	log.Debugf("start binlog gtid{%s}, binlog file{%s}, binlog position{%d}", string(off.ExedGtid), off.BinFile, off.BinPos)

	// init merge config
	mc, err := handler.NewMergeConfig(sp, off, i, para.mode)
	if err != nil {
		log.Error(err)
		return nil, err
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

	return mc, nil
}

// Start http server
func (h *HttpServer) Start() {
	http.HandleFunc("/", h.heartbeat)
	http.HandleFunc("/start", h.start)
	http.HandleFunc("/stop", h.stop)

	log.Info("start http server :8888")
	if err := http.ListenAndServe(":8888", nil); err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

// readRequest from http.Request
func readRequest(r *http.Request) (*meta.DbMeta, error) {
	log.Debugf("request %v", r)
	// read all
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		//resp.Code = 1000
		//resp.Message = fmt.Sprintf("read data from io reader error %v", err)
		log.Error(err)
		return nil, err
	}

	log.Debugf("read data %s", string(data))

	req := &meta.DbMeta{}
	if err := json.Unmarshal(data, req); err != nil {
		//resp.Message = fmt.Sprintf("unmarshal data{%s} error %v", string(data), err)
		//log.Error(resp.Message)
		//resp.Code = 1000
		return nil, err
	}

	return req, nil
}

// heartbeat for write current
func (h *HttpServer) heartbeat(w http.ResponseWriter, r *http.Request) {
	resp := &Response{
		Code:    Success,
		Message: Errs[Success],
	}

	// writer data to response writer
	defer func() {
		bt, err := json.Marshal(resp)
		if err != nil {
			log.Errorf("json marshal error %v", err)
			return
		}
		if _, err := w.Write(bt); err != nil {
			log.Errorf("writer ok package to response-writer error %v", err)
		}
	}()

	req, err := readRequest(r)
	if err != nil {
		resp.Code = ReadRequestError
		resp.Message = fmt.Sprintf(Errs[ReadRequestError], err)
		log.Error(resp.Message)
		return
	}

	// mutex lock
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if _, ok := h.mcs[req.Inst.CID]; !ok {
		resp.Code = ClusterIDNotFoundError
		resp.Message = fmt.Sprintf(Errs[ClusterIDNotFoundError], req.Inst.CID)
		return
	}

	// status error
	if err := h.mcs[req.Inst.CID].Status(); err != nil {
		resp.Code = DumpError
		resp.Message = fmt.Sprintf(Errs[DumpError], req.Inst.CID, err)
		log.Error(resp.Message)

		// close dump for MySQL
		h.mcs[req.Inst.CID].Close()

		// remove cluster id dump status
		delete(h.mcs, req.Inst.CID)
		return
	}

	// cluster id
	resp.Data = h.mcs[req.Inst.CID].NewlyOffset()
}

// start binlog dump for local
func (h *HttpServer) start(w http.ResponseWriter, r *http.Request) {
	resp := &Response{
		Code:    Success,
		Message: Errs[Success],
	}

	// writer data to response writer
	defer func() {
		bt, err := json.Marshal(resp)
		if err != nil {
			log.Errorf("json marshal error %v", err)
			return
		}
		if _, err := w.Write(bt); err != nil {
			log.Errorf("writer ok package to response-writer error %v", err)
		}
	}()

	// already on dump status for cluster id
	if len(h.mcs) != 0 {
		var cid int64
		for c := range h.mcs {
			cid = c
		}

		// already on dump status then let it go
		resp.Code = AlreadyDumpError
		resp.Message = fmt.Sprintf(Errs[AlreadyDumpError], cid)
		log.Error(resp.Message)
		return
	}

	req, err := readRequest(r)
	if err != nil {
		resp.Code = ReadRequestError
		resp.Message = fmt.Sprintf(Errs[ReadRequestError], err)
		log.Error(resp.Message)
		return
	}

	// mutex lock
	h.mutex.Lock()
	defer h.mutex.Unlock()

	for c, m := range h.mcs {
		log.Debugf("cluster id{%d} offset{%v}", c, m.NewlyOffset())
	}

	if _, ok := h.mcs[req.Inst.CID]; ok {
		resp.Code = AlreadyDumpError
		resp.Message = fmt.Sprintf(Errs[AlreadyDumpError], req.Inst.CID)
		log.Error(resp.Message)
		return
	}

	// two kinds of start http start or etcd start
	var mc *handler.MergeConfig

	if len(strings.TrimSpace(para.etcd)) > 0 {
		tmc, err := initUsingEtcd(req.Inst.CID)
		if err != nil {
			resp.Code = DumpError
			resp.Message = fmt.Sprintf(Errs[DumpError], req.Inst.CID, err)
			log.Error(resp.Message)
			return
		}
		mc = tmc
	} else {
		tmc, err := initUsingHttp(req)
		if err != nil {
			resp.Code = DumpError
			resp.Message = fmt.Sprintf(Errs[DumpError], req.Inst.CID, err)
			log.Error(resp.Message)
			return
		}
		mc = tmc
	}

	h.mcs[req.Inst.CID] = mc

	// start new thread for binlog dump
	go mc.Start()
}

// stop binlog dump for local
func (h *HttpServer) stop(w http.ResponseWriter, r *http.Request) {
	resp := &Response{
		Code:    Success,
		Message: Errs[Success],
	}

	// writer data to response writer
	defer func() {
		bt, err := json.Marshal(resp)
		if err != nil {
			log.Errorf("json marshal error %v", err)
			return
		}
		if _, err := w.Write(bt); err != nil {
			log.Errorf("writer ok package to response-writer error %v", err)
		}
	}()

	req, err := readRequest(r)
	if err != nil {
		resp.Code = ReadRequestError
		resp.Message = fmt.Sprintf(Errs[ReadRequestError], err)
		log.Error(resp.Message)
		return
	}

	// mutex lock
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if _, ok := h.mcs[req.Inst.CID]; !ok {
		resp.Code = ClusterIDNotFoundError
		resp.Message = fmt.Sprintf(Errs[ClusterIDNotFoundError], req.Inst.CID)
		log.Error(resp.Message)
		return
	}

	h.mcs[req.Inst.CID].Close()

	delete(h.mcs, req.Inst.CID)
}
