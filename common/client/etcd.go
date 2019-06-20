package client

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/zssky/log"
	"go.etcd.io/etcd/clientv3"

	"github.com/mysql-binlog/common/meta"
)

// EtcdMeta implement meta structure using compress data
type EtcdMeta struct {
	Url     string           // etcd url using domain
	Version string           // etcd meta version
	client  *clientv3.Client // etcd client
}

// NewEtcdMeta new etcd meta
func NewEtcdMeta(url, v string) (*EtcdMeta, error) {
	cfg := clientv3.Config{
		Endpoints:   []string{url},
		DialTimeout: time.Second * 5,
	}
	c, err := clientv3.New(cfg)
	if err != nil {
		log.Error("create new etcd client error", err)
		return nil, err
	}

	if strings.HasSuffix(v, "/") {
		v = strings.TrimSuffix(v, "/")
	}

	if strings.HasPrefix(v, "/") {
		v = strings.TrimPrefix(v, "/")
	}

	return &EtcdMeta{
		Url:     url,
		Version: fmt.Sprintf("/%s", v),
		client:  c,
	}, nil
}

// Read read data from etcd meta
func (m *EtcdMeta) Read(k interface{}) (*meta.Offset, error) {
	// format key
	key := fmt.Sprintf("%v", k)
	if strings.HasPrefix(key, "/") {
		key = strings.TrimPrefix(key, "/")
	}
	key = fmt.Sprintf("%s/%s", m.Version, key)

	// get response
	kv := clientv3.NewKV(m.client)
	resp, err := kv.Get(context.Background(), key)
	if err != nil {
		log.Errorf("get key %s/%s from etcd error %v", m.Version, key, err)
		return nil, err
	}

	if resp.Count == 0 {
		log.Warn("key{%s/%s} not exist", m.Version, key)
		return nil, nil
	}

	var out bytes.Buffer
	v := []byte(resp.Kvs[0].Value)
	b := bytes.NewReader(v)
	r, _ := zlib.NewReader(b)
	if _, err := io.Copy(&out, r); err != nil {
		log.Errorf("un-compress data from etcd for key{%s} error %v", key, err)
		return nil, err
	}

	off := &meta.Offset{}
	if err := json.Unmarshal(out.Bytes(), off);
		err != nil {
		log.Errorf("json unmarshal %s error %v", resp.Kvs[0].Value, err)
		return nil, err
	}

	return off, nil
}

// Save data to etcd
func (m *EtcdMeta) Save(o *meta.Offset) error {
	v, err := json.Marshal(o)
	if err != nil {
		log.Errorf("json marshal offset %v error %v", o, err)
		return err
	}

	k := fmt.Sprintf("%s/%d", m.Version, o.ClusterID)

	var out bytes.Buffer
	w := zlib.NewWriter(&out)
	if _, err := w.Write(v); err != nil {
		log.Errorf("copy compress data for key {%s} value{%s} error %v", k, string(v), err)
		return err
	}

	if err := w.Flush(); err != nil {
		log.Errorf("flush zlib writer for key{%s} value{%s} error %s ", k, string(v), err)
		return err
	}

	if err := w.Close(); err != nil {
		log.Errorf("close zlib compress writer for key{%s} error %v", k, err)
		return err
	}

	api := clientv3.NewKV(m.client)
	if _, err := api.Put(context.Background(), k, string(out.Bytes())); err != nil {
		log.Errorf("etcd put key {%s} value {%s} error {%v}", k, string(v), err)
		return err
	}
	log.Debugf("save value {%s} to key {%s} successfully", string(v), k)

	return nil
}
