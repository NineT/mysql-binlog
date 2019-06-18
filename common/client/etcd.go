package client

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/zssky/log"
	"go.etcd.io/etcd/clientv3"

	"github.com/mysql-binlog/common/meta"
)

// EtcdMeta implement meta structure
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
	key := fmt.Sprintf("%v", k)
	if strings.HasPrefix(key, "/") {
		key = strings.TrimPrefix(key, "/")
	}

	kv := clientv3.NewKV(m.client)

	resp, err := kv.Get(context.Background(), fmt.Sprintf("%s/%s", m.Version, key))
	if err != nil {
		log.Errorf("get key %s/%s from etcd error %v", m.Version, key, err)
		return nil, err
	}

	if resp.Count == 0 {
		log.Warn("key{%s/%s} not exist", m.Version, key)
		return nil, nil
	}

	off := &meta.Offset{}
	if err := json.Unmarshal([]byte(resp.Kvs[0].Value), off); err != nil {
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

	api := clientv3.NewKV(m.client)
	if _, err := api.Put(context.Background(), fmt.Sprintf("%s/%d", m.Version, o.ClusterID), string(v)); err != nil {
		log.Errorf("etcd put key {%s} value {%s} error {%v}", fmt.Sprintf("%s/%d", m.Version, o.ClusterID), string(v), err)
		return err
	}
	log.Debugf("save value {%s} to key {%s} successfully", string(v), fmt.Sprintf("%s/%d", m.Version, o.ClusterID))

	return nil
}
