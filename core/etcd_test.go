package core

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"log"
	"testing"
	"time"
)

func TestETCDList(t *testing.T) {
	// 1. etcd client
	// Initialize etcd client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"10.102.8.127:2379"},
		//Endpoints: []string{"10.102.10.120:2379"},
		//Endpoints: []string{"10.102.10.139:2379"},
		//Endpoints:   []string{"localhost:2379", "localhost:22379", "localhost:32379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Printf("Initialize etcd client failed. err: %v\n", err)
	}
	//kv := clientv3.NewKV(cli)

	ctx := context.TODO()

	opts := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithSerializable()}
	getResp, err := cli.Get(ctx, "", opts...)

	for _, kvs := range getResp.Kvs {
		log.Println(zap.Any("key", string(kvs.Key)), zap.Any("value", string(kvs.Value)))
	}

	//log.Println("getresp", zap.Any("resp", getResp), zap.Any("values", getResp.Kvs))
	cli.Close()
}

func TestETCDGet(t *testing.T) {
	// 1. etcd client
	// Initialize etcd client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"10.102.8.127:2379"},
		//Endpoints: []string{"10.102.10.120:2379"},
		//Endpoints: []string{"10.102.10.162:2379"},
		//Endpoints: []string{"10.102.10.139:2379"},
		//Endpoints:   []string{"localhost:2379", "localhost:22379", "localhost:32379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Printf("Initialize etcd client failed. err: %v\n", err)
	}
	//kv := clientv3.NewKV(cli)

	ctx := context.TODO()

	//opts := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithSerializable()}
	//getResp, err := cli.Get(ctx, "by-dev/meta/datacoord-meta/binlog/437433135932575088/437433135932575089/437433135932575098/102")
	getResp, err := cli.Get(ctx, "by-dev/meta/datacoord-meta/binlog/437454123484053509/437454123484053510/437454123484253594/0")

	for _, kvs := range getResp.Kvs {
		log.Println(zap.Any("key", string(kvs.Key)), zap.Any("value", string(kvs.Value)))
		m := &backuppb.FieldBinlog{}
		proto.Unmarshal(kvs.Value, m)
		log.Println(len(m.Binlogs))
		log.Println(m)
	}

	//log.Println("getresp", zap.Any("resp", getResp), zap.Any("values", getResp.Kvs))
	cli.Close()
}
