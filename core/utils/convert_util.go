package utils

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"time"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

const (
	logicalBits     = 18
	logicalBitsMask = (1 << logicalBits) - 1
	PARAMS          = "params"
)

// ComposeTS returns a timestamp composed of physical part and logical part
func ComposeTS(physical, logical int64) uint64 {
	return uint64((physical << logicalBits) + logical)
}

// ParseTS returns a timestamp composed of physical part and logical part
func ParseTS(ts uint64) (time.Time, uint64) {
	logical := ts & logicalBitsMask
	physical := ts >> logicalBits
	physicalTime := time.Unix(int64(physical/1000), int64(physical)%1000*time.Millisecond.Nanoseconds())
	return physicalTime, logical
}

// kvPairToMap largely copied from internal/proxy/task.go#parseIndexParams
func KVPairToMap(m []*backuppb.KeyValuePair) (map[string]string, error) {
	params := make(map[string]string)
	for _, kv := range m {
		if kv.Key == PARAMS {
			params, err := parseParamsMap(kv.Value)
			if err != nil {
				return nil, err
			}
			for k, v := range params {
				params[k] = v
			}
		} else {
			params[kv.Key] = kv.Value
		}
	}
	return params, nil
}

// parseParamsMap parse the jsonic index parameters to map
func parseParamsMap(mStr string) (map[string]string, error) {
	buffer := make(map[string]interface{})
	err := json.Unmarshal([]byte(mStr), &buffer)
	if err != nil {
		return nil, errors.New("Unmarshal params failed")
	}
	ret := make(map[string]string)
	for key, value := range buffer {
		valueStr := fmt.Sprintf("%v", value)
		ret[key] = valueStr
	}
	return ret, nil
}

func MapToKVPair(dict map[string]string) []*backuppb.KeyValuePair {
	kvs := make([]*backuppb.KeyValuePair, 0)

	for key, value := range dict {
		kvs = append(kvs, &backuppb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	}
	return kvs
}

// KvPairsMap converts common.KeyValuePair slices into map
func KvPairsMap(kvps []*backuppb.KeyValuePair) map[string]string {
	m := make(map[string]string)
	for _, kvp := range kvps {
		m[kvp.Key] = kvp.Value
	}
	return m
}

func ArrayToMap(strs []int64) map[int64]bool {
	ret := make(map[int64]bool)
	for _, value := range strs {
		ret[value] = true
	}
	return ret
}

func MapKeyArray(dict map[int64]bool) []int64 {
	arr := make([]int64, 0)
	for k, _ := range dict {
		arr = append(arr, k)
	}
	return arr
}

func Base64MsgPosition(position *milvuspb.MsgPosition) string {
	positionByte, err := proto.Marshal(position)
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(positionByte)
}
