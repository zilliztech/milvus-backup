// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memkv

import (
	"fmt"
	"sync"

	"github.com/google/btree"
)

// MemoryKV implements BaseKv interface and relies on underling btree.BTree.
// As its name implies, all data is stored in memory.
type MemoryKV struct {
	sync.RWMutex
	tree *btree.BTree
}

// NewMemoryKV returns an in-memory kvBase for testing.
func NewMemoryKV() *MemoryKV {
	return &MemoryKV{
		tree: btree.New(2),
	}
}

type memoryKVItem struct {
	key   string
	value string
}

var _ btree.Item = (*memoryKVItem)(nil)

// Less returns true if the item is less than the given one.
func (s memoryKVItem) Less(than btree.Item) bool {
	return s.key < than.(memoryKVItem).key
}

// Load loads an object with @key.
func (kv *MemoryKV) Load(key string) (string, error) {
	kv.RLock()
	defer kv.RUnlock()
	item := kv.tree.Get(memoryKVItem{key: key})
	if item == nil {
		return "", fmt.Errorf("invalid key: %s", key)
	}
	return item.(memoryKVItem).value, nil
}

// Get return value if key exists, or return empty string
func (kv *MemoryKV) Get(key string) string {
	kv.RLock()
	defer kv.RUnlock()
	item := kv.tree.Get(memoryKVItem{key: key})
	if item == nil {
		return ""
	}
	return item.(memoryKVItem).value
}

// LoadWithDefault loads an object with @key. If the object does not exist, @defaultValue will be returned.
func (kv *MemoryKV) LoadWithDefault(key, defaultValue string) string {
	kv.RLock()
	defer kv.RUnlock()
	item := kv.tree.Get(memoryKVItem{key: key})
	if item == nil {
		return defaultValue
	}
	return item.(memoryKVItem).value
}

// Save object with @key to btree. Object value is @value.
func (kv *MemoryKV) Save(key, value string) error {
	kv.Lock()
	defer kv.Unlock()
	kv.tree.ReplaceOrInsert(memoryKVItem{key, value})
	return nil
}

// Remove deletes an object with @key.
func (kv *MemoryKV) Remove(key string) error {
	kv.Lock()
	defer kv.Unlock()

	kv.tree.Delete(memoryKVItem{key: key})
	return nil
}
