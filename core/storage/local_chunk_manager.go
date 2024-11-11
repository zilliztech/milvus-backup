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

package storage

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/zilliztech/milvus-backup/internal/log"
)

func WrapErrFileNotFound(key string) error {
	return fmt.Errorf("%w(key=%s)", ErrNoSuchKey, key)
}

// LocalChunkManager is responsible for read and write local file.
type LocalChunkManager struct {
	rootPath       string
	backupRootPath string
	config         *StorageConfig
}

var _ ChunkManager = (*LocalChunkManager)(nil)

// NewLocalChunkManager create a new local manager object.
func NewLocalChunkManager(ctx context.Context, config *StorageConfig) (*LocalChunkManager, error) {
	return &LocalChunkManager{
		rootPath:       config.RootPath,
		backupRootPath: config.backupRootPath,
		config:         config,
	}, nil
}

func (mcm *LocalChunkManager) Config() *StorageConfig {
	return mcm.config
}

// RootPath returns lcm root path.
func (lcm *LocalChunkManager) RootPath() string {
	return lcm.rootPath
}

// Path returns the path of local data if exists.
func (lcm *LocalChunkManager) Path(ctx context.Context, bucketName string, filePath string) (string, error) {
	exist, err := lcm.Exist(ctx, bucketName, filePath)
	if err != nil {
		return "", err
	}

	if !exist {
		return "", WrapErrFileNotFound(filePath)
	}

	return filePath, nil
}

// Write writes the data to local storage.
func (lcm *LocalChunkManager) Write(ctx context.Context, bucketName string, filePath string, content []byte) error {
	dir := path.Dir(filePath)
	exist, err := lcm.Exist(ctx, bucketName, dir)
	if err != nil {
		return err
	}
	if !exist {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return WrapErrFileNotFound(filePath)
		}
	}
	return WriteFile(filePath, content, os.ModePerm)
}

// Exist checks whether chunk is saved to local storage.
func (lcm *LocalChunkManager) Exist(ctx context.Context, bucketName string, filePath string) (bool, error) {
	_, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, WrapErrFileNotFound(filePath)
	}
	return true, nil
}

// Read reads the local storage data if exists.
func (lcm *LocalChunkManager) Read(ctx context.Context, bucketName string, filePath string) ([]byte, error) {
	return ReadFile(filePath)
}

func (lcm *LocalChunkManager) ListWithPrefix(ctx context.Context, bucketName string, prefix string, recursive bool) ([]string, []int64, error) {
	var filePaths []string
	var sizes []int64
	if recursive {
		dir := filepath.Dir(prefix)
		err := filepath.Walk(dir, func(filePath string, f os.FileInfo, err error) error {
			if strings.HasPrefix(filePath, prefix) && !f.IsDir() {
				filePaths = append(filePaths, filePath)
			}
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
		for _, filePath := range filePaths {
			size, err := lcm.Size(ctx, bucketName, filePath)
			if err != nil {
				return filePaths, nil, err
			}
			sizes = append(sizes, size)
		}
		return filePaths, sizes, nil
	}

	globPaths, err := filepath.Glob(prefix + "*")
	if err != nil {
		return nil, nil, err
	}
	filePaths = append(filePaths, globPaths...)
	for _, filePath := range filePaths {
		size, err := lcm.Size(ctx, bucketName, filePath)
		if err != nil {
			return filePaths, nil, err
		}
		sizes = append(sizes, size)
	}

	return filePaths, sizes, nil
}

func (lcm *LocalChunkManager) Size(ctx context.Context, bucketName string, filePath string) (int64, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, WrapErrFileNotFound(filePath)
		}
		return 0, WrapErrFileNotFound(filePath)
	}
	// get the size
	size := fi.Size()
	return size, nil
}

func (lcm *LocalChunkManager) Remove(ctx context.Context, bucketName string, filePath string) error {
	err := os.RemoveAll(filePath)
	return err
}

func (lcm *LocalChunkManager) RemoveWithPrefix(ctx context.Context, bucketName string, prefix string) error {
	// If the prefix is empty string, the ListWithPrefix() will return all files under current process work folder,
	// MultiRemove() will delete all these files. This is a danger behavior, empty prefix is not allowed.
	if len(prefix) == 0 {
		errMsg := "empty prefix is not allowed for ChunkManager remove operation"
		log.Warn(errMsg)
		return errors.New(errMsg)
	}

	filePaths, _, err := lcm.ListWithPrefix(ctx, bucketName, prefix, true)
	if err != nil {
		return err
	}

	return lcm.MultiRemove(ctx, bucketName, filePaths)
}

func (lcm *LocalChunkManager) MultiRemove(ctx context.Context, bucketName string, filePaths []string) error {
	for _, filePath := range filePaths {
		err := lcm.Remove(ctx, bucketName, filePath)
		if err != nil {
			return err
		}
	}
	return nil
}

func (lcm *LocalChunkManager) Copy(ctx context.Context, fromBucketName string, toBucketName string, fromPath string, toPath string) error {
	sourceFileStat, err := os.Stat(fromPath)
	if err != nil {
		return err
	}

	if sourceFileStat.IsDir() {
		return CopyDir(fromPath, toPath)
	} else {
		return CopyFile(fromPath, toPath)
	}
}

func (lcm *LocalChunkManager) UploadObject(ctx context.Context, i UploadObjectInput) error {
	dir := path.Dir(i.Key)
	exist, err := lcm.Exist(ctx, i.Bucket, dir)
	if err != nil {
		return err
	}
	if !exist {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return WrapErrFileNotFound(i.Key)
		}
	}

	// Open or create a local file
	file, err := os.Create(i.Key)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return err
	}
	//defer file.Close()

	// Write the contents of the reader to the file using io.Copy
	_, err = io.Copy(file, i.Body)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return err
	}

	return nil
}

type fileReader struct {
	*os.File
}

type FileObject struct {
	Key  string
	Size int64
}

type LocalListObjectsPaginator struct {
	Files []ObjectAttr
	//PageSize    int
	//TotalPages  int
	//CurrentPage int
	currentFile int
}

func (p *LocalListObjectsPaginator) HasMorePages() bool {
	return p.currentFile < len(p.Files)
}

func (p *LocalListObjectsPaginator) NextPage(_ context.Context) (*Page, error) {
	if p.currentFile >= len(p.Files) {
		return nil, errors.New("storage: no more pages")
	}

	contents := make([]ObjectAttr, 0)
	obj := p.Files[p.currentFile]
	contents = append(contents, obj)
	p.currentFile = p.currentFile + 1

	return &Page{Contents: contents}, nil
}

func (lcm *LocalChunkManager) GetObject(ctx context.Context, bucket, key string) (*Object, error) {
	// Open the local file
	f, err := os.Open(key)
	if err != nil {
		return nil, err
	}
	size, err := lcm.Size(ctx, bucket, key)
	if err != nil {
		return nil, err
	}

	// Wrap the file in the SeekableReadCloser interface
	var src SeekableReadCloser = &fileReader{f}
	return &Object{Length: size, Body: src}, nil
}

func (lcm *LocalChunkManager) HeadObject(ctx context.Context, bucket, key string) (ObjectAttr, error) {
	size, err := lcm.Size(ctx, bucket, key)
	if err != nil {
		return ObjectAttr{}, err
	}
	return ObjectAttr{Key: key, Length: size}, nil
}

func (lcm *LocalChunkManager) ListObjectsPage(ctx context.Context, bucket, prefix string) (ListObjectsPaginator, error) {
	files, sizes, err := lcm.ListWithPrefix(ctx, bucket, prefix, true)
	if err != nil {
		return nil, err
	}

	// Create file objects to simulate S3 objects
	fileObjects := []ObjectAttr{}
	for i, fileKey := range files {
		fileObjects = append(fileObjects, ObjectAttr{
			Key:    fileKey,
			Length: sizes[i],
		})
	}

	paginator := &LocalListObjectsPaginator{
		Files:       fileObjects,
		currentFile: 0,
	}

	return paginator, nil
}

func CopyDir(source string, dest string) (err error) {
	// get properties of source dir
	sourceinfo, err := os.Stat(source)
	if err != nil {
		return err
	}

	// create dest dir
	err = os.MkdirAll(dest, sourceinfo.Mode())
	if err != nil {
		return err
	}

	directory, _ := os.Open(source)
	objects, err := directory.Readdir(-1)

	for _, obj := range objects {
		sourcefilepointer := source + "/" + obj.Name()
		destinationfilepointer := dest + "/" + obj.Name()
		if obj.IsDir() {
			// create sub-directories - recursively
			err = CopyDir(sourcefilepointer, destinationfilepointer)
			if err != nil {
				fmt.Println(err)
			}
		} else {
			// perform copy
			err = CopyFile(sourcefilepointer, destinationfilepointer)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
	return
}

func CopyFile(source string, dest string) (err error) {

	// get properties of source parent dir
	sourceParentDir := filepath.Dir(source)
	sourceParentDirInfo, err := os.Stat(sourceParentDir)
	if err != nil {
		return err
	}
	// create dest parent dir
	destParentDir := filepath.Dir(dest)
	err = os.MkdirAll(destParentDir, sourceParentDirInfo.Mode())
	if err != nil {
		return err
	}

	sourcefile, err := os.Open(source)
	if err != nil {
		return err
	}

	defer sourcefile.Close()
	destfile, err := os.Create(dest)
	if err != nil {
		return err
	}

	defer destfile.Close()
	_, err = io.Copy(destfile, sourcefile)
	if err == nil {
		sourceinfo, err := os.Stat(source)
		if err != nil {
			err = os.Chmod(dest, sourceinfo.Mode())
		}
	}
	return
}

// WriteFile writes file as os.WriteFile works，
// also converts the os errors to Milvus errors
func WriteFile(filePath string, data []byte, perm fs.FileMode) error {
	// NOLINT
	err := os.WriteFile(filePath, data, perm)
	if err != nil {
		return WrapErrFileNotFound(filePath)
	}
	return nil
}

// ReadFile reads file as os.ReadFile works,
// also converts the os errors to Milvus errors
func ReadFile(filePath string) ([]byte, error) {
	// NOLINT
	data, err := os.ReadFile(filePath)
	if os.IsNotExist(err) {
		return nil, WrapErrFileNotFound(filePath)
	} else if err != nil {
		return nil, err
	}

	return data, nil
}
