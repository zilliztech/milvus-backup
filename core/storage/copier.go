package storage

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/zilliztech/milvus-backup/internal/common"
	"github.com/zilliztech/milvus-backup/internal/log"
)

const (
	_32M  = 32 << 20
	_100M = 100 << 20
)

const _copyWorkerNum = 10

// limReader speed limit reader
type limReader struct {
	r   io.Reader
	lim *rate.Limiter
	ctx context.Context
}

func (r *limReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if err != nil {
		return n, err
	}

	if err := r.lim.WaitN(r.ctx, n); err != nil {
		return n, err
	}

	return n, err
}

type CopyOption struct {
	// BytePS byte/s copy speed limit, 0 is unlimited, default is unlimited
	BytePS float64
	// WorkerNum the number of copy task worker, default is 10
	WorkerNum int
	// RPS the number of copy requests initiated per second, 0 is unlimited, default is unlimited
	RPS int32
	// BufSizeByte the size of the buffer that the copier can use, default is 100MB
	BufSizeByte int
	// CopyByServer copy data through server when can't copy by client directly
	CopyByServer bool
}

type Copier struct {
	src  ChunkManager
	dest ChunkManager

	// lim stream copy speed limiter
	lim                  *rate.Limiter
	workerNum            int
	bufSizeBytePerWorker int
	rps                  int32

	totalSize atomic.Uint64
	totalCnt  atomic.Uint64

	size atomic.Uint64
	cnt  atomic.Uint64

	useCopyByServer bool
}

func NewCopier(src, dest ChunkManager, opt CopyOption) *Copier {
	var lim *rate.Limiter
	if opt.BytePS != 0 {
		lim = rate.NewLimiter(rate.Limit(opt.BytePS), _32M)
	}

	workerNum := _copyWorkerNum
	if opt.WorkerNum != 0 {
		workerNum = opt.WorkerNum
	}
	bufSizeBytePerWorker := _100M / workerNum
	if opt.BufSizeByte != 0 {
		bufSizeBytePerWorker = opt.BufSizeByte / workerNum
	}

	return &Copier{
		src:                  src,
		dest:                 dest,
		lim:                  lim,
		useCopyByServer:      opt.CopyByServer,
		workerNum:            workerNum,
		bufSizeBytePerWorker: bufSizeBytePerWorker,
	}
}

type Process struct {
	TotalSize uint64
	TotalCnt  uint64

	Size uint64
	Cnt  uint64
}

func (c *Copier) Process() Process {
	return Process{
		TotalSize: c.totalSize.Load(),
		TotalCnt:  c.totalCnt.Load(),

		Size: c.size.Load(),
		Cnt:  c.cnt.Load(),
	}
}

type CopyPathInput struct {
	SrcBucket string
	SrcPrefix string

	DestBucket string
	DestKeyFn  func(attr ObjectAttr) string

	// optional
	CopySuffix string

	// OnSuccess when an object copy success, this func will be call
	// May be executed concurrently, please pay attention to thread safety
	OnSuccess func(attr ObjectAttr)
}

// getAttrs get all attrs under bucket/prefix
func (c *Copier) getAttrs(ctx context.Context, bucket, prefix string, copySuffix string) ([]ObjectAttr, error) {
	var attrs []ObjectAttr

	paths, sizes, err := c.src.ListWithPrefix(ctx, bucket, prefix, true)
	if err != nil {
		return nil, err
	}

	for i, path := range paths {
		attrs = append(attrs, ObjectAttr{Key: path, Length: sizes[i]})
		c.totalSize.Add(uint64(sizes[i]))
		c.cnt.Add(1)
	}

	return attrs, nil
}

func (c *Copier) getAttrs2(ctx context.Context, bucket, prefix string, copySuffix string) ([]ObjectAttr, error) {
	var attrs []ObjectAttr

	p, err := c.src.ListObjectsPage(ctx, bucket, prefix)
	if err != nil {
		return nil, err
	}
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("storage: copier list objects %w", err)
		}
		for _, attr := range page.Contents {
			if attr.IsEmpty() {
				continue
			}

			if copySuffix != "" && !strings.HasSuffix(attr.Key, copySuffix) {
				continue
			}

			attrs = append(attrs, attr)
			c.totalSize.Add(uint64(attr.Length))
			c.cnt.Add(1)
		}
	}

	return attrs, nil
}

// CopyPrefix Copy all files under src path
func (c *Copier) CopyPrefix(ctx context.Context, i CopyPathInput) error {
	srcAttrs, err := c.getAttrs(ctx, i.SrcBucket, i.SrcPrefix, i.CopySuffix)
	if err != nil {
		return fmt.Errorf("storage: copier get src attrs %w", err)
	}

	wp, err := common.NewWorkerPool(ctx, c.workerNum, c.rps)
	if err != nil {
		return fmt.Errorf("storage: copier new worker pool %w", err)
	}
	wp.Start()
	fn := c.selectCopyFn()
	for _, srcAttr := range srcAttrs {
		attr := srcAttr
		job := func(ctx context.Context) error {
			destKey := i.DestKeyFn(attr)
			// copy
			destAttr, err := c.dest.HeadObject(ctx, i.DestBucket, destKey)
			if err != nil || !attr.SameAs(destAttr) {
				if err := fn(ctx, attr, destKey, i.SrcBucket, i.DestBucket); err != nil {
					return fmt.Errorf("storage: copier copy object %w", err)
				}
			}
			// check
			destAttr, err = c.dest.HeadObject(ctx, i.DestBucket, destKey)
			if err != nil {
				return fmt.Errorf("storage: after copy  %w", err)
			}
			if destAttr.Length != attr.Length {
				return fmt.Errorf("storage: dest len %d != src len %d", destAttr.Length, attr.Length)
			}

			if i.OnSuccess != nil {
				i.OnSuccess(attr)
			}
			c.cnt.Add(1)

			return nil
		}

		wp.Submit(job)
	}
	wp.Done()

	if err := wp.Wait(); err != nil {
		return fmt.Errorf("storage: copier copy prefix %w", err)
	}
	return nil
}

func (c *Copier) Copy(ctx context.Context, srcPrefix, destPrefix, srcBucket, destBucket string) error {
	fn := c.selectCopyFn()
	srcAttrs, err := c.getAttrs(ctx, srcBucket, srcPrefix, "")
	if err != nil {
		return fmt.Errorf("storage: copier get src attrs %w", err)
	}
	for _, srcAttr := range srcAttrs {
		destKey := strings.Replace(srcAttr.Key, srcPrefix, destPrefix, 1)
		err := fn(ctx, srcAttr, destKey, srcBucket, destBucket)
		if err != nil {
			return err
		}
	}
	return nil
}

type copyFn func(ctx context.Context, attr ObjectAttr, destKey, srcBucket, destBucket string) error

func (c *Copier) selectCopyFn() copyFn {
	if c.useCopyByServer {
		return c.copyByServer
	}
	return c.copyRemote
}

func (c *Copier) copyRemote(ctx context.Context, attr ObjectAttr, destKey, srcBucket, destBucket string) error {
	log.Debug("copyRemote", zap.String("srcBucket", srcBucket), zap.String("destBucket", destBucket), zap.String("key", attr.Key), zap.String("destKey", destKey))
	if err := c.dest.Copy(ctx, srcBucket, destBucket, attr.Key, destKey); err != nil {
		return fmt.Errorf("storage: copier copy object %w", err)
	}

	return nil
}

func (c *Copier) copyByServer(ctx context.Context, attr ObjectAttr, destKey, srcBucket, destBucket string) error {
	log.Debug("copyByServer", zap.String("srcBucket", srcBucket), zap.String("destBucket", destBucket), zap.String("key", attr.Key), zap.String("destKey", destKey))
	obj, err := c.src.GetObject(ctx, srcBucket, attr.Key)
	if err != nil {
		log.Warn("storage: copier get object", zap.String("bucket", srcBucket), zap.String("key", attr.Key), zap.Error(err))
		return err
	}
	defer obj.Body.Close()

	body := c.newProcessReader(bufio.NewReaderSize(obj.Body, c.bufSizeBytePerWorker))
	if c.lim != nil {
		body = &limReader{r: body, lim: c.lim, ctx: ctx}
	}
	i := UploadObjectInput{Body: body, Bucket: destBucket, Key: destKey, Size: attr.Length}
	if err := c.dest.UploadObject(ctx, i); err != nil {
		log.Warn("storage: copier upload object", zap.String("bucket", destBucket), zap.String("key", destKey), zap.Error(err))
		return err
	}

	return nil
}

type processReader struct {
	src io.Reader
	len *atomic.Uint64
}

func (r *processReader) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	r.len.Add(uint64(n))
	return n, err
}

func (c *Copier) newProcessReader(src io.Reader) io.Reader {
	return &processReader{src: src, len: &c.size}
}
