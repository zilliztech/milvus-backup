package backup

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
)

const _gcWarnMessage = "Pause GC failed: backup will continue. " +
	"This is not a fatal error — if pausing GC fails the backup process will still proceed because Milvus GC runs infrequently and is unlikely to affect the data being backed up." +
	"Pausing GC is only recommended for very large backups (takes > 1 hour)."

type gcCtrl interface {
	PauseGC(ctx context.Context)
	ResumeGC(ctx context.Context)

	PauseCollectionGC(ctx context.Context, collectionID int64)
	ResumeCollectionGC(ctx context.Context, collectionID int64)
}

type emptyGCCtrl struct{}

func (e *emptyGCCtrl) PauseGC(_ context.Context)                     {}
func (e *emptyGCCtrl) ResumeGC(_ context.Context)                    {}
func (e *emptyGCCtrl) PauseCollectionGC(_ context.Context, _ int64)  {}
func (e *emptyGCCtrl) ResumeCollectionGC(_ context.Context, _ int64) {}

var _defaultPauseDuration = 1 * time.Hour

var _ gcCtrl = (*clusterGCCtrl)(nil)

// clusterGCCtrl will pause and resume GC for the whole cluster.
type clusterGCCtrl struct {
	manage milvus.Manage

	stopGC chan struct{}

	logger *zap.Logger
}

func newClusterGCCtrl(taskID string, manage milvus.Manage) *clusterGCCtrl {
	return &clusterGCCtrl{
		manage: manage,
		stopGC: make(chan struct{}),
		logger: log.With(zap.String("task_id", taskID)),
	}
}

func (c *clusterGCCtrl) PauseGC(ctx context.Context) {
	resp, err := c.manage.PauseGC(ctx, milvus.WithPauseSeconds(int32(_defaultPauseDuration.Seconds())))
	if err != nil {
		c.logger.Warn(_gcWarnMessage, zap.Error(err), zap.String("resp", resp))
		return
	}

	c.logger.Info("pause gc success", zap.String("resp", resp))
	go c.renewalGCLease()
}

func (c *clusterGCCtrl) renewalGCLease() {
	for {
		select {
		case <-time.After(_defaultPauseDuration / 2):
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			resp, err := c.manage.PauseGC(ctx, milvus.WithPauseSeconds(int32(_defaultPauseDuration.Seconds())))
			if err != nil {
				c.logger.Warn("renewal pause gc lease failed", zap.Error(err), zap.String("resp", resp))
			}
			c.logger.Info("renewal pause gc lease done", zap.String("resp", resp))
			cancel()
		case <-c.stopGC:
			c.logger.Info("stop renewal pause gc lease")
			return
		}
	}
}

func (c *clusterGCCtrl) ResumeGC(ctx context.Context) {
	select {
	case c.stopGC <- struct{}{}:
	default:
		c.logger.Info("stop channel is full, maybe renewal lease goroutine not running")
	}

	if err := c.manage.ResumeGC(ctx); err != nil {
		c.logger.Warn("resume gc failed", zap.Error(err))
	}
}

// PauseCollectionGC is not implemented for clusterGCCtrl.
func (c *clusterGCCtrl) PauseCollectionGC(_ context.Context, _ int64) {}

// ResumeCollectionGC is not implemented for clusterGCCtrl.
func (c *clusterGCCtrl) ResumeCollectionGC(_ context.Context, _ int64) {}

type gcTicket struct {
	collectionID int64
	ticketID     string
	expire       time.Time
}

const _gcRenewalInterval = 5 * time.Minute

// collGCCtrl will pause and resume GC for a specific collection.
type collGCCtrl struct {
	manage milvus.Manage

	mu           sync.Mutex
	collIDTicket map[int64]gcTicket

	stopRenewal chan struct{}

	logger *zap.Logger
}

func newCollGCCtrl(taskID string, manage milvus.Manage) *collGCCtrl {
	return &collGCCtrl{
		manage:       manage,
		collIDTicket: make(map[int64]gcTicket),
		stopRenewal:  make(chan struct{}),
		logger:       log.With(zap.String("task_id", taskID)),
	}
}

var _ gcCtrl = (*collGCCtrl)(nil)

func (c *collGCCtrl) PauseGC(_ context.Context) {
	go c.loopRenew()
}

func (c *collGCCtrl) ResumeGC(_ context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for collID, ticket := range c.collIDTicket {
		c.logger.Info("resume collection gc", zap.Int64("collection_id", collID))
		err := c.manage.ResumeGC(context.Background(), milvus.WithTicket(ticket.ticketID))
		if err != nil {
			c.logger.Warn("resume collection gc failed", zap.Error(err))
			continue
		}
		delete(c.collIDTicket, collID)
	}

	close(c.stopRenewal)
}

func (c *collGCCtrl) loopRenew() {
	ticker := time.NewTicker(_gcRenewalInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.renewalGCLease()
		case <-c.stopRenewal:
			return
		}
	}
}

func (c *collGCCtrl) renewalGCLease() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c.mu.Lock()
	defer c.mu.Unlock()

	for collID, ticket := range c.collIDTicket {
		remaining := time.Until(ticket.expire)
		if remaining < 2*_gcRenewalInterval {
			c.logger.Info("gc ticket is about to expire, renewal it", zap.Int64("collection_id", collID))
			resp, err := c.manage.PauseGC(ctx, milvus.WithCollectionID(collID), milvus.WithPauseSeconds(int32(_defaultPauseDuration.Seconds())))
			if err != nil {
				c.logger.Warn("renewal pause gc lease failed", zap.Error(err), zap.String("resp", resp))
				continue
			}
			c.logger.Info("renewal pause gc lease done", zap.String("resp", resp))
			ticket.ticketID = resp
			ticket.expire = time.Now().Add(_defaultPauseDuration)
		}
	}
}

func (c *collGCCtrl) PauseCollectionGC(ctx context.Context, collectionID int64) {
	resp, err := c.manage.PauseGC(ctx, milvus.WithCollectionID(collectionID), milvus.WithPauseSeconds(int32(_defaultPauseDuration.Seconds())))
	if err != nil {
		c.logger.Warn(_gcWarnMessage, zap.Error(err), zap.Int64("collection_id", collectionID))
		return
	}
	if resp == "" {
		c.logger.Info("no ticket returned", zap.Int64("collection_id", collectionID))
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.collIDTicket[collectionID] = gcTicket{
		collectionID: collectionID,
		ticketID:     resp,
		expire:       time.Now().Add(_defaultPauseDuration),
	}

	c.logger.Info("pause collection gc done", zap.String("resp", resp))
}

func (c *collGCCtrl) ResumeCollectionGC(ctx context.Context, collectionID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ticket, ok := c.collIDTicket[collectionID]
	if !ok {
		c.logger.Info("no ticket found for collection, skip resume gc", zap.Int64("collection_id", collectionID))
		return
	}

	err := c.manage.ResumeGC(ctx, milvus.WithTicket(ticket.ticketID))
	if err != nil {
		c.logger.Warn("resume collection gc failed", zap.Error(err))
		return
	}
	delete(c.collIDTicket, collectionID)

	c.logger.Info("resume collection gc done", zap.Int64("collection_id", collectionID))
}
