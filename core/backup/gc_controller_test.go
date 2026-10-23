package backup

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/milvus-backup/internal/client/milvus"
)

func TestCollGCCtrl_RenewalGCLease(t *testing.T) {
	t.Run("RenewTicketAboutToExpire", func(t *testing.T) {
		manage := milvus.NewMockManage(t)
		manage.EXPECT().PauseGC(mock.Anything, mock.Anything).Return("new_ticket", nil).Once()

		ctrl := newCollGCCtrl("test_task", manage)
		ctrl.collIDTicket[1] = gcTicket{collectionID: 1, ticketID: "old_ticket", expire: time.Now().Add(time.Minute)}

		ctrl.renewalGCLease()

		// the renewed ticket must be stored, otherwise ResumeGC would send the stale one
		// and the pause records created by the renewal would stay until they expire.
		ticket := ctrl.collIDTicket[1]
		assert.Equal(t, "new_ticket", ticket.ticketID)
		assert.True(t, time.Until(ticket.expire) > 2*_gcRenewalInterval)
	})

	t.Run("SkipTicketNotAboutToExpire", func(t *testing.T) {
		// no PauseGC expectation, the mock fails the test if it is called
		manage := milvus.NewMockManage(t)

		ctrl := newCollGCCtrl("test_task", manage)
		expire := time.Now().Add(_defaultPauseDuration)
		ctrl.collIDTicket[1] = gcTicket{collectionID: 1, ticketID: "old_ticket", expire: expire}

		ctrl.renewalGCLease()

		ticket := ctrl.collIDTicket[1]
		assert.Equal(t, "old_ticket", ticket.ticketID)
		assert.Equal(t, expire, ticket.expire)
	})
}

func TestClusterGCCtrl_StopRenewal(t *testing.T) {
	t.Run("StopSignalSurvivesRenewalInFlight", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var renewals atomic.Int64
			firstRenewalStarted := make(chan struct{})
			gate := make(chan struct{})

			manage := milvus.NewMockManage(t)
			manage.EXPECT().PauseGC(mock.Anything, mock.Anything).Return("ticket", nil).Once()
			// exactly one renewal may happen: the one in flight while ResumeGC
			// signals the stop. A second one means the signal was dropped, and
			// the mock fails the test through its own unexpected-call path.
			// PauseGC's typed Run takes the unexported option type, so the hook
			// goes through Once's plain call.
			manage.EXPECT().PauseGC(mock.Anything, mock.Anything).Once().Run(func(args mock.Arguments) {
				close(firstRenewalStarted)
				<-gate // hold the renewal RPC while ResumeGC signals the stop
				renewals.Add(1)
			}).Return("ticket", nil)
			manage.EXPECT().ResumeGC(mock.Anything, mock.Anything).Return(nil).Once()

			ctrl := newClusterGCCtrl("test_task", manage)
			ctrl.PauseGC(context.Background())

			// resume while the renewal goroutine is inside the renewal RPC: the
			// stop signal must still reach it once the RPC returns.
			<-firstRenewalStarted
			ctrl.ResumeGC(context.Background())
			close(gate)

			// sleep past the next renewal tick: on a correct implementation the
			// goroutine has exited and the tick never fires; on a dropped signal
			// the tick fires a second renewal, which the mock rejects.
			time.Sleep(2 * _defaultPauseDuration)
			assert.EqualValues(t, 1, renewals.Load(), "renewal loop must stop after ResumeGC")
		})
	})
}
