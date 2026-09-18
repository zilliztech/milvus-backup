package backup

import (
	"testing"
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
