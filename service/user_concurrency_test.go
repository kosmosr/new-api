package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/stretchr/testify/require"
)

func TestInMemoryUserConcurrencyAcquireRelease(t *testing.T) {
	cache := NewInMemoryUserConcurrencyCache()
	ctx := context.Background()

	ok, err := cache.AcquireUserSlot(ctx, 1, 1, "req-1", 60)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = cache.AcquireUserSlot(ctx, 1, 1, "req-2", 60)
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, cache.ReleaseUserSlot(ctx, 1, "req-1"))

	ok, err = cache.AcquireUserSlot(ctx, 1, 1, "req-2", 60)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestInMemoryUserConcurrencyRequestIDIsIdempotent(t *testing.T) {
	cache := NewInMemoryUserConcurrencyCache()
	ctx := context.Background()

	ok, err := cache.AcquireUserSlot(ctx, 2, 1, "req-same", 60)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = cache.AcquireUserSlot(ctx, 2, 1, "req-same", 60)
	require.NoError(t, err)
	require.True(t, ok)

	count, err := cache.GetUserConcurrency(ctx, 2, 60)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestInMemoryUserConcurrencySlotExpires(t *testing.T) {
	cache := NewInMemoryUserConcurrencyCache()
	ctx := context.Background()

	ok, err := cache.AcquireUserSlot(ctx, 3, 1, "req-1", 1)
	require.NoError(t, err)
	require.True(t, ok)

	time.Sleep(1100 * time.Millisecond)

	ok, err = cache.AcquireUserSlot(ctx, 3, 1, "req-2", 1)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestInMemoryUserConcurrencyWaitQueue(t *testing.T) {
	cache := NewInMemoryUserConcurrencyCache()
	ctx := context.Background()

	ok, err := cache.IncrementWaitCount(ctx, 9, 2, 60)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = cache.IncrementWaitCount(ctx, 9, 2, 60)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = cache.IncrementWaitCount(ctx, 9, 2, 60)
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, cache.DecrementWaitCount(ctx, 9))

	usersLoad, err := cache.GetUsersLoadBatch(ctx, []UserConcurrencyLoadTarget{
		{UserID: 9, MaxConcurrency: 2},
	}, 60, 60)
	require.NoError(t, err)
	require.Equal(t, 1, usersLoad[9].WaitingCount)
}

func TestCalculateUserConcurrencyMaxWait(t *testing.T) {
	oldExtraSlots := common.UserConcurrencyWaitExtraSlots
	common.UserConcurrencyWaitExtraSlots = 20
	t.Cleanup(func() {
		common.UserConcurrencyWaitExtraSlots = oldExtraSlots
	})
	require.Equal(t, 25, CalculateUserConcurrencyMaxWait(5))
	require.Equal(t, 21, CalculateUserConcurrencyMaxWait(0))
}

func TestUserInsertUsesDefaultConcurrency(t *testing.T) {
	truncate(t)

	oldDefault := common.ConcurrencyForNewUser
	common.ConcurrencyForNewUser = 7
	t.Cleanup(func() {
		common.ConcurrencyForNewUser = oldDefault
	})

	user := &model.User{
		Username: fmt.Sprintf("concurrency_user_%d", time.Now().UnixNano()),
		Password: "12345678",
		Role:     common.RoleCommonUser,
		Status:   common.UserStatusEnabled,
	}
	require.NoError(t, user.Insert(0))

	stored, err := model.GetUserById(user.Id, false)
	require.NoError(t, err)
	require.Equal(t, 7, stored.Concurrency)
}
