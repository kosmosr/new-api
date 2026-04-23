package service

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/go-redis/redis/v8"
)

const (
	userConcurrencySlotKeyPrefix = "concurrency:user:"
	userConcurrencyWaitKeyPrefix = "concurrency:wait:"

	defaultConcurrencyForNewUser         = 5
	defaultUserConcurrencySlotTTLMinutes = 30
	defaultUserConcurrencyWaitTimeoutSec = 30
	defaultUserConcurrencyPingInterval   = 10
	defaultUserConcurrencyWaitExtraSlots = 20
)

var (
	acquireUserSlotScript = redis.NewScript(`
		local key = KEYS[1]
		local maxConcurrency = tonumber(ARGV[1])
		local ttl = tonumber(ARGV[2])
		local requestID = ARGV[3]

		local timeResult = redis.call('TIME')
		local now = tonumber(timeResult[1])
		local expireBefore = now - ttl

		redis.call('ZREMRANGEBYSCORE', key, '-inf', expireBefore)

		local exists = redis.call('ZSCORE', key, requestID)
		if exists ~= false then
			redis.call('ZADD', key, now, requestID)
			redis.call('EXPIRE', key, ttl)
			return 1
		end

		local count = redis.call('ZCARD', key)
		if count < maxConcurrency then
			redis.call('ZADD', key, now, requestID)
			redis.call('EXPIRE', key, ttl)
			return 1
		end

		return 0
	`)

	getUserConcurrencyCountScript = redis.NewScript(`
		local key = KEYS[1]
		local ttl = tonumber(ARGV[1])

		local timeResult = redis.call('TIME')
		local now = tonumber(timeResult[1])
		local expireBefore = now - ttl

		redis.call('ZREMRANGEBYSCORE', key, '-inf', expireBefore)
		return redis.call('ZCARD', key)
	`)

	incrementUserWaitCountScript = redis.NewScript(`
		local current = redis.call('GET', KEYS[1])
		if current == false then
			current = 0
		else
			current = tonumber(current)
		end

		if current >= tonumber(ARGV[1]) then
			return 0
		end

		redis.call('INCR', KEYS[1])
		redis.call('EXPIRE', KEYS[1], ARGV[2])
		return 1
	`)

	decrementUserWaitCountScript = redis.NewScript(`
		local current = redis.call('GET', KEYS[1])
		if current ~= false and tonumber(current) > 0 then
			local newVal = redis.call('DECR', KEYS[1])
			if tonumber(newVal) <= 0 then
				redis.call('DEL', KEYS[1])
			end
		end
		return 1
	`)

	sharedInMemoryUserConcurrencyCache = NewInMemoryUserConcurrencyCache()
)

type UserConcurrencyLoadTarget struct {
	UserID         int
	MaxConcurrency int
}

type UserConcurrencyLoadInfo struct {
	UserID             int `json:"user_id"`
	CurrentConcurrency int `json:"current_concurrency"`
	WaitingCount       int `json:"waiting_count"`
	LoadRate           int `json:"load_rate"`
}

type UserConcurrencyCache interface {
	AcquireUserSlot(ctx context.Context, userID int, maxConcurrency int, requestID string, slotTTLSeconds int) (bool, error)
	ReleaseUserSlot(ctx context.Context, userID int, requestID string) error
	GetUserConcurrency(ctx context.Context, userID int, slotTTLSeconds int) (int, error)
	IncrementWaitCount(ctx context.Context, userID int, maxWait int, waitTTLSeconds int) (bool, error)
	DecrementWaitCount(ctx context.Context, userID int) error
	GetUsersLoadBatch(ctx context.Context, users []UserConcurrencyLoadTarget, slotTTLSeconds int, waitTTLSeconds int) (map[int]*UserConcurrencyLoadInfo, error)
}

type UserConcurrencyService struct {
	cache UserConcurrencyCache
}

func NewUserConcurrencyService(cache UserConcurrencyCache) *UserConcurrencyService {
	if cache == nil {
		cache = defaultUserConcurrencyCache()
	}
	return &UserConcurrencyService{cache: cache}
}

func defaultUserConcurrencyCache() UserConcurrencyCache {
	if common.RedisEnabled && common.RDB != nil {
		return NewRedisUserConcurrencyCache(common.RDB)
	}
	return sharedInMemoryUserConcurrencyCache
}

func GetConcurrencyForNewUser() int {
	if common.ConcurrencyForNewUser > 0 {
		return common.ConcurrencyForNewUser
	}
	return defaultConcurrencyForNewUser
}

func GetUserConcurrencySlotTTLSeconds() int {
	minutes := common.UserConcurrencySlotTTLMinutes
	if minutes <= 0 {
		minutes = defaultUserConcurrencySlotTTLMinutes
	}
	return minutes * 60
}

func GetUserConcurrencyWaitTimeout() time.Duration {
	seconds := common.UserConcurrencyWaitTimeoutSeconds
	if seconds <= 0 {
		seconds = defaultUserConcurrencyWaitTimeoutSec
	}
	return time.Duration(seconds) * time.Second
}

func GetUserConcurrencyPingInterval() time.Duration {
	seconds := common.UserConcurrencyPingIntervalSeconds
	if seconds <= 0 {
		seconds = defaultUserConcurrencyPingInterval
	}
	return time.Duration(seconds) * time.Second
}

func GetUserConcurrencyWaitExtraSlots() int {
	if common.UserConcurrencyWaitExtraSlots >= 0 {
		return common.UserConcurrencyWaitExtraSlots
	}
	return defaultUserConcurrencyWaitExtraSlots
}

func CalculateUserConcurrencyMaxWait(maxConcurrency int) int {
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}
	return maxConcurrency + GetUserConcurrencyWaitExtraSlots()
}

func (s *UserConcurrencyService) AcquireUserSlot(ctx context.Context, userID int, maxConcurrency int, requestID string) (bool, error) {
	if maxConcurrency <= 0 {
		return true, nil
	}
	if userID <= 0 {
		return false, errors.New("invalid user id")
	}
	if requestID == "" {
		requestID = common.GetTimeString() + common.GetRandomString(8)
	}
	return s.cache.AcquireUserSlot(ctx, userID, maxConcurrency, requestID, GetUserConcurrencySlotTTLSeconds())
}

func (s *UserConcurrencyService) ReleaseUserSlot(ctx context.Context, userID int, requestID string) error {
	if userID <= 0 || requestID == "" {
		return nil
	}
	return s.cache.ReleaseUserSlot(ctx, userID, requestID)
}

func (s *UserConcurrencyService) GetUserConcurrency(ctx context.Context, userID int) (int, error) {
	if userID <= 0 {
		return 0, nil
	}
	return s.cache.GetUserConcurrency(ctx, userID, GetUserConcurrencySlotTTLSeconds())
}

func (s *UserConcurrencyService) IncrementWaitCount(ctx context.Context, userID int, maxWait int) (bool, error) {
	if userID <= 0 || maxWait <= 0 {
		return true, nil
	}
	return s.cache.IncrementWaitCount(ctx, userID, maxWait, GetUserConcurrencySlotTTLSeconds())
}

func (s *UserConcurrencyService) DecrementWaitCount(ctx context.Context, userID int) error {
	if userID <= 0 {
		return nil
	}
	return s.cache.DecrementWaitCount(ctx, userID)
}

func (s *UserConcurrencyService) GetUsersLoadBatch(ctx context.Context, users []UserConcurrencyLoadTarget) (map[int]*UserConcurrencyLoadInfo, error) {
	if len(users) == 0 {
		return map[int]*UserConcurrencyLoadInfo{}, nil
	}
	return s.cache.GetUsersLoadBatch(ctx, users, GetUserConcurrencySlotTTLSeconds(), GetUserConcurrencySlotTTLSeconds())
}

type RedisUserConcurrencyCache struct {
	rdb *redis.Client
}

func NewRedisUserConcurrencyCache(rdb *redis.Client) *RedisUserConcurrencyCache {
	return &RedisUserConcurrencyCache{rdb: rdb}
}

func (c *RedisUserConcurrencyCache) AcquireUserSlot(ctx context.Context, userID int, maxConcurrency int, requestID string, slotTTLSeconds int) (bool, error) {
	result, err := acquireUserSlotScript.Run(ctx, c.rdb, []string{userConcurrencySlotKey(userID)}, maxConcurrency, slotTTLSeconds, requestID).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (c *RedisUserConcurrencyCache) ReleaseUserSlot(ctx context.Context, userID int, requestID string) error {
	return c.rdb.ZRem(ctx, userConcurrencySlotKey(userID), requestID).Err()
}

func (c *RedisUserConcurrencyCache) GetUserConcurrency(ctx context.Context, userID int, slotTTLSeconds int) (int, error) {
	return getUserConcurrencyCountScript.Run(ctx, c.rdb, []string{userConcurrencySlotKey(userID)}, slotTTLSeconds).Int()
}

func (c *RedisUserConcurrencyCache) IncrementWaitCount(ctx context.Context, userID int, maxWait int, waitTTLSeconds int) (bool, error) {
	result, err := incrementUserWaitCountScript.Run(ctx, c.rdb, []string{userConcurrencyWaitKey(userID)}, maxWait, waitTTLSeconds).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (c *RedisUserConcurrencyCache) DecrementWaitCount(ctx context.Context, userID int) error {
	_, err := decrementUserWaitCountScript.Run(ctx, c.rdb, []string{userConcurrencyWaitKey(userID)}).Result()
	return err
}

func (c *RedisUserConcurrencyCache) GetUsersLoadBatch(ctx context.Context, users []UserConcurrencyLoadTarget, slotTTLSeconds int, _ int) (map[int]*UserConcurrencyLoadInfo, error) {
	if len(users) == 0 {
		return map[int]*UserConcurrencyLoadInfo{}, nil
	}

	now, err := c.rdb.Time(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("redis TIME: %w", err)
	}
	cutoffTime := now.Unix() - int64(slotTTLSeconds)

	type userCmds struct {
		userID         int
		maxConcurrency int
		zcardCmd       *redis.IntCmd
		getCmd         *redis.StringCmd
	}

	pipe := c.rdb.Pipeline()
	cmds := make([]userCmds, 0, len(users))
	for _, user := range users {
		slotKey := userConcurrencySlotKey(user.UserID)
		waitKey := userConcurrencyWaitKey(user.UserID)
		pipe.ZRemRangeByScore(ctx, slotKey, "-inf", strconv.FormatInt(cutoffTime, 10))
		cmds = append(cmds, userCmds{
			userID:         user.UserID,
			maxConcurrency: user.MaxConcurrency,
			zcardCmd:       pipe.ZCard(ctx, slotKey),
			getCmd:         pipe.Get(ctx, waitKey),
		})
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	loadMap := make(map[int]*UserConcurrencyLoadInfo, len(users))
	for _, cmd := range cmds {
		waitingCount := 0
		if value, err := cmd.getCmd.Int(); err == nil {
			waitingCount = value
		}
		loadRate := 0
		currentConcurrency := int(cmd.zcardCmd.Val())
		if cmd.maxConcurrency > 0 {
			loadRate = (currentConcurrency + waitingCount) * 100 / cmd.maxConcurrency
		}
		loadMap[cmd.userID] = &UserConcurrencyLoadInfo{
			UserID:             cmd.userID,
			CurrentConcurrency: currentConcurrency,
			WaitingCount:       waitingCount,
			LoadRate:           loadRate,
		}
	}
	return loadMap, nil
}

type waitCounterEntry struct {
	Count     int
	ExpiresAt int64
}

type InMemoryUserConcurrencyCache struct {
	mu          sync.Mutex
	userSlots   map[int]map[string]int64
	waitCounter map[int]waitCounterEntry
}

func NewInMemoryUserConcurrencyCache() *InMemoryUserConcurrencyCache {
	return &InMemoryUserConcurrencyCache{
		userSlots:   make(map[int]map[string]int64),
		waitCounter: make(map[int]waitCounterEntry),
	}
}

func (c *InMemoryUserConcurrencyCache) AcquireUserSlot(_ context.Context, userID int, maxConcurrency int, requestID string, slotTTLSeconds int) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().Unix()
	c.cleanupUserSlotsLocked(userID, now, slotTTLSeconds)

	if c.userSlots[userID] == nil {
		c.userSlots[userID] = make(map[string]int64)
	}
	if _, exists := c.userSlots[userID][requestID]; exists {
		c.userSlots[userID][requestID] = now
		return true, nil
	}
	if len(c.userSlots[userID]) >= maxConcurrency {
		return false, nil
	}

	c.userSlots[userID][requestID] = now
	return true, nil
}

func (c *InMemoryUserConcurrencyCache) ReleaseUserSlot(_ context.Context, userID int, requestID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if userSlots, ok := c.userSlots[userID]; ok {
		delete(userSlots, requestID)
		if len(userSlots) == 0 {
			delete(c.userSlots, userID)
		}
	}
	return nil
}

func (c *InMemoryUserConcurrencyCache) GetUserConcurrency(_ context.Context, userID int, slotTTLSeconds int) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().Unix()
	c.cleanupUserSlotsLocked(userID, now, slotTTLSeconds)
	return len(c.userSlots[userID]), nil
}

func (c *InMemoryUserConcurrencyCache) IncrementWaitCount(_ context.Context, userID int, maxWait int, waitTTLSeconds int) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().Unix()
	c.cleanupWaitCounterLocked(userID, now)
	entry := c.waitCounter[userID]
	if entry.Count >= maxWait {
		return false, nil
	}

	entry.Count++
	entry.ExpiresAt = now + int64(waitTTLSeconds)
	c.waitCounter[userID] = entry
	return true, nil
}

func (c *InMemoryUserConcurrencyCache) DecrementWaitCount(_ context.Context, userID int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().Unix()
	c.cleanupWaitCounterLocked(userID, now)
	entry, ok := c.waitCounter[userID]
	if !ok {
		return nil
	}
	entry.Count--
	if entry.Count <= 0 {
		delete(c.waitCounter, userID)
		return nil
	}
	c.waitCounter[userID] = entry
	return nil
}

func (c *InMemoryUserConcurrencyCache) GetUsersLoadBatch(_ context.Context, users []UserConcurrencyLoadTarget, slotTTLSeconds int, _ int) (map[int]*UserConcurrencyLoadInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().Unix()
	loadMap := make(map[int]*UserConcurrencyLoadInfo, len(users))
	for _, user := range users {
		c.cleanupUserSlotsLocked(user.UserID, now, slotTTLSeconds)
		c.cleanupWaitCounterLocked(user.UserID, now)

		currentConcurrency := len(c.userSlots[user.UserID])
		waitingCount := c.waitCounter[user.UserID].Count
		loadRate := 0
		if user.MaxConcurrency > 0 {
			loadRate = (currentConcurrency + waitingCount) * 100 / user.MaxConcurrency
		}

		loadMap[user.UserID] = &UserConcurrencyLoadInfo{
			UserID:             user.UserID,
			CurrentConcurrency: currentConcurrency,
			WaitingCount:       waitingCount,
			LoadRate:           loadRate,
		}
	}
	return loadMap, nil
}

func (c *InMemoryUserConcurrencyCache) cleanupUserSlotsLocked(userID int, now int64, slotTTLSeconds int) {
	userSlots, ok := c.userSlots[userID]
	if !ok {
		return
	}
	expireBefore := now - int64(slotTTLSeconds)
	for requestID, seenAt := range userSlots {
		if seenAt <= expireBefore {
			delete(userSlots, requestID)
		}
	}
	if len(userSlots) == 0 {
		delete(c.userSlots, userID)
	}
}

func (c *InMemoryUserConcurrencyCache) cleanupWaitCounterLocked(userID int, now int64) {
	entry, ok := c.waitCounter[userID]
	if !ok {
		return
	}
	if entry.ExpiresAt <= now {
		delete(c.waitCounter, userID)
	}
}

func userConcurrencySlotKey(userID int) string {
	return fmt.Sprintf("%s%d", userConcurrencySlotKeyPrefix, userID)
}

func userConcurrencyWaitKey(userID int) string {
	return fmt.Sprintf("%s%d", userConcurrencyWaitKeyPrefix, userID)
}
