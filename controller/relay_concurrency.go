package controller

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/logger"
	relayhelper "github.com/QuantumNous/new-api/relay/helper"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/types"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

const (
	relayWaitInitialBackoff = 100 * time.Millisecond
	relayWaitBackoffFactor  = 1.5
	relayWaitMaxBackoff     = 2 * time.Second

	relayWaitStreamStartedKey = "user_concurrency_wait_stream_started"
)

func acquireUserConcurrencySlot(c *gin.Context, relayFormat types.RelayFormat, relayInfo *typesafeRelayInfo, ws *websocket.Conn) (func(), bool) {
	userConcurrency := common.GetContextKeyInt(c, constant.ContextKeyUserConcurrency)
	if userConcurrency <= 0 {
		userConcurrency = service.GetConcurrencyForNewUser()
	}

	svc := service.NewUserConcurrencyService(nil)
	requestID := relayInfo.RequestId
	ctx := c.Request.Context()

	if relayFormat == types.RelayFormatOpenAIRealtime {
		acquired, err := svc.AcquireUserSlot(ctx, relayInfo.UserId, userConcurrency, requestID)
		if err != nil {
			writeUserConcurrencyError(c, relayFormat, ws, newRelayConcurrencyError("failed to acquire user concurrency slot"), false)
			return nil, false
		}
		if !acquired {
			writeUserConcurrencyError(c, relayFormat, ws, newRelayConcurrencyError("too many concurrent requests, please retry later"), false)
			return nil, false
		}
		return wrapUserConcurrencyRelease(ctx, func() {
			bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := svc.ReleaseUserSlot(bgCtx, relayInfo.UserId, requestID); err != nil {
				logger.LogWarn(c, fmt.Sprintf("failed to release realtime user concurrency slot: %v", err))
			}
		}), true
	}

	acquired, err := svc.AcquireUserSlot(ctx, relayInfo.UserId, userConcurrency, requestID)
	if err != nil {
		writeUserConcurrencyError(c, relayFormat, ws, newRelayConcurrencyError("failed to acquire user concurrency slot"), false)
		return nil, false
	}
	if acquired {
		return wrapUserConcurrencyRelease(ctx, func() {
			bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := svc.ReleaseUserSlot(bgCtx, relayInfo.UserId, requestID); err != nil {
				logger.LogWarn(c, fmt.Sprintf("failed to release user concurrency slot: %v", err))
			}
		}), true
	}

	maxWait := service.CalculateUserConcurrencyMaxWait(userConcurrency)
	waitCounted := false
	canWait, err := svc.IncrementWaitCount(ctx, relayInfo.UserId, maxWait)
	if err != nil {
		logger.LogWarn(c, fmt.Sprintf("increment user concurrency wait count failed: %v", err))
	} else if !canWait {
		writeUserConcurrencyError(c, relayFormat, ws, newRelayPendingQueueError("Too many pending requests, please retry later"), false)
		return nil, false
	} else {
		waitCounted = true
	}
	defer func() {
		if waitCounted {
			bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := svc.DecrementWaitCount(bgCtx, relayInfo.UserId); err != nil {
				logger.LogWarn(c, fmt.Sprintf("decrement user concurrency wait count failed: %v", err))
			}
		}
	}()

	streamStarted := false
	releaseFunc, waitErr := waitForUserConcurrencySlot(c, relayFormat, relayInfo, svc, userConcurrency, &streamStarted)
	if waitErr != nil {
		writeUserConcurrencyError(c, relayFormat, ws, waitErr, streamStarted)
		return nil, false
	}
	if waitCounted {
		bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := svc.DecrementWaitCount(bgCtx, relayInfo.UserId); err != nil {
			logger.LogWarn(c, fmt.Sprintf("decrement user concurrency wait count failed after acquire: %v", err))
		}
		waitCounted = false
	}
	return releaseFunc, true
}

type typesafeRelayInfo struct {
	RequestId string
	UserId    int
	IsStream  bool
}

func waitForUserConcurrencySlot(
	c *gin.Context,
	relayFormat types.RelayFormat,
	relayInfo *typesafeRelayInfo,
	svc *service.UserConcurrencyService,
	userConcurrency int,
	streamStarted *bool,
) (func(), *types.NewAPIError) {
	timeoutCtx, cancel := context.WithTimeout(c.Request.Context(), service.GetUserConcurrencyWaitTimeout())
	defer cancel()

	backoff := relayWaitInitialBackoff
	timer := time.NewTimer(backoff)
	defer timer.Stop()

	var pingCh <-chan time.Time
	if relayInfo.IsStream {
		pingTicker := time.NewTicker(service.GetUserConcurrencyPingInterval())
		defer pingTicker.Stop()
		pingCh = pingTicker.C
	}

	for {
		select {
		case <-timeoutCtx.Done():
			return nil, newRelayConcurrencyError("Concurrency limit exceeded for user, please retry later")
		case <-pingCh:
			if err := sendUserConcurrencyWaitPing(c, relayFormat, streamStarted); err != nil {
				return nil, newRelayConcurrencyError("failed to send concurrency wait ping")
			}
		case <-timer.C:
			acquired, err := svc.AcquireUserSlot(timeoutCtx, relayInfo.UserId, userConcurrency, relayInfo.RequestId)
			if err != nil {
				return nil, newRelayConcurrencyError("failed to acquire user concurrency slot")
			}
			if acquired {
				return wrapUserConcurrencyRelease(c.Request.Context(), func() {
					bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := svc.ReleaseUserSlot(bgCtx, relayInfo.UserId, relayInfo.RequestId); err != nil {
						logger.LogWarn(c, fmt.Sprintf("failed to release waited user concurrency slot: %v", err))
					}
				}), nil
			}

			backoff = nextRelayConcurrencyBackoff(backoff)
			timer.Reset(backoff)
		}
	}
}

func sendUserConcurrencyWaitPing(c *gin.Context, relayFormat types.RelayFormat, streamStarted *bool) error {
	if relayFormat == types.RelayFormatOpenAIRealtime {
		return nil
	}

	if !*streamStarted {
		relayhelper.SetEventStreamHeaders(c)
		*streamStarted = true
		c.Set(relayWaitStreamStartedKey, true)
	}

	if relayFormat == types.RelayFormatClaude {
		payload, err := common.Marshal(gin.H{"type": "ping"})
		if err != nil {
			return err
		}
		c.Render(-1, common.CustomEvent{Data: "event: ping\n"})
		c.Render(-1, common.CustomEvent{Data: "data: " + string(payload)})
		return relayhelper.FlushWriter(c)
	}

	if _, err := c.Writer.Write([]byte(":\n\n")); err != nil {
		return err
	}
	return relayhelper.FlushWriter(c)
}

func writeUserConcurrencyError(c *gin.Context, relayFormat types.RelayFormat, ws *websocket.Conn, err *types.NewAPIError, streamStarted bool) {
	if err == nil {
		return
	}
	apiErr := types.WithOpenAIError(types.OpenAIError{
		Message: common.MessageWithRequestId(err.Error(), c.GetString(common.RequestIdKey)),
		Type:    "rate_limit_error",
		Code:    "rate_limit_error",
	}, err.StatusCode)

	if streamStarted {
		writeStartedRelayStreamError(c, relayFormat, apiErr)
		return
	}

	switch relayFormat {
	case types.RelayFormatOpenAIRealtime:
		relayhelper.WssError(c, ws, apiErr.ToOpenAIError())
	case types.RelayFormatClaude:
		c.JSON(apiErr.StatusCode, gin.H{
			"type":  "error",
			"error": apiErr.ToClaudeError(),
		})
	default:
		c.JSON(apiErr.StatusCode, gin.H{
			"error": apiErr.ToOpenAIError(),
		})
	}
}

func writeStartedRelayStreamError(c *gin.Context, relayFormat types.RelayFormat, err *types.NewAPIError) {
	if relayFormat == types.RelayFormatClaude {
		claudeErr := err.ToClaudeError()
		if claudeErr.Message == "" {
			claudeErr.Message = err.Error()
		}
		claudeErr.Message = common.MessageWithRequestId(claudeErr.Message, c.GetString(common.RequestIdKey))
		payload, marshalErr := common.Marshal(gin.H{
			"type":  "error",
			"error": claudeErr,
		})
		if marshalErr == nil {
			c.Render(-1, common.CustomEvent{Data: "event: error\n"})
			c.Render(-1, common.CustomEvent{Data: "data: " + string(payload)})
			_ = relayhelper.FlushWriter(c)
		}
		return
	}

	openAIErr := err.ToOpenAIError()
	if openAIErr.Message == "" {
		openAIErr.Message = err.Error()
	}
	openAIErr.Message = common.MessageWithRequestId(openAIErr.Message, c.GetString(common.RequestIdKey))
	_ = relayhelper.ObjectData(c, gin.H{
		"error": openAIErr,
	})
}

func wrapUserConcurrencyRelease(ctx context.Context, releaseFunc func()) func() {
	if releaseFunc == nil {
		return nil
	}

	var once sync.Once
	var stop func() bool

	release := func() {
		once.Do(func() {
			if stop != nil {
				_ = stop()
			}
			releaseFunc()
		})
	}

	stop = context.AfterFunc(ctx, release)
	return release
}

func newRelayConcurrencyError(message string) *types.NewAPIError {
	return types.WithOpenAIError(types.OpenAIError{
		Message: message,
		Type:    "rate_limit_error",
		Code:    "rate_limit_error",
	}, http.StatusTooManyRequests)
}

func newRelayPendingQueueError(message string) *types.NewAPIError {
	return newRelayConcurrencyError(message)
}

func nextRelayConcurrencyBackoff(current time.Duration) time.Duration {
	next := time.Duration(float64(current) * relayWaitBackoffFactor)
	if next > relayWaitMaxBackoff {
		next = relayWaitMaxBackoff
	}
	jitter := 0.8 + rand.Float64()*0.4
	next = time.Duration(float64(next) * jitter)
	if next < relayWaitInitialBackoff {
		return relayWaitInitialBackoff
	}
	if next > relayWaitMaxBackoff {
		return relayWaitMaxBackoff
	}
	return next
}
