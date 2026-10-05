package main

import (
	"context"
	"sync"
	"time"
)

type pausableTimeout struct {
	mu        sync.Mutex
	cancel    context.CancelCauseFunc
	timer     *time.Timer
	remaining time.Duration
	started   time.Time
	paused    bool
	stopped   bool
}

func newPausableTimeout(parent context.Context, duration time.Duration) (context.Context, *pausableTimeout) {
	ctx, cancel := context.WithCancelCause(parent)
	timeout := &pausableTimeout{cancel: cancel, remaining: duration, started: time.Now()}
	timeout.timer = time.AfterFunc(duration, func() { cancel(context.DeadlineExceeded) })
	return ctx, timeout
}

func (timeout *pausableTimeout) Pause() {
	timeout.mu.Lock()
	defer timeout.mu.Unlock()
	if timeout.paused || timeout.stopped {
		return
	}
	if timeout.timer.Stop() {
		timeout.remaining -= time.Since(timeout.started)
	}
	if timeout.remaining < 0 {
		timeout.remaining = 0
	}
	timeout.paused = true
}

func (timeout *pausableTimeout) Resume() {
	timeout.mu.Lock()
	defer timeout.mu.Unlock()
	if !timeout.paused || timeout.stopped {
		return
	}
	timeout.paused = false
	if timeout.remaining <= 0 {
		timeout.cancel(context.DeadlineExceeded)
		return
	}
	timeout.started = time.Now()
	timeout.timer.Reset(timeout.remaining)
}

func (timeout *pausableTimeout) Stop() {
	timeout.mu.Lock()
	defer timeout.mu.Unlock()
	if timeout.stopped {
		return
	}
	timeout.stopped = true
	timeout.timer.Stop()
	timeout.cancel(context.Canceled)
}

func (timeout *pausableTimeout) Remaining() time.Duration {
	timeout.mu.Lock()
	defer timeout.mu.Unlock()
	if timeout.paused {
		return timeout.remaining
	}
	remaining := timeout.remaining - time.Since(timeout.started)
	if remaining < 0 {
		return 0
	}
	return remaining
}
