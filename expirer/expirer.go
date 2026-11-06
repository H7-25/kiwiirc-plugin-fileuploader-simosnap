package expirer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/kiwiirc/plugin-fileuploader/shardedfilestore"
	"github.com/rs/zerolog"
)

type Options struct {
	BatchLimit   int
	TimeBudget   time.Duration
	BacklogDelay time.Duration
}

type Expirer struct {
	ticker   *time.Ticker
	store    *shardedfilestore.ShardedFileStore
	ctx      context.Context
	cancel   context.CancelFunc
	doneChan chan struct{}
	stopOnce sync.Once
	options  Options
	log      *zerolog.Logger
}

func New(store *shardedfilestore.ShardedFileStore, interval time.Duration, log *zerolog.Logger, options ...Options) *Expirer {
	settings := Options{BatchLimit: shardedfilestore.MaintenanceLimit, TimeBudget: shardedfilestore.MaintenanceBudget, BacklogDelay: 250 * time.Millisecond}
	if len(options) > 0 {
		if options[0].BatchLimit > 0 {
			settings.BatchLimit = options[0].BatchLimit
		}
		if options[0].TimeBudget > 0 {
			settings.TimeBudget = options[0].TimeBudget
		}
		if options[0].BacklogDelay > 0 {
			settings.BacklogDelay = options[0].BacklogDelay
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	e := &Expirer{ticker: time.NewTicker(interval), store: store, ctx: ctx, cancel: cancel, doneChan: make(chan struct{}), log: log, options: settings}
	go func() {
		defer close(e.doneChan)
		wake := time.NewTimer(time.Hour)
		if !wake.Stop() {
			<-wake.C
		}
		defer wake.Stop()
		var backlog <-chan time.Time
		run := func(t time.Time) {
			e.gc(t)
			if e.ctx.Err() == nil && e.store.MoreMaintenance() {
				wake.Reset(e.options.BacklogDelay)
				backlog = wake.C
			} else {
				backlog = nil
			}
		}
		run(time.Now())
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-e.ticker.C:
				if !wake.Stop() {
					select {
					case <-wake.C:
					default:
					}
				}
				run(t)
			case t := <-backlog:
				run(t)
			}
		}
	}()
	return e
}
func (e *Expirer) Stop() {
	e.stopOnce.Do(func() { e.ticker.Stop(); e.cancel() })
	<-e.doneChan
}
func (e *Expirer) gc(t time.Time) {
	if e.ctx.Err() != nil {
		return
	}
	e.log.Debug().Str("event", "gc_tick").Msg("Filestore GC tick")
	// Independent budgets: a recovery backlog must not starve expiration.
	for _, run := range []func(context.Context) error{
		func(ctx context.Context) error {
			return e.store.ExpireBatch(ctx, time.Now(), e.options.BatchLimit)
		},
		func(ctx context.Context) error {
			return e.store.RecoverPendingContext(ctx, e.options.BatchLimit)
		},
	} {
		ctx, cancel := context.WithTimeout(e.ctx, e.options.TimeBudget)
		err := run(ctx)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			e.log.Warn().Dur("budget", e.options.TimeBudget).Msg("Maintenance budget exhausted; pending work retained for retry")
		}
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			e.log.Error().Err(err).Msg("Maintenance batch failed")
		}
		if e.ctx.Err() != nil {
			return
		}
	}
}
