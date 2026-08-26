// Package badgergc provides a small standalone Caddy app module that
// periodically runs Badger's value-log garbage collection on the *same*
// in-process *badger.DB handle that Souin's Badger storage already holds open.
//
// Souin (the Caddy HTTP cache, via github.com/caddyserver/cache-handler) stores
// its cache in a Badger value-log DB. Badger never reclaims dead value-log space
// on its own — RunValueLogGC has to be called explicitly — so a long-lived cache
// steadily accumulates garbage vlog files (tens of gigabytes in our case) that
// never shrink. This module reclaims that space with zero downtime and no fork
// of Souin.
//
// REACHING THE LIVE HANDLE. darkweak/storages exposes a process-global registry
// (core.GetRegisteredStorers), but cache-handler DRAINS it: its cache app's
// Start() calls core.ResetRegisteredStorages() after copying the storers into
// its own exported field. So the registry is empty at runtime — the live storers
// live on the cache app itself (httpcache.SouinApp.Storers []types.Storer, whose
// concrete elements are *badger.Badger, embedding the exported *badger.DB). We
// therefore reach them through Caddy's app graph: ctx.App("cache") →
// *httpcache.SouinApp → .Storers → *badger.Badger → .DB. Compiled into the same
// binary as Souin, these types resolve to the identical types, so we reuse
// Souin's own handle: no second Open(), no lock fight, and RunValueLogGC (which
// Badger serializes internally and is designed to run alongside live traffic)
// does its work with zero contention.
package badgergc

import (
	"strconv"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"go.uber.org/zap"

	httpcache "github.com/caddyserver/cache-handler"
	sbadger "github.com/darkweak/storages/badger" // the Souin storer type (*sbadger.Badger)
	badgerv3 "github.com/dgraph-io/badger/v3"     // the embedded value-log DB (*badgerv3.DB)
)

const (
	defaultInterval     = 10 * time.Minute
	defaultDiscardRatio = 0.5

	// souinAppID is the Caddy app module ID of the cache-handler app that holds
	// the live storers (the `cache { }` global directive configures it).
	souinAppID = "cache"

	// attachTimeout bounds how long the sweep goroutine waits for the cache app's
	// storers to be populated before deciding this module is a no-op.
	attachTimeout = 30 * time.Second
)

func init() {
	caddy.RegisterModule(App{})
	httpcaddyfile.RegisterGlobalOption("badger_gc", parseGlobalOption)
}

// App is the badger_gc Caddy app. It runs a single background goroutine that,
// on a fixed interval, runs Badger value-log GC on every Badger-backed storer
// the cache app holds, until Badger reports nothing left to reclaim.
type App struct {
	// Interval is how often to run a GC sweep. Default: 10m.
	Interval caddy.Duration `json:"interval,omitempty"`

	// DiscardRatio is passed to Badger's RunValueLogGC: a value-log file is
	// rewritten only if it can discard at least this fraction of its space.
	// Must be in (0.0, 1.0). Default: 0.5.
	DiscardRatio float64 `json:"discard_ratio,omitempty"`

	// MaxRewritesPerSweep caps how many value-log files a single sweep will
	// rewrite (across all Badger storers), spreading a large backlog -- e.g. the
	// tens of gigabytes reclaimed on the first sweep after deploy -- across
	// several ticks instead of one long, I/O-heavy sweep. 0 (default) = no cap.
	MaxRewritesPerSweep int `json:"max_rewrites_per_sweep,omitempty"`

	logger   *zap.Logger
	souinApp *httpcache.SouinApp // the cache app that holds the live storers
	done     chan struct{}       // closed by Stop() to signal the sweep goroutine
	stopped  chan struct{}       // closed by run() when it has fully exited
}

// CaddyModule returns the Caddy module information. badger_gc is a top-level app
// module (not an HTTP handler), so its ID has no dotted namespace prefix.
func (App) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "badger_gc",
		New: func() caddy.Module { return new(App) },
	}
}

// Provision sets defaults, grabs a logger, and resolves the cache app whose
// Storers hold the live Badger handle(s).
func (a *App) Provision(ctx caddy.Context) error {
	a.logger = ctx.Logger()
	if a.Interval <= 0 {
		a.Interval = caddy.Duration(defaultInterval)
	}
	if a.DiscardRatio <= 0 || a.DiscardRatio >= 1 {
		a.DiscardRatio = defaultDiscardRatio
	}
	if a.MaxRewritesPerSweep < 0 {
		a.MaxRewritesPerSweep = 0 // treat a negative as "unlimited"
	}
	// Resolve the cache app. ctx.App provisions it if needed and returns the
	// singleton instance; its Storers slice is populated while the HTTP cache
	// handler provisions, which may be after this returns -- so the goroutine
	// waits for it (see awaitStorers) rather than requiring it now.
	if capp, err := ctx.App(souinAppID); err != nil {
		a.logger.Warn("badger_gc: no '"+souinAppID+"' (Souin cache) app is configured -- module is a NO-OP",
			zap.Error(err))
	} else if sa, ok := capp.(*httpcache.SouinApp); ok {
		a.souinApp = sa
	} else {
		a.logger.Warn("badger_gc: the '" + souinAppID + "' app is not a *httpcache.SouinApp -- " +
			"cache-handler version mismatch; module is a NO-OP")
	}
	return nil
}

// badgerDBs returns the live *badger.DB handles the cache app currently holds.
func (a *App) badgerDBs() []*badgerv3.DB {
	if a.souinApp == nil {
		return nil
	}
	var dbs []*badgerv3.DB
	for _, s := range a.souinApp.Storers {
		if b, ok := s.(*sbadger.Badger); ok && b.DB != nil {
			dbs = append(dbs, b.DB)
		}
	}
	return dbs
}

// Start launches the background GC goroutine. Attach confirmation and the first
// reclaim happen inside the goroutine (see run), so Start never blocks Caddy.
func (a *App) Start() error {
	a.done = make(chan struct{})
	a.stopped = make(chan struct{})
	a.logger.Info("badger_gc started",
		zap.Duration("interval", time.Duration(a.Interval)),
		zap.Float64("discard_ratio", a.DiscardRatio),
		zap.Int("max_rewrites_per_sweep", a.MaxRewritesPerSweep),
	)
	go a.run()
	return nil
}

// Stop signals the sweep goroutine to exit AND waits (bounded) for it to return,
// so a config reload never leaves the old goroutine running GC on a Badger store
// the new Souin is closing/reopening. The sweep checks done between value-log
// rewrites, so the wait is normally near-instant; the timeout only guards a
// single in-flight file rewrite that cannot be interrupted.
func (a *App) Stop() error {
	if a.done != nil {
		close(a.done)
	}
	if a.stopped != nil {
		select {
		case <-a.stopped:
		case <-time.After(30 * time.Second):
			a.logger.Warn("badger_gc did not stop within 30s; proceeding anyway")
		}
	}
	return nil
}

func (a *App) run() {
	defer close(a.stopped)
	// Wait for the cache app's Badger storer(s) to be populated (logs attach or
	// no-op), then reclaim once immediately so a large backlog starts shrinking
	// at startup rather than one interval later.
	if a.awaitStorers() {
		a.collect()
	}
	ticker := time.NewTicker(time.Duration(a.Interval))
	defer ticker.Stop()
	for {
		select {
		case <-a.done:
			return
		case <-ticker.C:
			a.collect()
		}
	}
}

// awaitStorers polls until the cache app exposes a Badger-backed storer, then
// logs the attach confirmation -- or warns (once) that the module is a no-op if
// none appears within attachTimeout. The wait is necessary because the cache
// handler populates SouinApp.Storers during its own provisioning, whose ordering
// relative to this app's Start is not guaranteed. A no-op almost always means
// this module was built against storages/badger + cache-handler versions that
// differ from the running binary, or Souin isn't using the badger backend.
// Returns true if attached; interruptible via done.
func (a *App) awaitStorers() bool {
	deadline := time.Now().Add(attachTimeout)
	for {
		if n := len(a.badgerDBs()); n > 0 {
			a.logger.Info("badger_gc: attached to Souin badger storer(s)", zap.Int("count", n))
			return true
		}
		if time.Now().After(deadline) {
			a.logger.Warn("badger_gc: no Badger-backed Souin storer found within " + attachTimeout.String() +
				" -- this module is a NO-OP. Verify it was compiled with the SAME cache-handler + " +
				"storages/badger + dgraph-io/badger versions as Souin, and that Souin uses the badger backend.")
			return false
		}
		select {
		case <-a.done:
			return false
		case <-time.After(time.Second):
		}
	}
}

// collect runs one GC sweep across every Badger handle the cache app holds. A
// per-sweep rewrite budget (MaxRewritesPerSweep; 0 = unlimited) is shared across
// handles so a large backlog is spread over several ticks rather than one sweep.
func (a *App) collect() {
	budget := -1 // <0 means unlimited
	if a.MaxRewritesPerSweep > 0 {
		budget = a.MaxRewritesPerSweep
	}
	total := 0
	for _, db := range a.badgerDBs() {
		select { // bail promptly if Stop() was called mid-sweep
		case <-a.done:
			return
		default:
		}
		total += a.collectOne(db, &budget)
		if budget == 0 {
			break
		}
	}
	if total > 0 {
		a.logger.Info("badger_gc reclaimed value-log files", zap.Int("rewrites", total))
	}
}

// collectOne reclaims value-log space on one handle until Badger reports
// ErrNoRewrite (or any error), Stop() is signalled, or the shared per-sweep
// budget (*budget, when >= 0) is exhausted. Returns the number of files
// rewritten. Wrapped in a recover so that racing a Souin reload that closed this
// handle mid-sweep degrades to a logged warning rather than taking Caddy down.
func (a *App) collectOne(db *badgerv3.DB, budget *int) (rewrites int) {
	defer func() {
		if r := recover(); r != nil {
			a.logger.Warn("badger_gc recovered from panic during sweep", zap.Any("panic", r))
		}
	}()
	// RunValueLogGC rewrites at most one file per call, returning nil on a
	// successful rewrite and a non-nil error (ErrNoRewrite) once there is nothing
	// left worth reclaiming. Check done each iteration so a reload interrupts a
	// long first-run sweep (tens of files) promptly.
	for *budget != 0 {
		select {
		case <-a.done:
			return
		default:
		}
		if err := db.RunValueLogGC(a.DiscardRatio); err != nil {
			break
		}
		rewrites++
		if *budget > 0 {
			*budget--
		}
	}
	return
}

// parseGlobalOption parses the `badger_gc { ... }` global-options stanza in the
// Caddyfile and turns it into the app's JSON config.
func parseGlobalOption(d *caddyfile.Dispenser, _ any) (any, error) {
	app := &App{}
	if err := app.UnmarshalCaddyfile(d); err != nil {
		return nil, err
	}
	return httpcaddyfile.App{
		Name:  "badger_gc",
		Value: caddyconfig.JSON(app, nil),
	}, nil
}

// UnmarshalCaddyfile implements caddyfile.Unmarshaler.
//
//	badger_gc {
//	    interval                10m
//	    discard_ratio           0.5
//	    max_rewrites_per_sweep  0    # 0 = unlimited
//	}
func (a *App) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() { // consume the "badger_gc" token
		for d.NextBlock(0) {
			switch d.Val() {
			case "interval":
				var s string
				if !d.Args(&s) {
					return d.ArgErr()
				}
				dur, err := caddy.ParseDuration(s)
				if err != nil {
					return d.Errf("invalid interval %q: %v", s, err)
				}
				a.Interval = caddy.Duration(dur)
			case "discard_ratio":
				var s string
				if !d.Args(&s) {
					return d.ArgErr()
				}
				r, err := strconv.ParseFloat(s, 64)
				if err != nil {
					return d.Errf("invalid discard_ratio %q: %v", s, err)
				}
				if r <= 0 || r >= 1 {
					return d.Errf("discard_ratio must be in (0.0, 1.0), got %v", r)
				}
				a.DiscardRatio = r
			case "max_rewrites_per_sweep":
				var s string
				if !d.Args(&s) {
					return d.ArgErr()
				}
				n, err := strconv.Atoi(s)
				if err != nil {
					return d.Errf("invalid max_rewrites_per_sweep %q: %v", s, err)
				}
				if n < 0 {
					return d.Errf("max_rewrites_per_sweep must be >= 0 (0 = unlimited), got %d", n)
				}
				a.MaxRewritesPerSweep = n
			default:
				return d.Errf("unrecognized badger_gc option %q", d.Val())
			}
		}
	}
	return nil
}

// Interface guards.
var (
	_ caddy.App             = (*App)(nil)
	_ caddy.Provisioner     = (*App)(nil)
	_ caddyfile.Unmarshaler = (*App)(nil)
)
