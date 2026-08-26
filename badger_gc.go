// Package badgergc provides a small standalone Caddy app module that
// periodically runs Badger's value-log garbage collection on the *same*
// in-process *badger.DB handle that Souin's badger storage already holds open.
//
// Souin (the Caddy HTTP cache) stores its cache in a Badger value-log DB. Badger
// never reclaims dead value-log space on its own — RunValueLogGC has to be called
// explicitly — so a long-lived cache steadily accumulates garbage vlog files
// (tens of gigabytes in our case) that never shrink. This module reclaims that
// space with zero downtime and no fork of Souin.
//
// It works because darkweak/storages exports everything we need:
//
//   - github.com/darkweak/storages/core   → core.GetRegisteredStorers() []Storer
//   - github.com/darkweak/storages/badger → *badger.Badger, whose embedded
//     *badger.DB is the very handle Souin opened.
//
// Compiled into the same Caddy binary as Souin, the type assertion resolves to
// the identical type, so we reuse Souin's own handle: no second Open(), no lock
// fight, and RunValueLogGC (which Badger serializes internally and is designed to
// run alongside live traffic) does its work with zero contention.
//
// The storer set is re-resolved from the registry on every tick, so a Souin
// config reload (which calls ResetRegisteredStorages and reopens the DB) can't
// leave us pinned to a closed handle — a stale handle just yields an error and we
// move on to the next cycle.
package badgergc

import (
	"strconv"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"go.uber.org/zap"

	"github.com/darkweak/storages/badger"
	"github.com/darkweak/storages/core"
)

const (
	defaultInterval     = 10 * time.Minute
	defaultDiscardRatio = 0.5
)

func init() {
	caddy.RegisterModule(App{})
	httpcaddyfile.RegisterGlobalOption("badger_gc", parseGlobalOption)
}

// App is the badger_gc Caddy app. It runs a single background goroutine that,
// on a fixed interval, walks every registered Souin storer and — for each one
// backed by Badger — runs value-log GC until Badger reports nothing left to
// reclaim.
type App struct {
	// Interval is how often to run a GC sweep. Default: 10m.
	Interval caddy.Duration `json:"interval,omitempty"`

	// DiscardRatio is passed to Badger's RunValueLogGC: a value-log file is
	// rewritten only if it can discard at least this fraction of its space.
	// Must be in (0.0, 1.0). Default: 0.5.
	DiscardRatio float64 `json:"discard_ratio,omitempty"`

	logger     *zap.Logger
	done       chan struct{} // closed by Stop() to signal the sweep goroutine
	stopped    chan struct{} // closed by run() when it has fully exited
	firstSweep bool          // startup confirmation is logged once (run() goroutine only)
}

// CaddyModule returns the Caddy module information. badger_gc is a top-level app
// module (not an HTTP handler), so its ID has no dotted namespace prefix.
func (App) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "badger_gc",
		New: func() caddy.Module { return new(App) },
	}
}

// Provision sets defaults and grabs a logger.
func (a *App) Provision(ctx caddy.Context) error {
	a.logger = ctx.Logger()
	if a.Interval <= 0 {
		a.Interval = caddy.Duration(defaultInterval)
	}
	if a.DiscardRatio <= 0 || a.DiscardRatio >= 1 {
		a.DiscardRatio = defaultDiscardRatio
	}
	return nil
}

// Start launches the background GC goroutine.
func (a *App) Start() error {
	a.done = make(chan struct{})
	a.stopped = make(chan struct{})
	a.firstSweep = true
	go a.run()
	a.logger.Info("badger_gc started",
		zap.Duration("interval", time.Duration(a.Interval)),
		zap.Float64("discard_ratio", a.DiscardRatio),
	)
	return nil
}

// Stop signals the background goroutine to exit AND waits (bounded) for it to
// actually return. Blocking here is deliberate: on a config reload Caddy stops
// the old app before the new Souin reopens the DB, so if we returned while a
// sweep were still running we could leave the OLD goroutine calling
// RunValueLogGC on a handle the new Souin is closing/reopening -- the exact race
// we otherwise only paper over with recover(). The sweep loop checks `done`
// between value-log rewrites, so this wait is normally near-instant; the timeout
// only guards a single in-flight file rewrite that cannot be interrupted.
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

// collect runs one GC sweep across all registered Badger-backed storers. It
// re-resolves the registry every sweep so a Souin reload can never leave us on a
// stale handle. It also surfaces the SILENT-NO-OP case: if no Badger-backed
// storer is found, this module is doing nothing -- almost always because it was
// built against storages/badger versions that differ from the running binary (so
// the type assertion never matches) or because Souin isn't using the badger
// backend. That failure is otherwise invisible (garbage just keeps growing), so
// we log it explicitly on the first sweep.
func (a *App) collect() {
	found := 0
	for _, s := range core.GetRegisteredStorers() {
		select { // bail promptly if Stop() was called mid-sweep
		case <-a.done:
			return
		default:
		}
		b, ok := s.(*badger.Badger)
		if !ok || b.DB == nil {
			continue
		}
		found++
		a.collectOne(b)
	}

	if a.firstSweep {
		a.firstSweep = false
		if found == 0 {
			a.logger.Warn("badger_gc: no Badger-backed Souin storer found -- this module is a NO-OP. " +
				"Verify it was compiled with the SAME storages/badger + dgraph-io/badger versions as Souin, " +
				"and that Souin uses the badger storage backend.")
		} else {
			a.logger.Info("badger_gc: attached to Souin badger storer(s)", zap.Int("count", found))
		}
	}
}

// collectOne reclaims value-log space on a single handle until Badger reports
// ErrNoRewrite (or any other error), or until Stop() is signalled. It is wrapped
// in a recover so that racing a Souin reload that closed this handle mid-sweep
// degrades to a logged warning rather than taking Caddy down.
func (a *App) collectOne(b *badger.Badger) {
	defer func() {
		if r := recover(); r != nil {
			a.logger.Warn("badger_gc recovered from panic during sweep",
				zap.Any("panic", r))
		}
	}()

	rewrites := 0
	// RunValueLogGC rewrites at most one file per call, returning nil on a
	// successful rewrite and a non-nil error (ErrNoRewrite) once there is
	// nothing left worth reclaiming. Check `done` each iteration so a reload
	// interrupts a long first-run sweep (tens of files) promptly.
	for {
		select {
		case <-a.done:
			return
		default:
		}
		if err := b.DB.RunValueLogGC(a.DiscardRatio); err != nil {
			break
		}
		rewrites++
	}
	if rewrites > 0 {
		a.logger.Info("badger_gc reclaimed value-log files",
			zap.Int("rewrites", rewrites))
	}
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
//	    interval       10m
//	    discard_ratio  0.5
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
