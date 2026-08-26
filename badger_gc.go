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

	// MaxRewritesPerSweep caps how many value-log files a single sweep will
	// rewrite (across all Badger storers), spreading a large backlog -- e.g. the
	// tens of gigabytes reclaimed on the first sweep after deploy -- across
	// several ticks instead of one long, I/O-heavy sweep. 0 (default) means no
	// cap: reclaim everything reclaimable each sweep.
	MaxRewritesPerSweep int `json:"max_rewrites_per_sweep,omitempty"`

	logger  *zap.Logger
	done    chan struct{} // closed by Stop() to signal the sweep goroutine
	stopped chan struct{} // closed by run() when it has fully exited
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
	if a.MaxRewritesPerSweep < 0 {
		a.MaxRewritesPerSweep = 0 // treat a negative as "unlimited"
	}
	return nil
}

// attachTimeout bounds how long the sweep goroutine waits for Souin's badger
// storer to appear in the registry before deciding this module is a no-op.
const attachTimeout = 30 * time.Second

// Start launches the background GC goroutine. The attach confirmation and the
// first reclaim happen inside the goroutine (see run), so Start never blocks
// Caddy's startup.
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

// badgerStorers reports how many registered Souin storers are Badger-backed.
func (a *App) badgerStorers() int {
	n := 0
	for _, s := range core.GetRegisteredStorers() {
		if b, ok := s.(*badger.Badger); ok && b.DB != nil {
			n++
		}
	}
	return n
}

// awaitStorers polls the registry until a Badger-backed storer appears, then
// logs the attach confirmation -- or warns (once) that the module is a no-op if
// none shows up within attachTimeout. Souin registers its storer while the HTTP
// app provisions its cache handler; whether that finishes before or after this
// app's Start is not guaranteed (it depends on provision ordering and how long
// opening the Badger store takes), so we must NOT probe just once at Start. A
// no-op almost always means this module was built against storages/badger
// versions that differ from the running binary (so the *badger.Badger assertion
// never matches) or Souin isn't using the badger backend. Returns true if
// attached. Interruptible via done.
func (a *App) awaitStorers() bool {
	deadline := time.Now().Add(attachTimeout)
	for {
		if n := a.badgerStorers(); n > 0 {
			a.logger.Info("badger_gc: attached to Souin badger storer(s)", zap.Int("count", n))
			return true
		}
		if time.Now().After(deadline) {
			a.logger.Warn("badger_gc: no Badger-backed Souin storer found within " + attachTimeout.String() +
				" -- this module is a NO-OP. Verify it was compiled with the SAME storages/badger + " +
				"dgraph-io/badger versions as Souin, and that Souin uses the badger storage backend.")
			return false
		}
		select {
		case <-a.done:
			return false
		case <-time.After(time.Second):
		}
	}
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
	// Wait for Souin's badger storer to register (logs attach/no-op), then
	// reclaim once immediately so a large backlog starts shrinking at startup
	// rather than one interval later.
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

// collect runs one GC sweep across all registered Badger-backed storers,
// re-resolving the registry every sweep so a Souin reload can never leave us on a
// stale handle. A per-sweep rewrite budget (MaxRewritesPerSweep; 0 = unlimited)
// is shared across all storers so a large backlog is spread over several ticks
// rather than done in one long sweep.
func (a *App) collect() {
	// budget < 0 means "unlimited"; a non-negative budget is decremented as files
	// are rewritten and stops the sweep once it hits zero.
	budget := -1
	if a.MaxRewritesPerSweep > 0 {
		budget = a.MaxRewritesPerSweep
	}
	total := 0
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
		total += a.collectOne(b, &budget)
		if budget == 0 {
			break
		}
	}
	if total > 0 {
		a.logger.Info("badger_gc reclaimed value-log files", zap.Int("rewrites", total))
	}
}

// collectOne reclaims value-log space on a single handle until Badger reports
// ErrNoRewrite (or any other error), Stop() is signalled, or the shared per-sweep
// budget (*budget, when >= 0) is exhausted. Returns the number of files rewritten.
// It is wrapped in a recover so that racing a Souin reload that closed this handle
// mid-sweep degrades to a logged warning rather than taking Caddy down.
func (a *App) collectOne(b *badger.Badger, budget *int) (rewrites int) {
	defer func() {
		if r := recover(); r != nil {
			a.logger.Warn("badger_gc recovered from panic during sweep",
				zap.Any("panic", r))
		}
	}()

	// RunValueLogGC rewrites at most one file per call, returning nil on a
	// successful rewrite and a non-nil error (ErrNoRewrite) once there is nothing
	// left worth reclaiming. Check `done` each iteration so a reload interrupts a
	// long first-run sweep (tens of files) promptly.
	for *budget != 0 {
		select {
		case <-a.done:
			return
		default:
		}
		if err := b.DB.RunValueLogGC(a.DiscardRatio); err != nil {
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
