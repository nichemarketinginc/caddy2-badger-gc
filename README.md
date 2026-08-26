# caddy2-badger-gc

A standalone Caddy 2 app module that periodically runs **Badger value-log
garbage collection** on the *same* in-process `*badger.DB` handle that
[Souin](https://github.com/darkweak/souin)'s Badger storage already holds open.

## Why

Souin (our Caddy HTTP cache) keeps its cache in a Badger value-log DB. Badger
never reclaims dead value-log space on its own — `RunValueLogGC` has to be called
explicitly — so a long-lived cache steadily accumulates garbage vlog files (tens
of gigabytes, in our case) that never shrink. This module reclaims that space
with **zero downtime and no fork of Souin**.

It works because `darkweak/storages` exports everything we need:

- `github.com/darkweak/storages/core` → `core.GetRegisteredStorers() []Storer`
- `github.com/darkweak/storages/badger` → `*badger.Badger`, whose embedded
  `*badger.DB` is the very handle Souin opened.

Compiled into the same Caddy binary as Souin, the type assertion resolves to the
identical type, so we reuse Souin's own handle: **no second `Open()`, no lock
fight.** `RunValueLogGC` is designed to run alongside live traffic (Badger
serializes it internally) and runs on the handle that already holds the lock — so
zero contention, zero downtime, and the dead vlog reclaims itself over the first
few cycles and then stays flat.

The storer set is re-resolved from the registry on **every tick**, so a Souin
config reload (which calls `ResetRegisteredStorages` and reopens the DB) can't
leave us pinned to a closed handle — a stale handle just yields an error (or, at
worst, a recovered panic) and we move on to the next cycle.

### Why this instead of patching Souin

- **No fork to maintain.** Uses only stable exported APIs — no `replace`
  directive, no rebasing a third-party patch on every Souin bump.
- **Fits the existing pattern.** One more in-house Caddy module alongside
  `caddy-dns-mywahosting`, `caddy2-wordpress-version`, and
  `caddy2-validate-vhost-dir` — same `xcaddy --with`, same golden-host build,
  same `deploy-caddy.sh`.
- **Elegant.** A pure in-process goroutine: no restarts, no external tooling, no
  volume growth.

## Build

Compile it into your Caddy binary with [`xcaddy`](https://github.com/caddyserver/xcaddy),
in the *same* build as Souin:

```sh
xcaddy build \
    --with github.com/darkweak/souin/plugins/caddy \
    --with github.com/nichemarketinginc/caddy2-badger-gc
```

> **Version pinning matters.** The plugin's `go.mod` pins the same versions
> already in the binary — `storages/core` + `storages/badger` `v0.0.11`,
> `dgraph-io/badger/v3` `v3.2103.5` — so the type assertion is against the
> *identical* type. If you bump Souin (and with it these storage deps), bump them
> here to match.

## Configure

`badger_gc` is a Caddy **app** (not an HTTP handler), so it is enabled in the
global-options block at the top of the Caddyfile. Provisioning it is what starts
the background goroutine:

```caddyfile
{
    badger_gc {
        interval       10m    # how often to sweep (default: 10m)
        discard_ratio  0.5     # rewrite a vlog file only if it can discard
                               # at least this fraction (0.0–1.0, default: 0.5)
    }

    # ... your Souin / cache global config ...
}
```

Both knobs are optional; the block can be empty (`badger_gc`) to accept the
defaults.

### Config reference

| Option          | JSON key        | Type       | Default | Meaning                                                              |
|-----------------|-----------------|------------|---------|---------------------------------------------------------------------|
| `interval`      | `interval`      | duration   | `10m`   | Time between GC sweeps.                                              |
| `discard_ratio` | `discard_ratio` | float      | `0.5`   | Passed to `RunValueLogGC`; a vlog file is rewritten only if it can discard at least this fraction of its space. Must be in `(0.0, 1.0)`. |

## What it does at runtime

On each tick the module walks `core.GetRegisteredStorers()`, and for every storer
that type-asserts to `*badger.Badger` it calls `RunValueLogGC(discard_ratio)` in a
loop until Badger returns `ErrNoRewrite` (one file is rewritten per successful
call). A sweep that reclaimed anything is logged:

```
badger_gc reclaimed value-log files    {"rewrites": 3}
```

## License

GPL-3.0 (see [LICENSE](LICENSE)).
