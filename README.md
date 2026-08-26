# caddy2-badger-gc

A standalone Caddy 2 **app module** that periodically runs **BadgerDB value-log
garbage collection** on the *same* in-process `*badger.DB` handle that
[Souin](https://github.com/darkweak/souin)'s Badger storage already holds open —
reclaiming dead cache space with **zero downtime and no fork of Souin**.

## Purpose

Souin (the Caddy HTTP cache) stores cached responses in a BadgerDB value-log
database (`/var/cache/badgerdb` on our caddy2 frontends). Badger is an LSM store:
deleting/expiring a key (Souin does this via TTL) shrinks the key index (the
`.sst` files) but leaves the response bodies behind in the value log (the `.vlog`
files). **Badger never reclaims that dead value-log space on its own** — the
application must call `db.RunValueLogGC()` explicitly — and Souin's badger
provider never calls it. So the store grows without bound: on our fleet a
frontend reached **34 GB of `.vlog` with only 13 MB of live `.sst`** (≈99.9%
reclaimable garbage), filling the cache volume until it alarmed.

This module is the missing garbage collector. Because it is compiled into the
same binary as Souin, it reuses Souin's *own* open handle and calls the GC that
Souin omits — no second `Open()` (which would fail Badger's exclusive lock), no
separate process, no patched/forked Souin.

## Goal

- Keep the BadgerDB value log **bounded** so the cache volume stops filling with
  dead data — no volume resizing, no scheduled cache-clears, no restarts.
- Do it **online, with zero downtime and zero cache loss**: GC runs alongside
  live traffic on the handle that already holds the lock, so serving is never
  interrupted and warm cache entries are preserved.
- Be an **independent, drop-in module** — nothing in Souin or the Caddyfile's
  cache config has to change; add this module, enable one global-options block.

## How it works

`darkweak/storages` exposes everything needed, all as stable **exported** API:

- `github.com/darkweak/storages/core` → `core.GetRegisteredStorers() []Storer`
  (every storage provider registers itself here; it's how Souin itself finds them).
- `github.com/darkweak/storages/badger` → the `*badger.Badger` storer, whose
  **embedded exported `*badger.DB`** is the very handle Souin opened.

On each tick the module walks `core.GetRegisteredStorers()`, type-asserts each to
`*badger.Badger`, and calls `RunValueLogGC(discard_ratio)` in a loop until Badger
returns `ErrNoRewrite` (one value-log file is rewritten per successful call).
`RunValueLogGC` is designed to run concurrently with live traffic (Badger
serializes it internally). The registry is re-resolved **every tick**, so a Souin
config reload (which calls `ResetRegisteredStorages` and reopens the DB) can never
leave the sweeper pinned to a closed handle.

## Requirements

These are hard requirements. If any is unmet the module **silently does nothing**
(the value log just keeps growing), so it logs a one-time warning at startup when
it finds no Badger storer — watch for it (see *Verifying*).

1. **Same binary as Souin's badger backend.** This module must be compiled into
   the *same* Caddy binary as `github.com/darkweak/souin/plugins/caddy` **and**
   the badger storage module — it shares Souin's in-process handle; it does not
   open the DB itself. On a binary without Souin/badger it is a no-op.
2. **Version-identical storage deps.** The `go.mod` here pins the **same**
   versions the target binary uses — currently `storages/core` +
   `storages/badger` `v0.0.11` and `dgraph-io/badger/v3` `v3.2103.5`. The
   `*badger.Badger` type assertion only matches when the types are byte-for-byte
   the same package version; a mismatch compiles fine but never matches at
   runtime → silent no-op. **If you bump Souin/storages, bump them here to match.**
   (Caddy itself is unified across the xcaddy build; this module tracks the
   fleet's `caddy/v2` — currently `v2.8.4`.)
3. **Souin must use the `badger` storage backend.** If Souin is configured with a
   different backend (`otter`, `nuts`, `redis`, …) there is no `*badger.Badger`
   to GC and this module no-ops. (For those backends, use their own eviction.)
4. **Enabled in the Caddyfile.** As a Caddy *app*, it runs only when present in
   the config — add the `badger_gc` global-options block (see *Configure*).
5. **Runs where Souin/badger runs.** In our topology that's the caddy2 *frontends*
   (one GC goroutine per Caddy process). It is harmless where badger is absent.

## Build

Compile it into your Caddy binary with [`xcaddy`](https://github.com/caddyserver/xcaddy),
in the **same** build as Souin and its badger backend:

```sh
xcaddy build \
    --with github.com/darkweak/souin/plugins/caddy \
    --with github.com/darkweak/storages/badger/caddy \
    --with github.com/nichemarketinginc/caddy2-badger-gc
```

## Configure

`badger_gc` is a Caddy **app** (not an HTTP handler), enabled in the
global-options block at the top of the Caddyfile:

```caddyfile
{
    badger_gc {
        interval                10m   # how often to sweep (default: 10m)
        discard_ratio           0.5   # rewrite a vlog file only if it can discard
                                      # at least this fraction (0.0–1.0, default: 0.5)
        max_rewrites_per_sweep  0     # cap files rewritten per sweep (0 = unlimited)
    }

    # ... your Souin / cache global config ...
}
```

All knobs are optional; the block can be empty (`badger_gc`) to accept defaults.

| Option                   | Type     | Default | Meaning                                                                                                                                   |
|--------------------------|----------|---------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `interval`               | duration | `10m`   | Time between GC sweeps.                                                                                                                    |
| `discard_ratio`          | float    | `0.5`   | Passed to `RunValueLogGC`: a vlog file is rewritten only if ≥ this fraction is reclaimable. `(0,1)`.                                       |
| `max_rewrites_per_sweep` | int      | `0`     | Cap on value-log files rewritten per sweep (shared across storers). `0` = unlimited. Use it to spread a large first-run backlog over ticks so a single sweep doesn't do minutes of disk I/O at once. |

## Verifying it works

At **startup** the module waits (up to 30s, polling) for Souin's badger storer to
register, then logs whether it found one and runs a first reclaim immediately —
this is the check that all the *Requirements* above are met, surfaced at deploy
time (not one interval later):

```
badger_gc started                          {"interval":"10m0s","discard_ratio":0.5,"max_rewrites_per_sweep":0}
badger_gc: attached to Souin badger storer(s)  {"count":1}      # ✅ requirements met
badger_gc reclaimed value-log files        {"rewrites":7}       # first sweep, at startup
```

(The wait is deliberate: Souin registers its storer while the HTTP app provisions
its cache handler, which may finish *after* this app starts — so a single probe at
start would race it. The GC sweep also re-resolves the registry every tick, so a
reload can never pin it to a closed handle.) If instead you see this, the module
is a **no-op** — fix the build/version/backend (see *Requirements*):

```
badger_gc: no Badger-backed Souin storer found within 30s -- this module is a NO-OP. ...
```

Each sweep that reclaims space logs its file count, and you can watch the value
log shrink on disk:

```
badger_gc reclaimed value-log files        {"rewrites":7}

# on the host:
ls /var/cache/badgerdb/*.vlog | wc -l      # file count should fall then hold flat
du -sh /var/cache/badgerdb                 # total should drop toward the live set
```

## Limitations / non-goals

- **Not a size cap.** It reclaims *dead* space; it does not bound the *live*
  working set. If genuinely-live cache exceeds the volume, that's a TTL/volume
  sizing question, not a GC one.
- **Badger backend only** (requirement #3).
- Reclamation is asynchronous: the first sweep after deploy rewrites many files
  and the value log shrinks over the following cycles rather than instantly.

## License

GPL-3.0 (see [LICENSE](LICENSE)).
