import type { LifecycleHealth, LoopHealth } from '@dormice/shared';
import type { Archiver } from './archive/archiver';
import type { Db } from './db/db';
import type { WatcherTable } from './e2b/watcher-table';
import type { Executor } from './executor/executor';
import type { KeyedQueue } from './keyed-queue';
import { reconcile } from './reconciler';
import { scanOnce } from './scanner';
import { Watchdog } from './watchdog';

/**
 * The daemon's lifecycle work once it serves, as two loops that never wait
 * on each other: idle cooling (scanner.ts — freeze, stop, archive, the E2B
 * deadlines) and reconciliation (reconciler.ts — the ledger squared against
 * Docker's full container list).
 *
 * Why two (2026-10-09, Beijing production): they used to be one tick,
 * reconcile then scan, in one try. Reconciliation opens with the container
 * list, the one Docker read whose cost grows with the fleet; at the evening
 * peak 2,600 containers took 88s to list against a 30s deadline, every tick
 * failed at its first step, and the scan behind it never ran. Freezing is
 * what makes running sandboxes fewer, so none happened: active sandboxes
 * went from 280 to 714 in six hours, 646 of them past their freeze time,
 * swap from 25 to 86 GB — and dockerd only got slower, a loop that locks
 * itself once load crosses a line. Cooling needs no list: it reads the
 * ledger, and each move inspects the one container it moves. So it runs on
 * its own, and a reconciliation that fails or takes minutes costs late
 * repairs, never cooling.
 *
 * Each loop is chained — its next pass is scheduled when this one is done,
 * so a long pass (a backlog of freezes at up to 45s of memory.reclaim each,
 * a list of thousands) never has passes pile up on it — and the two share
 * the per-sandbox slots (KeyedQueue), so no sandbox is moved by both at
 * once. Failures are logged, never fatal: the ledger is only written after
 * reality moved, so the next pass retries whatever failed.
 *
 * Each pass runs under its own dead-man switch (watchdog.ts): a pass that
 * makes no progress for the stall limit — an await that never settles —
 * exits the daemon crash-only. One switch per loop, because one loop's
 * progress must never vouch for the other's: under a shared switch,
 * reconciliation's beats would hide a cooling pass stuck for good — the
 * 2026-08-13 incident, eleven hours without a freeze. A switch watches a
 * pass, not the wait between passes.
 *
 * And each loop keeps its health (shared LoopHealth), answered by /healthz
 * and carried in every check-in, so failing passes are counted where an
 * operator or a monitor can read them — the node's open /healthz, and
 * each node's reading in the gateway's listNodes — not only in the journal,
 * where on 2026-10-09 they went unread for six hours.
 */

export type LoopName = 'cooling' | 'reconcile';

export interface HeartbeatLog {
  info(obj: object, msg: string): void;
  warn(obj: object, msg: string): void;
  error(obj: object, msg: string): void;
}

export interface HeartbeatOptions {
  db: Db;
  executor: Executor;
  locks: KeyedQueue;
  archiver?: Archiver;
  watchers?: WatcherTable;
  /** DORMICE_SCAN_INTERVAL_SECONDS: the pause between one loop's passes. */
  intervalSeconds: number;
  /** How long a pass may go without progress before onStall. */
  stallAfterMs: number;
  /** How often the switches look at the clock; the suites shorten it. */
  checkEveryMs?: number;
  /** The bite: called once a pass stalls (main.ts exits, crash-only). */
  onStall: (loop: LoopName, stalledForMs: number) => void;
  log: HeartbeatLog;
  /** Injected clock: the suites travel in time instead of sleeping. */
  now?: () => Date;
}

/** One chained loop: its passes, its dead-man switch, its health. */
class Loop {
  private timer: NodeJS.Timeout | undefined;
  private closing = false;
  private readonly state: LoopHealth = {
    lastSuccessAt: null,
    runningSince: null,
    failures: 0,
    failingSince: null,
    lastError: null,
    sandboxFailures: 0,
  };

  constructor(
    private readonly name: LoopName,
    private readonly intervalMs: number,
    private readonly watchdog: Watchdog,
    private readonly now: () => Date,
    private readonly log: HeartbeatLog,
    /** One pass; answers how many sandboxes failed in it, throws when the pass itself failed. */
    private readonly work: (beat: () => void) => Promise<number>,
  ) {}

  health(): LoopHealth {
    return { ...this.state };
  }

  start(): void {
    this.schedule();
  }

  stop(): void {
    this.closing = true;
    clearTimeout(this.timer);
    this.watchdog.stop();
  }

  /** One pass, now. Never throws: a failure is recorded, logged, and retried by the next pass. */
  async runOnce(): Promise<void> {
    const { state } = this;
    state.runningSince = this.now().toISOString();
    this.watchdog.start();
    try {
      const sandboxFailures = await this.work(() => this.watchdog.beat());
      if (state.failures > 0) {
        this.log.info(
          { failedPasses: state.failures, failingSince: state.failingSince },
          `${this.name} pass completed again`,
        );
      }
      state.lastSuccessAt = this.now().toISOString();
      state.failures = 0;
      state.failingSince = null;
      state.lastError = null;
      state.sandboxFailures = sandboxFailures;
    } catch (error) {
      state.failures += 1;
      state.failingSince ??= this.now().toISOString();
      state.lastError = error instanceof Error ? error.message : String(error);
      this.log.error(
        {
          err: error,
          failedPasses: state.failures,
          failingSince: state.failingSince,
        },
        `${this.name} pass failed`,
      );
    } finally {
      state.runningSince = null;
      this.watchdog.stop();
    }
  }

  private schedule(): void {
    if (this.closing) return;
    this.timer = setTimeout(async () => {
      await this.runOnce();
      this.schedule();
    }, this.intervalMs);
  }
}

export class Heartbeat {
  private readonly cooling: Loop;
  private readonly reconciling: Loop;
  /** Reconciliation's first strikes, carried from one pass to the next (reconciler.ts). */
  private suspects: ReadonlySet<string> = new Set();

  constructor(private readonly opts: HeartbeatOptions) {
    const now = opts.now ?? (() => new Date());
    const loop = (
      name: LoopName,
      work: (beat: () => void) => Promise<number>,
    ) =>
      new Loop(
        name,
        opts.intervalSeconds * 1000,
        new Watchdog({
          stallAfterMs: opts.stallAfterMs,
          checkEveryMs: opts.checkEveryMs ?? 60_000,
          onStall: (stalledForMs) => opts.onStall(name, stalledForMs),
        }),
        now,
        opts.log,
        work,
      );
    this.cooling = loop('cooling', (beat) => this.cool(now(), beat));
    this.reconciling = loop('reconcile', (beat) => this.reconcile(beat));
  }

  /** Both loops, each first passing one interval from now (the startup reconcile has just run). */
  start(): void {
    this.cooling.start();
    this.reconciling.start();
  }

  stop(): void {
    this.cooling.stop();
    this.reconciling.stop();
  }

  /** One pass of one loop, now and outside its schedule — the suites' handle. */
  pass(loop: LoopName): Promise<void> {
    return (loop === 'cooling' ? this.cooling : this.reconciling).runOnce();
  }

  health(): LifecycleHealth {
    return {
      cooling: this.cooling.health(),
      reconcile: this.reconciling.health(),
    };
  }

  private async cool(now: Date, beat: () => void): Promise<number> {
    const { db, executor, locks, archiver, watchers, log } = this.opts;
    const scan = await scanOnce(
      db,
      executor,
      locks,
      now,
      archiver,
      watchers,
      beat,
    );
    for (const failure of scan.failures) {
      log.error(failure, 'idle scan: sandbox transition failed');
    }
    return scan.failures.length;
  }

  private async reconcile(beat: () => void): Promise<number> {
    const { db, executor, locks, archiver, watchers, log } = this.opts;
    const drift = await reconcile(db, executor, locks, {
      priorSuspects: this.suspects,
      archiver,
      watchers,
      onProgress: beat,
    });
    this.suspects = new Set(drift.suspects);
    const repairs = {
      repairedStates: drift.repairedStates,
      deletedRows: drift.deletedRows,
      destroyedOrphans: drift.destroyedOrphans,
      removedDisks: drift.removedDisks,
      archivedSwept: drift.archivedSwept,
    };
    if (Object.values(repairs).some((count) => count > 0)) {
      log.warn(
        { ...repairs, removedShells: drift.removedShells },
        'runtime reconcile repaired drift',
      );
    } else if (drift.removedShells > 0) {
      // Routine, not drift: the shell a recorded death left behind.
      log.info(
        { removedShells: drift.removedShells },
        'runtime reconcile removed exited shells, their disks kept',
      );
    }
    for (const failure of drift.failures) {
      log.error(failure, 'reconcile: sandbox repair failed');
    }
    return drift.failures.length;
  }
}
