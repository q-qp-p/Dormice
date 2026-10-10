import type { SandboxState } from '@dormice/shared';
import type { Db } from './db/db';
import {
  deleteSandbox,
  findById,
  listSandboxes,
  overwriteState,
} from './db/ledger';
import type { SandboxRow } from './db/schema';
import type { WatcherTable } from './e2b/watcher-table';
import type { ContainerState, Executor } from './executor/executor';
import type { KeyedQueue } from './keyed-queue';
import { recordShellDeath } from './lifecycle';

export interface ReconcileResult {
  /** Rows whose state was corrected to what reality actually shows. */
  repairedStates: number;
  /** Rows deleted because both the container and the disk are gone. */
  deletedRows: number;
  /** Containers destroyed because no row points at them. */
  destroyedOrphans: number;
  /** Disks removed because neither a row nor a container owns them. */
  removedDisks: number;
  /** Stale local remains of archived rows swept away (interrupted cleanups). */
  archivedSwept: number;
  /** Exited shells under stopped rows removed, their disks kept. */
  removedShells: number;
  /**
   * Containers and disks with no row, seen for the first time — not
   * destroyed yet. The caller passes them back as `priorSuspects` on the
   * next run.
   */
  suspects: string[];
  /** Repairs that failed this pass, by sandbox; the caller logs them, the next pass retries. */
  failures: Array<{ sandboxId: string; message: string }>;
}

export interface ReconcileOptions {
  /**
   * The first strikes of the previous pass. Omitted at startup, when
   * nothing can be in flight: orphans are then destroyed at once, and
   * exited shells are left to the runtime passes.
   */
  priorSuspects?: ReadonlySet<string>;
  archiver?: { hasLiveRestore(sandboxId: string): boolean };
  watchers?: WatcherTable;
  /**
   * Called once per row/orphan/disk considered — the watchdog's evidence
   * (the boot's at startup, the reconcile loop's afterwards) that a long
   * pass is progressing rather than stuck on one await.
   */
  onProgress?: () => void;
  /**
   * Reality as the caller has just read it, instead of reading it here:
   * startup's, shared with the startup guard so a boot pays for one
   * container list, not two. Startup only — before listen nothing can
   * insert a row, while at runtime the ledger must be read before reality
   * (reconcile has why).
   */
  reality?: {
    containers: ReadonlyMap<string, ContainerState>;
    disks: readonly string[];
  };
}

/** What a container's observed state means in ledger terms. */
const LEDGER_STATE: Record<ContainerState, SandboxState> = {
  running: 'active',
  paused: 'frozen',
  stopped: 'stopped',
};

/**
 * Reads the ledger, then all of reality, and repairs every disagreement.
 * Runs once at startup, before the daemon serves traffic, and then as its
 * own loop for as long as the daemon runs (heartbeat.ts): reality moves
 * while the daemon runs (a gVisor box exits whole on OOM), and drift only
 * repaired at boot would leave acquire returning corpses.
 *
 * The ledger snapshot is deliberately taken before the reality snapshots.
 * acquire brings reality up first and inserts the row second, so any row in
 * this snapshot had its container (and disk, made even earlier) up before
 * the listings below were taken — a row without a container here really
 * lost it. The other order judged rows inserted mid-pass against container
 * listings from before their containers existed, and deleted live sandboxes.
 *
 * Reality wins every disagreement, but reality is container AND disk, and
 * the disk is the sandbox's durable data:
 * - state differs             -> the row records what the container is
 * - container gone, disk kept -> the sandbox is stopped, not dead: the row
 *                                stays and the next acquire builds a shell
 *                                around the disk. This is also what a
 *                                stopped sandbox normally is — stop()
 *                                removes the shell.
 * - container and disk gone   -> the sandbox truly is gone (an interrupted
 *                                destroy, a wiped data dir): the row is
 *                                deleted and the name is free again
 * - exited shell, row stopped -> removed, the disk kept: the death it
 *                                records is in the ledger already, and every
 *                                shell kept costs every list dockerd answers
 * - container without a row   -> destroyed; without a row it has no user
 *                                key, so nothing could ever reach it
 * - disk owned by nothing     -> removed; leaked disks silently eat the host
 *
 * The list nominates; the container as it is now decides. The list is the
 * one read whose cost grows with the fleet — 2,600 containers took 88s on
 * Beijing production on 2026-10-09 (LIST_DEADLINE_SECONDS) — so by the
 * time a nominated row's slot is free the observation can be minutes old.
 * Every rowed repair happens inside the sandbox's per-key queue slot, only
 * if the row still shows the state the snapshot read, and on one fresh
 * inspect of its container (executor.containerState): rows the list shows
 * healthy cost nothing, a repair costs one inspect. A busy key is skipped
 * outright — whatever holds it has fresher knowledge — and the next pass
 * sees the settled picture. An orphan is re-read in the ledger just before
 * it goes, for the same reason: a row that landed while the list ran makes
 * it no orphan.
 *
 * Orphan destruction takes two strikes at runtime: a container or disk
 * whose acquire is still in flight has no row yet, and killing it would
 * sabotage the very request creating it. Whatever is still unowned on two
 * consecutive passes (a pass interval apart) is genuinely unreachable.
 * `priorSuspects` carries the first strikes between runs; omitting it (at
 * startup, when nothing can be in flight) destroys orphans immediately.
 * Startup also leaves exited shells alone: a backlog of them is minutes of
 * dockerd work (1,900 on Beijing the day stop() began removing its own),
 * which the first runtime pass does while the daemon serves.
 *
 * Archived rows claim S3, not local reality — anything of theirs still
 * here is an interrupted cleanup (the transition to archived IS the upload
 * confirmation), swept by resuming it: a leftover container is destroyed,
 * a leftover disk removed. Restoring rows with a live restore task are the
 * task's own business and skipped; without one they are crash zombies (the
 * tracker is daemon memory) repaired reality-first — a container means the
 * restore physically finished before the crash, so the ledger records it;
 * no container means a half-restore, whose disk is removed and whose row
 * reverts to archived (the S3 object is intact — the next acquire retries
 * cleanly). The startup run happens before listen, so no request ever
 * observes an unrepaired zombie.
 *
 * One sandbox's failure must not cost the rest their repairs (the
 * scanner's rule): a repair that throws is recorded in `failures` and the
 * pass goes on. Only the reads of the whole picture — the ledger, the
 * container list, the disks — fail the pass itself.
 */
export async function reconcile(
  db: Db,
  executor: Executor,
  locks: KeyedQueue,
  opts: ReconcileOptions = {},
): Promise<ReconcileResult> {
  const { priorSuspects, archiver, watchers, onProgress, reality } = opts;
  const startup = priorSuspects === undefined;
  const rows = listSandboxes(db);
  // A copy either way: entries are deleted as rows claim them, and the
  // caller's observation is not this pass's to consume.
  const containers = new Map(
    reality?.containers ?? (await executor.listContainers()),
  );
  const disks = new Set(reality?.disks ?? (await executor.listDisks()));
  const result: ReconcileResult = {
    repairedStates: 0,
    deletedRows: 0,
    destroyedOrphans: 0,
    removedDisks: 0,
    archivedSwept: 0,
    removedShells: 0,
    suspects: [],
    failures: [],
  };
  // Everything a disk may legitimately belong to: rows (a row deleted
  // below had no disk), and containers that still exist afterwards
  // (destroy tears its own disk, so a container's disk is never removed
  // out from under it).
  const owners = new Set<string>();

  const attempt = async (sandboxId: string, work: () => Promise<unknown>) => {
    try {
      await work();
    } catch (error) {
      result.failures.push({
        sandboxId,
        message: error instanceof Error ? error.message : String(error),
      });
    }
  };

  // Re-reads the row inside its queue slot and applies the repair only if
  // the state this pass read still holds, handing it the container as it
  // is now. The slot is the guard: apply may await executor work, and
  // nothing else touches this sandbox while the slot is held.
  const repairUnderLock = (
    row: SandboxRow,
    apply: (current: ContainerState | null) => void | Promise<void>,
  ) =>
    attempt(row.id, () =>
      locks.tryRun(row.name, async () => {
        const fresh = findById(db, row.id);
        if (fresh && fresh.state === row.state) {
          await apply(await executor.containerState(row.id));
        }
      }),
    );

  for (const row of rows) {
    onProgress?.();
    owners.add(row.id);
    const observed = containers.get(row.id);
    containers.delete(row.id);
    if (row.state === 'archived') {
      if (observed === undefined && !disks.has(row.id)) continue;
      await repairUnderLock(row, async (current) => {
        if (current !== null) {
          await executor.destroy(row.id);
        } else if (disks.has(row.id)) {
          await executor.removeDisk(row.id);
        } else {
          return; // The leftover the list saw is gone already.
        }
        result.archivedSwept += 1;
      });
      continue;
    }
    if (row.state === 'restoring') {
      if (archiver?.hasLiveRestore(row.id)) {
        continue; // The task owns this row; what we observe is its mid-work.
      }
      await repairUnderLock(row, async (current) => {
        if (current !== null) {
          overwriteState(db, row.id, LEDGER_STATE[current]);
        } else {
          await executor.removeDisk(row.id);
          overwriteState(db, row.id, 'archived');
        }
        result.repairedStates += 1;
      });
      continue;
    }
    const healthy =
      observed === undefined
        ? row.state === 'stopped' && disks.has(row.id)
        : LEDGER_STATE[observed] === row.state &&
          (observed !== 'stopped' || startup);
    if (healthy) continue;
    await repairUnderLock(row, async (current) => {
      if (current === null) {
        if (!disks.has(row.id)) {
          watchers?.disposeSandbox(row.id);
          deleteSandbox(db, row.id);
          result.deletedRows += 1;
        } else if (row.state !== 'stopped') {
          watchers?.disposeSandbox(row.id);
          overwriteState(db, row.id, 'stopped');
          result.repairedStates += 1;
        }
        return;
      }
      if (current === 'stopped' && row.state === 'stopped') {
        if (!startup) {
          await executor.removeContainer(row.id);
          result.removedShells += 1;
        }
        return;
      }
      if (current === 'stopped') {
        // A stopped container under a row that never ordered a stop is a
        // death the daemon did not cause (the daemon's own stop leaves no
        // shell). Read how it died before the ledger moves on — inside the
        // slot, so a wake that just revived the shell (the other reader of
        // deaths) is seen as the live container it left. recordShellDeath
        // writes what the wake would have: state and lastExit, one story.
        const exit = await executor.exitOf(row.id);
        // Revived under us (or gone — the next pass sees that shape).
        if (exit === null) return;
        recordShellDeath(db, row, exit, watchers);
        result.repairedStates += 1;
        return;
      }
      if (LEDGER_STATE[current] !== row.state) {
        overwriteState(db, row.id, LEDGER_STATE[current]);
        result.repairedStates += 1;
      }
    });
  }

  // Everything still in the map has no ledger row. Either way the disk is
  // accounted for — destroy tears its own, a suspect keeps its — so the
  // disk pass below must not touch these ids.
  for (const sandboxId of containers.keys()) {
    onProgress?.();
    owners.add(sandboxId);
    if (priorSuspects === undefined || priorSuspects.has(sandboxId)) {
      await attempt(sandboxId, async () => {
        if (findById(db, sandboxId) !== undefined) return;
        await executor.destroy(sandboxId);
        result.destroyedOrphans += 1;
      });
    } else {
      result.suspects.push(sandboxId);
    }
  }

  for (const sandboxId of disks) {
    onProgress?.();
    if (owners.has(sandboxId)) continue;
    if (priorSuspects === undefined || priorSuspects.has(sandboxId)) {
      await attempt(sandboxId, async () => {
        if (findById(db, sandboxId) !== undefined) return;
        await executor.removeDisk(sandboxId);
        result.removedDisks += 1;
      });
    } else {
      result.suspects.push(sandboxId);
    }
  }

  return result;
}
