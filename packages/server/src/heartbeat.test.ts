import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { DEFAULT_LIFECYCLE_POLICY } from '@dormice/shared';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { type Db, migrateDb, openDb } from './db/db';
import { createSandbox, findById } from './db/ledger';
import type { SandboxRow } from './db/schema';
import { FakeExecutor } from './executor/fake';
import { Heartbeat, type HeartbeatOptions } from './heartbeat';
import { KeyedQueue } from './keyed-queue';

/**
 * The two lifecycle loops, staged on the 2026-10-09 Beijing incident: the
 * full container list stops answering in time, and idle cooling must go on
 * regardless — it was one tick with reconciliation then, and every overdue
 * sandbox stayed active for six hours.
 */

const MIGRATIONS = fileURLToPath(new URL('../drizzle', import.meta.url));

function setup() {
  const db = openDb(':memory:');
  migrateDb(db, MIGRATIONS);
  return { db, executor: new FakeExecutor(), locks: new KeyedQueue() };
}

async function seed(
  db: Db,
  executor: FakeExecutor,
  name: string,
): Promise<SandboxRow> {
  const id = randomUUID();
  await executor.create(id);
  return createSandbox(db, {
    id,
    name,
    nodeId: 'node-test',
    policy: DEFAULT_LIFECYCLE_POLICY,
  });
}

/** A clock past every seeded sandbox's freeze time: time travel, no sleeping. */
const pastFreezeTime = () =>
  new Date(
    Date.now() + (DEFAULT_LIFECYCLE_POLICY.freezeAfterSeconds + 1) * 1000,
  );

function logSpy() {
  return { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
}

function heartbeatOf(
  base: ReturnType<typeof setup>,
  over: Partial<HeartbeatOptions> = {},
) {
  return new Heartbeat({
    ...base,
    intervalSeconds: 60,
    stallAfterMs: 30 * 60_000,
    onStall: vi.fn(),
    log: logSpy(),
    now: pastFreezeTime,
    ...over,
  });
}

/** A list that never answers — dockerd under the evening peak, or a lost response. */
const neverAnswers = () => new Promise<never>(() => {});

afterEach(() => {
  vi.useRealTimers();
});

describe('the heartbeat: cooling and reconciliation never wait on each other', () => {
  it('freezes an overdue sandbox while the full container list never answers', async () => {
    const base = setup();
    const row = await seed(base.db, base.executor, 'alice');
    vi.spyOn(base.executor, 'listContainers').mockImplementation(neverAnswers);
    const heartbeat = heartbeatOf(base);

    // A reconciliation pass in flight for good...
    void heartbeat.pass('reconcile');
    // ...and the cooling pass goes about its business all the same.
    await heartbeat.pass('cooling');

    expect(base.executor.stateOf(row.id)).toBe('paused');
    expect(findById(base.db, row.id)?.state).toBe('frozen');
    const health = heartbeat.health();
    expect(health.reconcile.runningSince).not.toBeNull();
    expect(health.reconcile.lastSuccessAt).toBeNull();
    expect(health.cooling).toMatchObject({
      lastSuccessAt: expect.any(String),
      runningSince: null,
      failures: 0,
      sandboxFailures: 0,
    });
    heartbeat.stop();
  });

  it('a failing list fails reconciliation alone — counted and logged until a pass completes again — and cooling goes on', async () => {
    const base = setup();
    const row = await seed(base.db, base.executor, 'alice');
    const list = vi
      .spyOn(base.executor, 'listContainers')
      .mockRejectedValue(
        new Error('listContainers got no answer from dockerd within 600s'),
      );
    const log = logSpy();
    const heartbeat = heartbeatOf(base, { log });

    await heartbeat.pass('reconcile');
    await heartbeat.pass('reconcile');
    expect(heartbeat.health().reconcile).toMatchObject({
      lastSuccessAt: null,
      runningSince: null,
      failures: 2,
      failingSince: expect.any(String),
      lastError: 'listContainers got no answer from dockerd within 600s',
    });
    expect(log.error).toHaveBeenCalledWith(
      expect.objectContaining({ failedPasses: 2 }),
      'reconcile pass failed',
    );

    await heartbeat.pass('cooling');
    expect(findById(base.db, row.id)?.state).toBe('frozen');
    expect(heartbeat.health().cooling.failures).toBe(0);

    // dockerd answers again: the run of failures ends, once and audibly.
    list.mockRestore();
    await heartbeat.pass('reconcile');
    expect(heartbeat.health().reconcile).toMatchObject({
      lastSuccessAt: expect.any(String),
      failures: 0,
      failingSince: null,
      lastError: null,
    });
    expect(log.info).toHaveBeenCalledWith(
      expect.objectContaining({ failedPasses: 2 }),
      'reconcile pass completed again',
    );
  });

  it('on their own schedules: cooling keeps its interval while a reconcile pass is stuck, and only the stuck pass bites', async () => {
    vi.useFakeTimers();
    const base = setup();
    const alice = await seed(base.db, base.executor, 'alice');
    vi.spyOn(base.executor, 'listContainers').mockImplementation(neverAnswers);
    const onStall = vi.fn();
    const heartbeat = heartbeatOf(base, {
      stallAfterMs: 10 * 60_000,
      checkEveryMs: 60_000,
      onStall,
    });
    heartbeat.start();

    await vi.advanceTimersByTimeAsync(60_000);
    expect(base.executor.stateOf(alice.id)).toBe('paused');

    // Due the moment it lands: the next cooling pass, one interval on,
    // freezes it — reconciliation has not finished a single pass.
    const bob = await seed(base.db, base.executor, 'bob');
    await vi.advanceTimersByTimeAsync(60_000);
    expect(base.executor.stateOf(bob.id)).toBe('paused');
    expect(heartbeat.health().reconcile.lastSuccessAt).toBeNull();

    // The stuck pass makes no progress for the stall limit: its own switch
    // bites. Cooling completed a pass every interval meanwhile, and each
    // of those vouched for nothing but itself.
    await vi.advanceTimersByTimeAsync(10 * 60_000);
    expect(onStall).toHaveBeenCalledTimes(1);
    expect(onStall.mock.calls[0]?.[0]).toBe('reconcile');
    expect(onStall.mock.calls[0]?.[1]).toBeGreaterThanOrEqual(10 * 60_000);
    heartbeat.stop();
  });

  it('a cooling pass that completes with sandboxes failing their move says how many', async () => {
    const base = setup();
    await seed(base.db, base.executor, 'alice');
    vi.spyOn(base.executor, 'freeze').mockRejectedValue(
      new Error('pause of alice got no answer from dockerd within 120s'),
    );
    const log = logSpy();
    const heartbeat = heartbeatOf(base, { log });

    await heartbeat.pass('cooling');
    expect(heartbeat.health().cooling).toMatchObject({
      lastSuccessAt: expect.any(String),
      failures: 0,
      sandboxFailures: 1,
    });
    expect(log.error).toHaveBeenCalledWith(
      expect.objectContaining({
        message: 'pause of alice got no answer from dockerd within 120s',
      }),
      'idle scan: sandbox transition failed',
    );
  });

  it('carries reconciliation’s first strikes from one pass to the next', async () => {
    const base = setup();
    // A container no row points at: suspected on one pass, put down on the
    // next — the strikes live in the heartbeat between passes.
    await base.executor.create('orphan');
    const heartbeat = heartbeatOf(base);

    await heartbeat.pass('reconcile');
    expect(base.executor.stateOf('orphan')).toBe('running');
    await heartbeat.pass('reconcile');
    expect(base.executor.stateOf('orphan')).toBeUndefined();
  });
});
