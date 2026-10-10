import { z } from 'zod';

/**
 * How one of the daemon's two lifecycle loops is doing (server/heartbeat.ts):
 * idle cooling (freeze, stop, archive, the E2B deadlines) and reconciliation
 * (the ledger squared against Docker's full container list). A pass either
 * completes or fails as a whole; within a completed pass, single sandboxes
 * may still fail their move or repair. Both are news an operator must be
 * able to read without the daemon's log — on 2026-10-09 Beijing's
 * reconciliation failed every pass for six hours, logged and unread, and
 * cooling (then tied to it) stopped with it.
 */
export const loopHealthSchema = z.object({
  /** When a pass last completed; null until one has. */
  lastSuccessAt: z.iso.datetime().nullable(),
  /**
   * When the pass now in flight began; null between passes. A list of
   * thousands of containers can take minutes, and this is how a slow pass
   * shows before it fails.
   */
  runningSince: z.iso.datetime().nullable(),
  /** Passes failed in a row; 0 once a pass completes. */
  failures: z.number().int().nonnegative(),
  /** When the current run of failed passes began; null while passes complete. */
  failingSince: z.iso.datetime().nullable(),
  /** The latest failed pass's error; null while passes complete. */
  lastError: z.string().nullable(),
  /**
   * Sandboxes whose move (cooling) or repair (reconciliation) failed in
   * the last completed pass — each retried by the next. A pass that
   * completes while every freeze in it fails is not a healthy one.
   */
  sandboxFailures: z.number().int().nonnegative(),
});

export type LoopHealth = z.infer<typeof loopHealthSchema>;

export const lifecycleHealthSchema = z.object({
  cooling: loopHealthSchema,
  reconcile: loopHealthSchema,
});

export type LifecycleHealth = z.infer<typeof lifecycleHealthSchema>;
