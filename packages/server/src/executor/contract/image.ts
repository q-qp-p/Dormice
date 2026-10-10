import { describe, expect, it } from 'vitest';
import type { ContractContext } from './index';

/**
 * Which image a shell boots from — the physical half of templates. The rule
 * under exam: an image is a property of the shell, fixed at the shell's
 * birth (create, or start's rebuild-from-surviving-disk), and never changed
 * by merely starting an existing container.
 */
export function imageTests(ctx: ContractContext) {
  const { timeoutMs } = ctx;

  describe('image', () => {
    it(
      'create boots the requested image',
      async () => {
        const id = await ctx.fresh({ image: ctx.subject.altImage });
        expect(await ctx.subject.imageOf(id)).toBe(ctx.subject.altImage);
      },
      timeoutMs,
    );

    it(
      'create without an image boots the base image',
      async () => {
        const id = await ctx.fresh();
        expect(await ctx.subject.imageOf(id)).toBe(ctx.subject.baseImage);
      },
      timeoutMs,
    );

    it(
      'a rebuilt shell boots the requested image over the same disk',
      async () => {
        // Born on the base image, with data on the disk.
        const id = await ctx.fresh();
        await ctx.executor.writeFiles(id, [
          { path: 'kept.txt', content: Buffer.from('survives the swap') },
        ]);
        await ctx.executor.freeze(id);
        // stop() removes the shell; the start after it builds a new one.
        await ctx.executor.stop(id);
        // The rebuild is where a new image takes effect: new shell, old disk.
        await ctx.executor.start(id, { image: ctx.subject.altImage });
        expect(await ctx.subject.imageOf(id)).toBe(ctx.subject.altImage);
        const kept = await ctx.executor.readFile(id, 'kept.txt');
        expect(kept.toString()).toBe('survives the swap');
      },
      timeoutMs,
    );

    it(
      'starting an existing container never changes its image',
      async () => {
        // A shell that died on its own: its exited container still exists
        // — start only starts it; the image asked for here must be
        // ignored, not applied.
        const id = await ctx.freshExited();
        await ctx.executor.start(id, { image: ctx.subject.altImage });
        expect(await ctx.subject.imageOf(id)).toBe(ctx.subject.baseImage);
      },
      timeoutMs,
    );

    it(
      'imageOf reads the born image in any state, exited included',
      async () => {
        const id = await ctx.fresh({ image: ctx.subject.altImage });
        expect(await ctx.executor.imageOf(id)).toBe(ctx.subject.altImage);
        await ctx.executor.freeze(id);
        expect(await ctx.executor.imageOf(id)).toBe(ctx.subject.altImage);
        // A shell that died on its own is still the shell, and start would
        // boot it as-is — so it honestly reports the old image.
        await ctx.executor.unfreeze(id);
        await ctx.subject.crashContainer(id);
        expect(await ctx.executor.imageOf(id)).toBe(ctx.subject.altImage);
      },
      timeoutMs,
    );

    it(
      'imageOf answers null when no shell exists, never a guess',
      async () => {
        const id = await ctx.fresh();
        await ctx.executor.freeze(id);
        // stop() takes the shell; a death's exited shell can be pruned
        // behind the daemon's back. Either way the disk survives, but an
        // image is a property of the shell — with no shell there is nothing
        // to report; the next start decides.
        await ctx.executor.stop(id);
        expect(await ctx.executor.imageOf(id)).toBe(null);
        const pruned = await ctx.freshExited();
        await ctx.subject.vanishContainer(pruned);
        expect(await ctx.executor.imageOf(pruned)).toBe(null);
        // An observation verb: an unknown sandbox is null too, not a throw.
        expect(await ctx.executor.imageOf('never-created')).toBe(null);
      },
      timeoutMs,
    );
  });
}
