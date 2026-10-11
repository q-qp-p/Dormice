import type Docker from 'dockerode';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { LIST_DEADLINE_SECONDS, VERB_DEADLINE_SECONDS } from './deadline';
import { DockerExecutor, SANDBOX_LABEL } from './docker';

/**
 * The two Docker calls the 2026-10-09 Beijing incident turned on, against a
 * stub client: the contract exam runs them on a real host, where a list of
 * thousands of containers and a dockerd that refuses a removal cannot be
 * staged. The container list has a deadline of its own and is aborted when
 * given up on, so lists never pile up inside dockerd; stop() takes the
 * shell with its processes, and a removal that fails costs a shell the
 * reconciler removes later, never the stop.
 */

function executor(docker: Docker, log?: (msg: string) => void) {
  return new DockerExecutor(
    {
      baseImage: () => 'unused',
      registry: { address: () => null, username: 'dormice', password: 'x' },
      dataDir: '/nonexistent',
      resources: () => ({ diskSizeGb: 1, cpus: 1, memoryGb: 1 }),
      pidsLimit: () => 4096,
      reclaimTimeoutSeconds: 1,
      log,
    },
    docker,
  );
}

interface ListCall {
  all?: boolean;
  filters?: unknown;
  abortSignal?: AbortSignal;
}

/** A Docker client whose list answers with `answer`, recording each call. */
function listingDocker(answer: (call: ListCall) => Promise<unknown[]>) {
  const calls: ListCall[] = [];
  const docker = {
    listContainers(call: ListCall) {
      calls.push(call);
      return answer(call);
    },
  };
  return { docker: docker as unknown as Docker, calls };
}

/** A list still running when it is given up on: what docker-modem does on abort — the request is destroyed, the call fails. */
function failsOnAbort(call: ListCall): Promise<never> {
  return new Promise((_, reject) => {
    call.abortSignal?.addEventListener('abort', () => {
      reject(
        Object.assign(new Error('The operation was aborted'), {
          name: 'AbortError',
        }),
      );
    });
  });
}

/**
 * A Docker client holding one shell in `status`, recording every call in
 * order; its removal answers with `remove`.
 */
function shellDocker(status: string, remove: () => Promise<unknown>) {
  const calls: string[] = [];
  const docker = {
    getContainer(ref: string) {
      return {
        async inspect() {
          calls.push(`inspect ${ref}`);
          return {
            Id: 'c0ffee',
            State: {
              Status: status,
              OOMKilled: false,
              ExitCode: 0,
              Pid: 4242,
              FinishedAt: '0001-01-01T00:00:00Z',
            },
            HostConfig: { NanoCpus: 1e9, PidsLimit: 4096 },
          };
        },
        async unpause() {
          calls.push(`unpause ${ref}`);
        },
        async kill() {
          calls.push(`kill ${ref}`);
        },
        async wait(opts: { condition?: string }) {
          calls.push(`wait(${opts.condition}) ${ref}`);
          return { StatusCode: 137 };
        },
        remove(opts: { force?: boolean }) {
          calls.push(`remove(force=${opts.force}) ${ref}`);
          return remove();
        },
      };
    },
  };
  return { docker: docker as unknown as Docker, calls };
}

afterEach(() => {
  vi.useRealTimers();
});

describe('the container list', () => {
  it('reads every sandbox shell by its label, as running, paused or stopped', async () => {
    const { docker, calls } = listingDocker(async () => [
      { Labels: { [SANDBOX_LABEL]: 'alice' }, State: 'running' },
      { Labels: { [SANDBOX_LABEL]: 'bob' }, State: 'paused' },
      { Labels: { [SANDBOX_LABEL]: 'carol' }, State: 'exited' },
    ]);

    const observed = await executor(docker).listContainers();
    expect([...observed]).toEqual([
      ['alice', 'running'],
      ['bob', 'paused'],
      ['carol', 'stopped'],
    ]);
    expect(calls).toHaveLength(1);
    expect(calls[0]).toMatchObject({
      all: true,
      filters: { label: [SANDBOX_LABEL] },
    });
    expect(calls[0]?.abortSignal?.aborted).toBe(false);
  });

  it('is waited for far longer than a single read, and the list given up on is aborted', async () => {
    vi.useFakeTimers();
    const { docker, calls } = listingDocker(failsOnAbort);
    const outcome = executor(docker)
      .listContainers()
      .then(
        () => 'answered',
        (err: Error) => err.message,
      );
    let settled = false;
    void outcome.then(() => {
      settled = true;
    });

    // Short of its deadline the list is waited for: Beijing's took 88s on
    // 2026-10-09, where every single-object read is allowed 30s.
    await vi.advanceTimersByTimeAsync(LIST_DEADLINE_SECONDS * 1000 - 1);
    expect(settled).toBe(false);
    expect(calls[0]?.abortSignal?.aborted).toBe(false);

    // Given up on, it is aborted: its socket closes and dockerd cancels the
    // request, instead of the next pass's list running on top of it.
    await vi.advanceTimersByTimeAsync(1);
    expect(await outcome).toBe(
      `listContainers got no answer from dockerd within ${LIST_DEADLINE_SECONDS}s`,
    );
    expect(calls[0]?.abortSignal?.aborted).toBe(true);
    expect(calls).toHaveLength(1);
  });
});

describe('stop', () => {
  it('kills the shell, waits for its exit, then removes it', async () => {
    const { docker, calls } = shellDocker('paused', async () => undefined);

    await executor(docker).stop('alice');
    expect(calls).toEqual([
      'inspect sbx-alice',
      // A signal cannot land in a paused gVisor sandbox: unpause first.
      'unpause c0ffee',
      'kill c0ffee',
      'wait(not-running) c0ffee',
      'remove(force=true) c0ffee',
    ]);
  });

  it('a removal dockerd refuses or never answers fails nothing: the processes are dead, and the log leaves the shell to the reconciler', async () => {
    const log = vi.fn();
    const refused = shellDocker('paused', () =>
      Promise.reject(
        Object.assign(
          new Error('removal of container c0ffee is already in progress'),
          { statusCode: 409 },
        ),
      ),
    );
    await executor(refused.docker, log).stop('alice');
    expect(log).toHaveBeenCalledWith(
      'stopped alice; its exited shell stays until the reconciler removes it — removal of container c0ffee is already in progress',
    );

    vi.useFakeTimers();
    const silent = shellDocker('paused', () => new Promise<never>(() => {}));
    const stopped = executor(silent.docker, log).stop('bob');
    await vi.advanceTimersByTimeAsync(VERB_DEADLINE_SECONDS * 1000);
    await stopped;
    expect(log).toHaveBeenLastCalledWith(
      `stopped bob; its exited shell stays until the reconciler removes it — remove of bob got no answer from dockerd within ${VERB_DEADLINE_SECONDS}s`,
    );
  });

  it('refuses a shell that is not paused, and removes nothing', async () => {
    const { docker, calls } = shellDocker('running', async () => undefined);

    await expect(executor(docker).stop('alice')).rejects.toThrow(
      'container alice is running, expected paused',
    );
    expect(calls).toEqual(['inspect sbx-alice']);
  });
});

describe('create', () => {
  it('a birth whose image cannot be had leaves no disk: the image is settled before the disk exists', async () => {
    const calls: string[] = [];
    const docker = {
      getContainer(ref: string) {
        return {
          async inspect() {
            calls.push(`inspect ${ref}`);
            throw Object.assign(new Error('no such container'), {
              statusCode: 404,
            });
          },
        };
      },
      getImage(ref: string) {
        return {
          async inspect() {
            calls.push(`inspect image ${ref}`);
            throw Object.assign(new Error('no such image'), {
              statusCode: 404,
            });
          },
        };
      },
    } as unknown as Docker;

    // No registry: the absent image is a refusal, and it comes before
    // provisioning would touch dataDir (here /nonexistent — reaching it
    // would fail with a different message).
    await expect(executor(docker).create('alice')).rejects.toThrow(
      'image unused is not on this host and the fleet has no registry',
    );
    expect(calls).toEqual(['inspect sbx-alice', 'inspect image unused']);
  });
});
