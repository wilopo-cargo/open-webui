import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { executePyodide, terminatePyodideWorker } from './executePyodide';

class ControlledWorker extends EventTarget {
	requests: unknown[] = [];
	stopped = false;

	postMessage(message: unknown) {
		if (this.stopped) throw new Error('Stopped worker received another execution');
		this.requests.push(message);
	}

	terminate() {
		this.stopped = true;
	}

	reply(data: object) {
		this.dispatchEvent(new MessageEvent('message', { data }));
	}
}

beforeEach(() => {
	vi.useFakeTimers();
	vi.stubGlobal('window', { setTimeout: globalThis.setTimeout, clearTimeout: globalThis.clearTimeout });
});

afterEach(() => {
	vi.clearAllTimers();
	vi.useRealTimers();
	vi.unstubAllGlobals();
});

describe('Pyodide execution lifecycle', () => {
	it('gives a queued 30-second execution its own budget after a 45-second execution', async () => {
		const controlled = new ControlledWorker();
		// The EventTarget double implements the Worker messaging/termination boundary used here.
		const worker = controlled as unknown as Worker;
		const first = executePyodide(worker, { id: 'first', code: 'first request' });
		const second = executePyodide(worker, { id: 'second', code: 'second request' });
		let secondFinished = false;
		void second.then(() => { secondFinished = true; });
		controlled.reply({ id: 'first', type: 'execution:start' });
		await vi.advanceTimersByTimeAsync(45000);
		controlled.reply({ id: 'first', stdout: 'first output', files: [] });
		controlled.reply({ id: 'second', type: 'execution:start' });
		await vi.advanceTimersByTimeAsync(15001);
		expect(controlled.stopped).toBe(false);
		expect(secondFinished).toBe(false); // Neither the acknowledgement nor 60 seconds of wall time is completion.
		await vi.advanceTimersByTimeAsync(14999);
		controlled.reply({ id: 'second', stdout: 'second output', files: [] });
		expect((await Promise.all([first, second])).map((output) => output.stdout)).toEqual(['first output', 'second output']);
		expect(controlled.stopped).toBe(false);
		expect(vi.getTimerCount()).toBe(0);
	});

	it('stops an over-budget active execution and explicitly cancels every queued caller', async () => {
		const controlled = new ControlledWorker();
		const worker = controlled as unknown as Worker;
		const first = executePyodide(worker, { id: 'first', code: 'long-running request' });
		const second = executePyodide(worker, { id: 'second', code: 'queued request' });
		controlled.reply({ id: 'first', type: 'execution:start' });
		await vi.advanceTimersByTimeAsync(60000);
		expect(await first).toEqual({ stderr: 'Execution Time Limit Exceeded', workerTerminated: true });
		expect(await second).toEqual({ stderr: 'Python runtime was stopped because another execution exceeded its time limit', workerTerminated: true });
		expect(controlled.stopped).toBe(true);
		expect(vi.getTimerCount()).toBe(0);
		controlled.reply({ id: 'second', type: 'execution:start' });
		controlled.reply({ id: 'second', stdout: 'late output must not replace cancellation' });
		const late = await executePyodide(worker, { id: 'late', code: 'input preparation finished after timeout' });
		expect(late.workerTerminated).toBe(true);
		expect(late.stderr).toContain('has been stopped');
		expect(controlled.requests).toHaveLength(2);
	});

	it('resolves callers even when a runtime is stopped before any execution-start acknowledgement', async () => {
		const controlled = new ControlledWorker();
		const worker = controlled as unknown as Worker;
		const first = executePyodide(worker, { id: 'first', code: 'waiting for runtime' });
		const second = executePyodide(worker, { id: 'second', code: 'also waiting' });
		terminatePyodideWorker(worker, 'Code block closed');
		expect(await Promise.all([first, second])).toEqual([
			{ stderr: 'Code block closed', workerTerminated: true },
			{ stderr: 'Code block closed', workerTerminated: true }
		]);
		expect(controlled.stopped).toBe(true);
		expect(vi.getTimerCount()).toBe(0);
	});
});
