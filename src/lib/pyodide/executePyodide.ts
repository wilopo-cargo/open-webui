import type { GeneratedFile } from './generatedFiles';
import type { PyodideOutput } from './uploadGeneratedFiles';

export type PyodideExecutionRequest = {
	id: string;
	code: string;
	packages?: unknown[];
	files?: GeneratedFile[];
};

export type PyodideExecutionOutput = PyodideOutput & { workerTerminated?: boolean };

const pendingExecutions = new WeakMap<Worker, Set<(reason: string) => void>>();
const terminatedWorkers = new WeakSet<Worker>();

export function terminatePyodideWorker(worker: Worker, reason = 'Python runtime was stopped before this execution completed') {
	if (terminatedWorkers.has(worker)) return;
	terminatedWorkers.add(worker);
	worker.terminate();
	// Worker.terminate() emits no error event. Resolve every queued/active caller explicitly.
	for (const cancel of pendingExecutions.get(worker) ?? []) cancel(reason);
	pendingExecutions.delete(worker);
}

export function executePyodide(worker: Worker, request: PyodideExecutionRequest): Promise<PyodideExecutionOutput> {
	// A caller can finish fetching its input files after another execution stopped this worker.
	if (terminatedWorkers.has(worker)) {
		return Promise.resolve({ stderr: 'Python runtime has been stopped; run the code again', workerTerminated: true });
	}
	return new Promise((resolve) => {
		let finished = false;
		let timeoutId: number | undefined;
		const pending = pendingExecutions.get(worker) ?? new Set<(reason: string) => void>();
		pendingExecutions.set(worker, pending);
		const finish = (output: PyodideExecutionOutput) => {
			if (finished) return;
			finished = true;
			window.clearTimeout(timeoutId);
			worker.removeEventListener('message', onMessage);
			worker.removeEventListener('error', onError);
			pending.delete(cancel);
			if (pending.size === 0) pendingExecutions.delete(worker);
			resolve(output);
		};
		const cancel = (reason: string) => finish({ stderr: reason, workerTerminated: true });
		const onMessage = (event: MessageEvent<PyodideOutput & { id?: string; type?: string }>) => {
			const data = event.data;
			if (!data || data.id !== request.id) return;
			if (data.type === 'execution:start') {
				// Queue wait is not execution time. The runtime acknowledges only the dequeued request.
				if (timeoutId === undefined) {
					timeoutId = window.setTimeout(() => {
						finish({ stderr: 'Execution Time Limit Exceeded', workerTerminated: true });
						terminatePyodideWorker(worker, 'Python runtime was stopped because another execution exceeded its time limit');
					}, 60000);
				}
				return;
			}
			if (!data.type) finish(data);
		};
		const onError = (event: Event & { message?: string }) => {
			terminatePyodideWorker(worker, event.message || 'Python worker failed');
		};
		pending.add(cancel);
		worker.addEventListener('message', onMessage);
		worker.addEventListener('error', onError);
		try {
			worker.postMessage({ type: 'execute', ...request });
		} catch (error) {
			finish({ stderr: error instanceof Error ? error.message : String(error) });
		}
	});
}
