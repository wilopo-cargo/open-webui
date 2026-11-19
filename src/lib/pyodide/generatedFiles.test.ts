import { webcrypto } from 'node:crypto';
import { resolve } from 'node:path';
import { runInNewContext } from 'node:vm';
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { loadPyodide, type PyodideInterface } from 'pyodide';
import { uploadFile } from '$lib/apis/files';
import {
	createGeneratedFileTracker,
	GENERATED_FILE_LIMITS,
	isSafeGeneratedFileName,
	type GeneratedFile
} from './generatedFiles';
import { uploadGeneratedFiles, type PyodideOutput } from './uploadGeneratedFiles';

vi.mock('$lib/apis/files', () => ({ uploadFile: vi.fn() }));
vi.mock('$lib/constants', () => ({ WEBUI_API_BASE_URL: '/api/v1' }));

let python: PyodideInterface;
type RuntimeMessage = PyodideOutput & { id: string; type?: string; files?: GeneratedFile[] };

beforeAll(async () => {
	// Use the checked-out runtime assets, never a CDN or an installed Python environment.
	python = await loadPyodide({ indexURL: `${resolve('static/pyodide')}/`, packages: [] });
}, 60000);

beforeEach(() => {
	python.runPython("import os, shutil\nif os.path.islink('/mnt/uploads'): os.unlink('/mnt/uploads')\nelse: shutil.rmtree('/mnt/uploads', ignore_errors=True)\nos.makedirs('/mnt/uploads')");
	vi.stubGlobal('crypto', webcrypto);
	vi.stubGlobal('window', {
		location: { origin: 'https://webui.test' },
		addEventListener: vi.fn(),
		removeEventListener: vi.fn()
	});
	vi.mocked(uploadFile).mockReset();
	vi.mocked(uploadFile).mockImplementation(async (_token, file) => ({ id: `stored-${file.name}` }));
});

afterEach(() => {
	vi.restoreAllMocks();
	vi.unstubAllGlobals();
	vi.doUnmock('pyodide');
});

describe('generated file handoff', () => {
	it('publishes CSV, nested binary and empty files without republishing staged inputs or old runtime files', async () => {
		const tracker = createGeneratedFileTracker(python.FS, GENERATED_FILE_LIMITS, isSafeGeneratedFileName);
		python.FS.writeFile('/mnt/uploads/old.csv', 'private previous output');
		tracker.stage([{ name: 'input.csv', data: await new Blob(['input\n']).arrayBuffer() }]);
		const before = await tracker.snapshot();
		python.FS.mkdir('/mnt/uploads/reports');
		python.FS.writeFile('/mnt/uploads/reports/測試.csv', 'name,total\nAlice,42\n');
		python.FS.writeFile('/mnt/uploads/bytes.bin', new Uint8Array([0, 255, 128, 10]));
		python.FS.writeFile('/mnt/uploads/empty.txt', new Uint8Array());
		const exported = await tracker.collect(before);
		expect(exported.errors).toEqual([]);
		const output = await uploadGeneratedFiles({ stdout: 'done\n', result: 0, files: exported.files }, 'alice-token');
		expect(output.stdout).toBe('done\n');
		expect(output.result).toBe(0);
		expect(output.stderr).toBeNull();
		expect(output.files.map((file) => file.name)).toEqual(['bytes.bin', 'empty.txt', 'reports/測試.csv']);
		for (const [token, file, metadata, process, stream] of vi.mocked(uploadFile).mock.calls) {
			expect([token, metadata, process, stream]).toEqual(['alice-token', null, false, false]);
			const source = exported.files.find((item) => item.name.split('/').at(-1) === file.name)!;
			expect(new Uint8Array(await file.arrayBuffer())).toEqual(new Uint8Array(source.data));
		}
		for (const file of output.files) {
			expect(file.url).toBe(`https://webui.test/api/v1/files/${encodeURIComponent(file.id)}/content?attachment=true`);
			expect(Object.keys(file).sort()).toEqual(['id', 'name', 'url']);
		}
	});

	it('detects same-size modified content and never re-exports unchanged bytes', async () => {
		const tracker = createGeneratedFileTracker(python.FS, GENERATED_FILE_LIMITS, isSafeGeneratedFileName);
		python.FS.writeFile('/mnt/uploads/report.csv', 'old\n');
		const before = await tracker.snapshot();
		expect((await tracker.collect(before)).files).toEqual([]);
		const stat = python.FS.stat('/mnt/uploads/report.csv');
		python.FS.writeFile('/mnt/uploads/report.csv', 'new\n');
		python.FS.utime('/mnt/uploads/report.csv', stat.atime.getTime(), stat.mtime.getTime());
		const changed = await tracker.collect(before);
		expect(changed.files.map((file) => new TextDecoder().decode(file.data))).toEqual(['new\n']);
		expect((await tracker.collect(await tracker.snapshot())).files).toEqual([]);
	});

	it('rejects traversal, symlink files/ancestors and empty directories without reading outside uploads', async () => {
		const tracker = createGeneratedFileTracker(python.FS, GENERATED_FILE_LIMITS, isSafeGeneratedFileName);
		for (const name of ['../escape.csv', '/tmp/escape.csv', 'nested/../../escape.csv', 'bad\\name.csv', 'bad\nname.csv']) {
			expect(() => tracker.stage([{ name, data: new ArrayBuffer(0) }])).toThrow();
		}
		const before = await tracker.snapshot();
		python.FS.writeFile('/tmp/private-generated-test.txt', 'must not be exported');
		python.FS.symlink('/tmp/private-generated-test.txt', '/mnt/uploads/link.txt');
		python.FS.symlink('/tmp', '/mnt/uploads/linked-dir');
		python.FS.mkdir('/mnt/uploads/empty-directory');
		const output = await tracker.collect(before);
		expect(output.files).toEqual([]);
		expect(output.errors.join('\n')).toMatch(/no symlinks/);
		expect(output.errors.join('\n')).toMatch(/directories cannot be downloaded/);
		expect(() => tracker.stage([{ name: 'linked-dir/secret.txt', data: new ArrayBuffer(0) }])).toThrow();
		const recoveryBaseline = await tracker.snapshot();
		python.FS.unlink('/mnt/uploads/link.txt');
		python.FS.unlink('/mnt/uploads/linked-dir');
		python.FS.rmdir('/mnt/uploads/empty-directory');
		python.FS.writeFile('/mnt/uploads/recovered.csv', 'safe output');
		const recovered = await tracker.collect(recoveryBaseline);
		expect(recovered.errors).toEqual([]);
		expect(recovered.files.map((file) => file.name)).toEqual(['recovered.csv']);
		python.FS.unlink('/mnt/uploads/recovered.csv');
		python.FS.rmdir('/mnt/uploads');
		python.FS.symlink('/tmp', '/mnt/uploads');
		expect((await tracker.collect(before)).errors.join('\n')).toMatch(/real directory/);
		python.FS.unlink('/mnt/uploads');
		python.FS.mkdir('/mnt/uploads');
	});

	it('fails closed on unreadable baselines and reports export read failures', async () => {
		const tracker = createGeneratedFileTracker(python.FS, GENERATED_FILE_LIMITS, isSafeGeneratedFileName);
		const before = await tracker.snapshot();
		python.FS.writeFile('/mnt/uploads/unreadable.csv', 'private');
		const readFile = python.FS.readFile;
		vi.spyOn(python.FS, 'readFile').mockImplementation((path, options) => {
			if (path === '/mnt/uploads/unreadable.csv') throw new Error('Permission denied');
			return readFile(path, options);
		});
		await expect(tracker.snapshot()).rejects.toThrow('Permission denied');
		const output = await tracker.collect(before);
		expect(output.files).toEqual([]);
		expect(output.errors.join('\n')).toContain('Could not export unreadable.csv: Permission denied');
	});

	it('accepts the exact count/byte ceiling and visibly skips oversized or excess files', async () => {
		const tracker = createGeneratedFileTracker(python.FS, { ...GENERATED_FILE_LIMITS, maxFiles: 2, maxBytes: 4 }, isSafeGeneratedFileName);
		const before = await tracker.snapshot();
		python.FS.writeFile('/mnt/uploads/a.bin', new Uint8Array([0, 1]));
		python.FS.writeFile('/mnt/uploads/ab.bin', new Uint8Array(3));
		python.FS.writeFile('/mnt/uploads/b.bin', new Uint8Array([2, 3]));
		python.FS.writeFile('/mnt/uploads/c.bin', new Uint8Array());
		python.FS.writeFile('/mnt/uploads/d.bin', new Uint8Array(5));
		const output = await tracker.collect(before);
		expect(output.files.map((file) => file.name)).toEqual(['a.bin', 'b.bin']);
		expect(output.errors).toHaveLength(3);
		const limited = createGeneratedFileTracker(python.FS, { ...GENERATED_FILE_LIMITS, maxEntries: 1 }, isSafeGeneratedFileName);
		await expect(limited.snapshot()).rejects.toThrow(/filesystem entries/);
		python.FS.mkdirTree('/mnt/uploads/nested/deep');
		const shallow = createGeneratedFileTracker(python.FS, { ...GENERATED_FILE_LIMITS, maxDepth: 1 }, isSafeGeneratedFileName);
		await expect(shallow.snapshot()).rejects.toThrow(/directory levels/);
	});

	it('returns no invented link on upload failure, malformed bytes or configured size rejection', async () => {
		vi.mocked(uploadFile).mockRejectedValueOnce(new Error('upload unavailable')).mockResolvedValueOnce(null);
		const output = await uploadGeneratedFiles({
			stdout: 'stdout stays', stderr: 'original stderr', result: false,
			file_errors: ['symlink skipped'],
			files: [
				{ name: '../escape.csv', data: new ArrayBuffer(0) },
				{ name: 'not-bytes.csv', data: 'fake' },
				{ name: 'too-big.bin', data: new ArrayBuffer(1024 * 1024 + 1) },
				{ name: 'failed.csv', data: new ArrayBuffer(0) },
				{ name: 'missing-id.csv', data: new ArrayBuffer(0) }
			]
		}, 'alice-token', 1);
		expect(output.files).toEqual([]);
		expect(output.stdout).toBe('stdout stays');
		expect(output.result).toBe(false);
		expect(output.stderr).toContain('original stderr');
		expect(output.stderr).toContain('symlink skipped');
		expect(output.stderr).toContain('configured 1 MB upload limit');
		expect(output.stderr).toContain('upload unavailable');
		expect(output.stderr).toContain('stored file ID');
		expect(uploadFile).toHaveBeenCalledTimes(2);
	});

	it('bounds even malformed worker handoffs before uploads and still publishes eligible remaining files', async () => {
		const output = await uploadGeneratedFiles({
			files: [
				{ name: 'oversized.bin', data: new ArrayBuffer(GENERATED_FILE_LIMITS.maxBytes + 1) },
				...Array.from({ length: GENERATED_FILE_LIMITS.maxFiles }, (_, index) => ({ name: `${index}.txt`, data: new ArrayBuffer(0) }))
			]
		}, 'alice-token');
		expect(output.files).toHaveLength(GENERATED_FILE_LIMITS.maxFiles - 1);
		expect(uploadFile).toHaveBeenCalledTimes(GENERATED_FILE_LIMITS.maxFiles - 1);
		expect(output.stderr).toContain('limited to 10 files');
		expect(output.stderr).toContain('20 MiB total export limit');
	});
});

it.each(['persistent worker', 'opaque-origin sandbox'])('isolates concurrent %s outputs and recovers from a skipped symlink', async (mode) => {
	const received: RuntimeMessage[] = [];
	const events: string[] = [];
	let finish!: () => void;
	const done = new Promise<void>((resolve) => { finish = resolve; });
	const deliver = (value: RuntimeMessage, transfer: Transferable[] = []) => {
		if (value.type === 'execution:start') {
			events.push(`${value.id}:start`);
			return;
		}
		events.push(`${value.id}:result`);
		received.push(structuredClone(value, { transfer }));
		if (received.length === 2) finish();
	};
	const runtime = async (options: { stdout: (text: string) => void; stderr: (text: string) => void }) => {
		python.setStdout({ batched: options.stdout });
		python.setStderr({ batched: options.stderr });
		return {
			FS: { ...python.FS, filesystems: { IDBFS: {} }, mount: vi.fn(), syncfs: (_populate: boolean, callback: (error: null) => void) => callback(null) },
			loadedPackages: {},
			pyimport: () => ({ install: async () => {} }),
			runPythonAsync: python.runPythonAsync.bind(python)
		};
	};
	let send: (data: object) => void;
	let terminate = () => {};
	if (mode === 'persistent worker') {
		vi.doMock('pyodide', () => ({ loadPyodide: runtime }));
		const scope = {
			onmessage: null as ((event: MessageEvent) => void) | null,
			postMessage: (value: RuntimeMessage, options?: { transfer: Transferable[] }) => deliver(value, options?.transfer)
		};
		vi.stubGlobal('self', scope);
		// Load after installing the worker globals and its runtime dependency.
		await import('../workers/pyodide.worker');
		send = (data) => scope.onmessage?.({ data } as MessageEvent);
	} else {
		const iframe = { style: {}, srcdoc: '', setAttribute: vi.fn(), addEventListener: vi.fn(), removeEventListener: vi.fn(), remove: vi.fn() };
		vi.stubGlobal('document', { createElement: () => iframe, body: { appendChild: vi.fn() } });
		// Construct the real srcdoc only after installing the opaque-frame DOM boundary.
		const { PyodideSandboxHost } = await import('./pyodideSandboxHost');
		const host = new PyodideSandboxHost();
		terminate = () => host.terminate();
		let listener!: (event: { source: object; data: object }) => void;
		const parent = { postMessage: (value: RuntimeMessage, _target: string, transfer: Transferable[]) => deliver(value, transfer) };
		const sandboxWindow = { addEventListener: (_type: string, callback: typeof listener) => { listener = callback; } };
		const context = { window: sandboxWindow, self: sandboxWindow, parent, loadPyodide: runtime, crypto: webcrypto, ArrayBuffer, Uint8Array, console };
		for (const script of iframe.srcdoc.matchAll(/<script>([\s\S]*?)<\/script>/g)) runInNewContext(script[1], context);
		send = (data) => listener({ source: parent, data });
	}
	try {
		const input = await new Blob(['input']).arrayBuffer();
		for (const id of ['first', 'second']) {
			const recover = id === 'first' ? "Path('/mnt/uploads/skip-link').symlink_to('/tmp')" : "Path('/mnt/uploads/skip-link').unlink()";
			send({ type: 'execute', id, packages: [], files: [{ name: 'input.csv', data: input }], code: `import asyncio\nfrom pathlib import Path\nawait asyncio.sleep(0.01)\n${recover}\nPath('/mnt/uploads/result.csv').write_text('${id}')\nprint('${id} stdout')\n0` });
		}
		await done;
		expect(received.map((value) => value.id)).toEqual(['first', 'second']);
		expect(events).toEqual(['first:start', 'first:result', 'second:start', 'second:result']);
		for (const value of received) {
			expect(value.stderr).toBeNull();
			expect(value.stdout).toBe(`${value.id} stdout\n`);
			expect(value.result).toBe(0);
			expect(value.file_errors).toEqual(value.id === 'first' ? [expect.stringContaining('no symlinks')] : []);
			expect(value.files?.map((file) => [file.name, new TextDecoder().decode(file.data)])).toEqual([['result.csv', value.id]]);
		}
	} finally {
		terminate();
	}
}, 30000);
