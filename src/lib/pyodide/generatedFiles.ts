import type { PyodideInterface } from 'pyodide';

export const GENERATED_FILE_LIMITS = {
	maxFiles: 10,
	maxBytes: 20 * 1024 * 1024,
	maxEntries: 1000,
	maxDepth: 16
};

export type GeneratedFile = { name: string; data: ArrayBuffer };

export type GeneratedFileTracker = {
	stage(files: GeneratedFile[], dir?: string): void;
	snapshot(): Promise<Map<string, string>>;
	collect(before: Map<string, string>): Promise<{ files: GeneratedFile[]; errors: string[] }>;
};

export function isSafeGeneratedFileName(name: unknown): name is string {
	return (
		typeof name === 'string' &&
		name.length <= 1024 &&
		!/[\\\x00-\x1f\x7f]/.test(name) &&
		name.split('/').every((part) => part !== '' && part !== '.' && part !== '..')
	);
}

// Keep this factory self-contained: the opaque-origin sandbox embeds its compiled source.
export function createGeneratedFileTracker(
	FS: PyodideInterface['FS'],
	limits: typeof GENERATED_FILE_LIMITS,
	isSafeName: typeof isSafeGeneratedFileName
) {
	const root = '/mnt/uploads';

	function checkRoot() {
		for (const path of ['/mnt', root]) {
			if (!FS.isDir(FS.lstat(path).mode)) throw new Error(`${path} must be a real directory`);
		}
	}

	function stage(files: GeneratedFile[], dir = root) {
		if (files.length === 0) return;
		checkRoot();
		if (dir !== root && !dir.startsWith(`${root}/`)) throw new Error('Input directory is outside /mnt/uploads');
		const relativeDir = dir === root ? '' : dir.slice(root.length + 1);
		for (const file of files) {
			const name = relativeDir ? `${relativeDir}/${file.name}` : file.name;
			if (!isSafeName(name)) throw new Error(`Unsafe input filename: ${JSON.stringify(name)}`);
			let path = root;
			const parts = name.split('/');
			for (const part of parts.slice(0, -1)) {
				path += `/${part}`;
				if (!FS.analyzePath(path).exists) FS.mkdir(path);
				if (!FS.isDir(FS.lstat(path).mode)) throw new Error(`Input directory is not a real directory: ${path}`);
			}
			path += `/${parts.at(-1)}`;
			// lstat also detects dangling symlinks, unlike analyzePath's default stat.
			if (FS.analyzePath(path, true).exists && !FS.isFile(FS.lstat(path).mode)) {
				throw new Error(`Input path is not a regular file: ${path}`);
			}
			FS.writeFile(path, new Uint8Array(file.data));
		}
	}

	function scan() {
		checkRoot();
		const entries: { name: string; size: number; stamp: string; directory: boolean }[] = [];
		const errors: string[] = [];
		let visited = 0;
		function walk(dir: string, depth: number) {
			if (depth > limits.maxDepth) throw new Error(`Generated files exceed ${limits.maxDepth} directory levels`);
			const names = FS.readdir(dir).filter((name) => name !== '.' && name !== '..').sort();
			if (dir !== root && names.length === 0) {
				entries.push({ name: dir.slice(root.length + 1), size: 0, stamp: 'directory', directory: true });
			}
			for (const name of names) {
				if (++visited > limits.maxEntries) throw new Error(`Generated files exceed ${limits.maxEntries} filesystem entries`);
				const path = `${dir}/${name}`;
				const relative = path.slice(root.length + 1);
				if (!isSafeName(relative)) {
					errors.push(`Unsafe generated filename: ${JSON.stringify(relative)}`);
					continue;
				}
				const stat = FS.lstat(path);
				if (FS.isDir(stat.mode)) walk(path, depth + 1);
				else if (FS.isFile(stat.mode)) {
					entries.push({ name: relative, size: stat.size, stamp: `${stat.size}:${stat.mtime}:${stat.ctime}`, directory: false });
				} else errors.push(`Skipped ${relative}: only regular files can be downloaded (no symlinks)`);
			}
		}
		walk(root, 0);
		return { entries, errors };
	}

	async function fingerprint(name: string) {
		// Re-check every ancestor immediately before reading, not just during the initial scan.
		checkRoot();
		let path = root;
		const parts = name.split('/');
		for (const [index, part] of parts.entries()) {
			path += `/${part}`;
			const stat = FS.lstat(path);
			if (!(index === parts.length - 1 ? FS.isFile(stat.mode) : FS.isDir(stat.mode))) {
				throw new Error(`Not a regular generated file path: ${name}`);
			}
		}
		const bytes = FS.readFile(path);
		if (bytes.byteLength > limits.maxBytes) throw new Error(`File exceeds the ${limits.maxBytes / 1024 / 1024} MiB export limit`);
		// Emscripten readFile owns this buffer; it is not a view into live WASM memory.
		const data = (bytes.byteOffset === 0 && bytes.byteLength === bytes.buffer.byteLength
			? bytes.buffer
			: bytes.slice().buffer) as ArrayBuffer;
		const digest = await crypto.subtle.digest('SHA-256', data);
		return { data, hash: Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, '0')).join('') };
	}

	async function snapshot() {
		// Deterministically skipped unsafe entries are never read, so they do not make the baseline partial.
		// Real scan/read failures still throw; collect reports any unsafe entries that remain after execution.
		const { entries } = scan();
		const before = new Map<string, string>();
		for (const entry of entries) {
			before.set(entry.name, entry.directory || entry.size > limits.maxBytes ? entry.stamp : (await fingerprint(entry.name)).hash);
		}
		return before;
	}

	async function collect(before: Map<string, string>) {
		const files: GeneratedFile[] = [];
		const errors: string[] = [];
		let total = 0;
		try {
			const scanned = scan();
			errors.push(...scanned.errors);
			for (const entry of scanned.entries) {
				try {
					if (entry.directory) {
						if (!before.has(entry.name)) errors.push(`Skipped ${entry.name}: directories cannot be downloaded; create an archive instead`);
						continue;
					}
					if (entry.size > limits.maxBytes) {
						if (before.get(entry.name) !== entry.stamp) errors.push(`Skipped ${entry.name}: file exceeds the ${limits.maxBytes / 1024 / 1024} MiB export limit`);
						continue;
					}
					const { data, hash } = await fingerprint(entry.name);
					if (before.get(entry.name) === hash) continue;
					if (files.length >= limits.maxFiles || total + data.byteLength > limits.maxBytes) {
						errors.push(`Skipped ${entry.name}: generated downloads are limited to ${limits.maxFiles} files and ${limits.maxBytes / 1024 / 1024} MiB total per execution`);
						continue;
					}
					total += data.byteLength;
					files.push({ name: entry.name, data });
				} catch (error) {
					errors.push(`Could not export ${entry.name}: ${error instanceof Error ? error.message : String(error)}`);
				}
			}
		} catch (error) {
			errors.push(`Could not export generated files: ${error instanceof Error ? error.message : String(error)}`);
		}
		return { files, errors };
	}

	return { stage, snapshot, collect };
}
