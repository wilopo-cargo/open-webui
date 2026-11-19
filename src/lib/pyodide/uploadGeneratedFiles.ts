import { uploadFile } from '$lib/apis/files';
import { WEBUI_API_BASE_URL } from '$lib/constants';
import { GENERATED_FILE_LIMITS, isSafeGeneratedFileName } from './generatedFiles';

export type GeneratedDownload = { id: string; name: string; url: string };

export type PyodideOutput = {
	stdout?: string | null;
	stderr?: string | null;
	result?: unknown;
	files?: unknown;
	file_errors?: unknown;
};

export async function uploadGeneratedFiles(
	output: PyodideOutput,
	token: string,
	maxFileSizeMB?: number | null
) {
	const files: GeneratedDownload[] = [];
	const errors: string[] = Array.isArray(output.file_errors) && output.file_errors.every((error) => typeof error === 'string')
		? [...output.file_errors]
		: [];
	if (output.file_errors != null && (!Array.isArray(output.file_errors) || output.file_errors.some((error) => typeof error !== 'string'))) {
		errors.push('Invalid generated file error response');
	}
	let total = 0;
	const candidates: unknown[] = Array.isArray(output.files) ? output.files : [];
	if (output.files != null && !Array.isArray(output.files)) errors.push('Invalid generated file response');
	if (candidates.length > GENERATED_FILE_LIMITS.maxFiles) errors.push(`Generated downloads are limited to ${GENERATED_FILE_LIMITS.maxFiles} files per execution`);
	for (const candidate of candidates.slice(0, GENERATED_FILE_LIMITS.maxFiles)) {
		let name = 'generated file';
		try {
			if (typeof candidate !== 'object' || candidate === null || !('name' in candidate) || !('data' in candidate) ||
				!isSafeGeneratedFileName(candidate.name) || !(candidate.data instanceof ArrayBuffer)) {
				throw new Error('Invalid generated file name or bytes');
			}
			const file = { name: candidate.name, data: candidate.data };
			name = file.name;
			if (total + file.data.byteLength > GENERATED_FILE_LIMITS.maxBytes) throw new Error('Generated downloads exceed the 20 MiB total export limit');
			if (maxFileSizeMB && file.data.byteLength > maxFileSizeMB * 1024 * 1024) {
				throw new Error(`File exceeds the configured ${maxFileSizeMB} MB upload limit`);
			}
			total += file.data.byteLength;
			const uploaded = await uploadFile(
				token,
				new File([file.data], file.name.split('/').at(-1)!, { type: 'application/octet-stream' }),
				null,
				false,
				false
			);
			if (!uploaded || uploaded.error || typeof uploaded.id !== 'string' || !uploaded.id) {
				throw new Error(uploaded?.error || 'Upload did not return a stored file ID');
			}
			files.push({
				id: uploaded.id,
				name: file.name,
				url: new URL(`${WEBUI_API_BASE_URL}/files/${encodeURIComponent(uploaded.id)}/content?attachment=true`, window.location.origin).href
			});
		} catch (error) {
			errors.push(`Could not publish ${name}: ${error instanceof Error ? error.message : String(error)}`);
		}
	}
	return {
		stdout: output.stdout ?? null,
		stderr: [output.stderr, ...errors.map((error) => `Generated file download: ${error}`)].filter(Boolean).join('\n') || null,
		result: output.result ?? null,
		files,
		file_errors: errors
	};
}
