<script lang="ts">
	import { getContext } from 'svelte';
	import { WEBUI_API_BASE_URL } from '$lib/constants';
	import CodeExecutionModal from './CodeExecutionModal.svelte';
	import Spinner from '$lib/components/common/Spinner.svelte';
	import Check from '$lib/components/icons/Check.svelte';
	import XMark from '$lib/components/icons/XMark.svelte';
	import EllipsisHorizontal from '$lib/components/icons/EllipsisHorizontal.svelte';

	export let codeExecutions = [];
	export let output = [];

	const i18n = getContext('i18n');

	function parseResult(value) {
		if (typeof value !== 'string') return value;
		try {
			return JSON.parse(value);
		} catch {
			return null;
		}
	}

	function getOutputText(item) {
		return Array.isArray(item?.output)
			? item.output.map((part) => part?.text ?? '').join('')
			: item?.output;
	}

	function getDownload(file) {
		if (
			typeof window === 'undefined' ||
			typeof file?.id !== 'string' ||
			!/^[\da-f]{8}(?:-[\da-f]{4}){3}-[\da-f]{12}$/i.test(file.id) ||
			typeof file.name !== 'string' ||
			!file.name
		) {
			return null;
		}
		const url = new URL(
			`${WEBUI_API_BASE_URL}/files/${encodeURIComponent(file.id)}/content?attachment=true`,
			window.location.origin
		);
		return url.origin === window.location.origin
			? { url: url.href, name: file.name.split('/').pop() }
			: null;
	}

	function getUniqueDownloads(files) {
		const downloads = new Map();
		for (const file of files) {
			const download = getDownload(file);
			if (download && !downloads.has(file.id)) {
				downloads.set(file.id, { id: file.id, ...download });
			}
		}
		return [...downloads.values()];
	}

	$: toolCalls = Object.fromEntries(
		(output ?? [])
			.filter((item) => item?.type === 'function_call' && item.name === 'execute_code')
			.map((item) => [item.call_id ?? item.id, item])
	);
	$: generatedFiles = getUniqueDownloads([
		...(output ?? [])
			.filter((item) => item?.type === 'function_call_output' && toolCalls[item.call_id])
			.flatMap((item) => {
				const result = parseResult(getOutputText(item));
				return Array.isArray(result?.files) ? result.files : [];
			}),
		...(output ?? [])
			.filter((item) => ['code_interpreter', 'open_webui:code_interpreter'].includes(item?.type))
			.flatMap((item) => {
				const result = parseResult(getOutputText(item));
				return Array.isArray(result?.files) ? result.files : [];
			}),
		...(codeExecutions ?? []).flatMap((execution) => {
			const result = execution?.result ?? {};
			const parsed = parseResult(result.output);
			return Array.isArray(result.files)
				? result.files
				: Array.isArray(parsed?.files)
					? parsed.files
					: [];
		})
	]);

	let selectedCodeExecution = null;
	let showCodeExecutionModal = false;

	$: if (codeExecutions) {
		updateSelectedCodeExecution();
	}

	const updateSelectedCodeExecution = () => {
		if (selectedCodeExecution) {
			selectedCodeExecution = codeExecutions.find(
				(execution) => execution.id === selectedCodeExecution.id
			);
		}
	};
</script>

<CodeExecutionModal bind:show={showCodeExecutionModal} codeExecution={selectedCodeExecution} />

{#if codeExecutions?.length > 0}
	<div class="mt-1 mb-2 w-full flex gap-1 items-center flex-wrap">
		{#each codeExecutions ?? [] as execution (execution.id)}
			<div class="flex gap-1 text-xs font-normal">
				<button
					class="flex dark:text-gray-300 py-1 px-1 bg-gray-50 hover:bg-gray-100 dark:bg-gray-850 dark:hover:bg-gray-800 transition rounded-xl max-w-96"
					on:click={() => {
						selectedCodeExecution = execution;
						showCodeExecutionModal = true;
					}}
				>
					<div
						class="bg-white dark:bg-gray-700 rounded-full size-4 flex items-center justify-center"
					>
						{#if execution?.result}
							{#if execution.result?.error}
								<XMark />
							{:else if execution.result?.output}
								<Check strokeWidth="3" className="size-3" />
							{:else}
								<EllipsisHorizontal />
							{/if}
						{:else}
							<Spinner className="size-4" />
						{/if}
					</div>
					<div
						class="flex-1 mx-2 line-clamp-1 code-execution-name {execution?.result ? '' : 'pulse'}"
					>
						{execution.name}
					</div>
				</button>
			</div>
		{/each}
	</div>
{/if}
{#if generatedFiles.length > 0}
	<div class="mt-1 mb-2 flex flex-wrap gap-2 text-xs">
		{#each generatedFiles as download (download.id)}
			<a
				class="inline-flex items-center gap-1 rounded-full border border-gray-100 px-3 py-1.5 text-gray-600 transition hover:bg-gray-100 dark:border-gray-850/30 dark:text-gray-300 dark:hover:bg-gray-800"
				href={download.url}
				download={download.name}
				aria-label="{$i18n.t('Download')} {download.name}"
			>
				{$i18n.t('Download')} {download.name}
			</a>
		{/each}
	</div>
{/if}

<style>
	@keyframes pulse {
		0%,
		100% {
			opacity: 1;
		}
		50% {
			opacity: 0.6;
		}
	}

	.pulse {
		opacity: 1;
		animation: pulse 1.5s ease;
	}
</style>
