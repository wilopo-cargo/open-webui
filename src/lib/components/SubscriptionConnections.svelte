<script lang="ts">
	import { getContext, onMount } from 'svelte';
	import { config, models, settings } from '$lib/stores';
	import { getModels } from '$lib/apis';
	import AccessControlModal from '$lib/components/workspace/common/AccessControlModal.svelte';
	import Modal from '$lib/components/common/Modal.svelte';
	import Spinner from '$lib/components/common/Spinner.svelte';
	import Tooltip from '$lib/components/common/Tooltip.svelte';
	import Plus from '$lib/components/icons/Plus.svelte';
	import XMark from '$lib/components/icons/XMark.svelte';
	import {
		authorizeSubscriptionConnection,
		completeSubscriptionConnection,
		deleteSubscriptionConnection,
		getSubscriptionConnections,
		refreshSubscriptionModels,
		updateSubscriptionConnection,
		SubscriptionApiError,
		type SubscriptionAuthorization,
		type SubscriptionConnection,
		type SubscriptionGrant
	} from '$lib/apis/subscriptions';

	const i18n: any = getContext('i18n');
	export let admin = false;

	let connections: SubscriptionConnection[] = [];
	let loading = true;
	let busy = '';
	let error = '';
	let notice = '';
	let provider: 'codex' | 'claude' = 'codex';
	let name = '';
	let callback = '';
	let pending: SubscriptionAuthorization | null = null;
	let showGrants = false;
	let grantConnection: SubscriptionConnection | null = null;
	let grantDraft: SubscriptionGrant[] = [];
	let showConnectionModal = false;
	$: visibleConnections = admin
		? connections.filter((connection) => connection.scope === 'shared' && connection.can_manage)
		: connections;
	const errorText = (value: unknown, fallback: string): string =>
		value instanceof Error && value.message.trim()
			? value.message.trim()
			: typeof value === 'string' && value.trim()
				? value.trim()
				: fallback;

	const refreshList = async () => {
		connections = await getSubscriptionConnections(localStorage.token);
	};

	const refreshModels = async () => {
		await models.set(
			await getModels(
				localStorage.token,
				$config?.features?.enable_direct_connections ? ($settings?.directConnections ?? null) : null
			)
		);
	};

	const run = async (key: string, action: () => Promise<void>) => {
		busy = key;
		error = '';
		notice = '';
		try {
			await action();
		} catch (e) {
			error = errorText(e, 'Unable to update the subscription connection.');
		} finally {
			busy = '';
		}
	};

	const connect = async () => {
		const authWindow = window.open('about:blank', '_blank');
		if (!authWindow) throw new Error('Allow pop-ups to open the provider authorization page.');
		authWindow.opener = null;
		try {
			pending = await authorizeSubscriptionConnection(localStorage.token, {
				provider,
				name: name.trim() || (provider === 'codex' ? 'Codex' : 'Claude'),
				scope: admin ? 'shared' : 'personal',
				access_grants: []
			});
			authWindow.location.href = pending.authorization_url;
			callback = '';
		} catch (e) {
			authWindow.close();
			throw e;
		}
	};

	const complete = async () => {
		if (!pending || !callback.trim() || busy !== '') return;
		const activePending = pending;
		busy = 'complete';
		error = '';
		notice = '';
		try {
			await completeSubscriptionConnection(localStorage.token, activePending.state, callback.trim());
			pending = null;
			callback = '';
			name = '';
			showConnectionModal = false;
			await refreshList();
			await refreshModels();
			notice = 'Connection added and model list refreshed.';
		} catch (e) {
			if (e instanceof SubscriptionApiError && e.connection_id) {
				pending = null;
				callback = '';
				name = '';
				showConnectionModal = false;
				let refreshError = '';
				try {
					await refreshList();
					await refreshModels();
				} catch (refreshIssue) {
					refreshError = ` ${errorText(refreshIssue, 'The saved connection could not be reloaded.')}`;
				}
				error = `${errorText(e, 'The connection was saved, but model discovery failed.')}${refreshError}`;
			} else {
				error = errorText(e, 'Unable to complete provider authorization.');
			}
		} finally {
			busy = '';
		}
	};

	const updateName = async (connection: SubscriptionConnection, nextName: string) => {
		if (!nextName || nextName === connection.name) return;
		await run(`name:${connection.id}`, async () => {
			await updateSubscriptionConnection(localStorage.token, connection.id, { name: nextName });
			await refreshList();
		});
	};

	const toggleEnabled = async (connection: SubscriptionConnection, enabled: boolean) => {
		await run(`enable:${connection.id}`, async () => {
			await updateSubscriptionConnection(localStorage.token, connection.id, { enabled });
			await refreshList();
			await refreshModels();
		});
	};

	const refreshCatalog = async (connection: SubscriptionConnection) => {
		await run(`models:${connection.id}`, async () => {
			await refreshSubscriptionModels(localStorage.token, connection.id);
			await refreshList();
			await refreshModels();
			notice = 'Provider model list refreshed.';
		});
	};

	const remove = async (connection: SubscriptionConnection) => {
		if (!window.confirm(`Disconnect ${connection.name}?`)) return;
		await run(`delete:${connection.id}`, async () => {
			await deleteSubscriptionConnection(localStorage.token, connection.id);
			await refreshList();
			await refreshModels();
			notice = 'Connection disconnected.';
		});
	};

	const openGrants = (connection: SubscriptionConnection) => {
		grantConnection = connection;
		grantDraft = connection.access_grants.filter(
			(grant) => (grant.principal_type === 'user' || grant.principal_type === 'group') && grant.permission === 'read'
		);
		showGrants = true;
	};

	const saveGrants = async (grants: SubscriptionGrant[]) => {
		if (!grantConnection) return;
		const safeGrants = grants.filter(
			(grant) => (grant.principal_type === 'user' || grant.principal_type === 'group') && grant.permission === 'read'
		);
		await run(`grants:${grantConnection.id}`, async () => {
			const updated = await updateSubscriptionConnection(localStorage.token, grantConnection!.id, {
				access_grants: safeGrants
			});
			grantConnection = updated;
			grantDraft = updated.access_grants;
			await refreshList();
		});
	};

	onMount(async () => {
		try {
			await refreshList();
		} catch (e) {
			error = errorText(e, 'Unable to load subscription connections.');
		} finally {
			loading = false;
		}
	});
</script>

<section class="mt-5 border-t border-gray-100/70 pt-4 text-sm dark:border-white/[0.06]" aria-labelledby="subscription-connections-heading">
	<div class="mb-3 flex items-start justify-between gap-3">
		<div>
			<h3 id="subscription-connections-heading" class="text-xs font-medium text-gray-700 dark:text-gray-200">
				{$i18n.t('Codex and Claude subscriptions')}
			</h3>
			<p class="mt-1 text-[0.6875rem] leading-relaxed text-gray-500 dark:text-gray-400">
				{$i18n.t('Connect a provider account. Credentials stay on the server; paste the callback URL or code#state here to finish authorization.')}
			</p>
		</div>
		<Tooltip content={$i18n.t('Add subscription connection')}>
			<button
				type="button"
				aria-label={$i18n.t('Add subscription connection')}
				class="flex size-6 shrink-0 items-center justify-center rounded-lg text-gray-400 transition-colors hover:bg-black/5 hover:text-gray-900 disabled:opacity-50 dark:text-gray-500 dark:hover:bg-white/5 dark:hover:text-white"
				disabled={busy !== ''}
				on:click={() => {
					if (!pending) error = '';
					showConnectionModal = true;
				}}
			>
				<Plus />
			</button>
		</Tooltip>
	</div>

	{#if !admin}
		<p class="mb-3 text-xs text-gray-500 dark:text-gray-400">{$i18n.t('Connect your personal provider account.')}</p>
	{:else}
		<p class="mb-3 text-xs text-gray-500 dark:text-gray-400">{$i18n.t('Create a shared connection that can be granted to selected users or groups.')}</p>
	{/if}

	{#if error && !showConnectionModal}<p class="mb-2 text-xs text-red-600 dark:text-red-400" role="alert">{error}</p>{/if}
	{#if notice}<p class="mb-2 text-xs text-green-700 dark:text-green-400" role="status">{notice}</p>{/if}

	{#if loading}
		<div class="flex items-center gap-2 py-3 text-xs text-gray-500" role="status" aria-live="polite"><Spinner className="size-4" /> {$i18n.t('Loading subscription connections…')}</div>
	{:else if visibleConnections.length === 0}
		<p class="py-2 text-xs text-gray-500 dark:text-gray-400">{$i18n.t('No subscription connections yet.')}</p>
	{:else}
		<ul class="space-y-2">
			{#each visibleConnections as connection (connection.id)}
				<li class="rounded-lg border border-gray-200/80 p-3 dark:border-white/[0.08]">
					<div class="flex flex-wrap items-center justify-between gap-2">
						<div class="min-w-0 flex-1">
							<div class="flex flex-wrap items-center gap-2">
								<strong class="truncate text-xs text-gray-800 dark:text-gray-100">{connection.name}</strong>
								<span class="rounded-full bg-gray-100 px-2 py-0.5 text-[0.625rem] text-gray-600 dark:bg-white/10 dark:text-gray-300">{connection.provider} · {connection.scope}</span>
								<span class="text-[0.625rem] {connection.status === 'connected' ? 'text-green-700 dark:text-green-400' : 'text-amber-700 dark:text-amber-400'}">{connection.status === 'connected' ? $i18n.t('Connected') : $i18n.t('Reauthorization required')}</span>
							</div>
							<p class="mt-1 text-[0.6875rem] text-gray-500 dark:text-gray-400">{connection.models.length} {$i18n.t('models')}</p>
						</div>
						{#if connection.can_manage}
							<div class="flex flex-wrap items-center gap-2">
								<label class="flex items-center gap-1.5 text-[0.6875rem] text-gray-600 dark:text-gray-300">
									<input type="checkbox" checked={connection.enabled} disabled={busy !== ''} on:change={(event) => toggleEnabled(connection, (event.currentTarget as HTMLInputElement).checked)} /> {$i18n.t('Enabled')}
								</label>
								<button type="button" class="text-[0.6875rem] text-gray-600 underline disabled:opacity-50 dark:text-gray-300" disabled={busy !== ''} on:click={() => { const value = window.prompt('Rename connection', connection.name); if (value?.trim()) updateName(connection, value.trim()); }}>{$i18n.t('Rename')}</button>
								<button type="button" class="text-[0.6875rem] text-gray-600 underline disabled:opacity-50 dark:text-gray-300" disabled={busy !== ''} on:click={() => refreshCatalog(connection)}>{busy === `models:${connection.id}` ? $i18n.t('Refreshing…') : $i18n.t('Refresh models')}</button>
								{#if connection.scope === 'shared'}<button type="button" class="text-[0.6875rem] text-gray-600 underline dark:text-gray-300" on:click={() => openGrants(connection)}>{$i18n.t('Access')}</button>{/if}
								<button type="button" class="text-[0.6875rem] text-red-600 underline disabled:opacity-50" disabled={busy !== ''} on:click={() => remove(connection)}>{$i18n.t('Disconnect')}</button>
							</div>
						{/if}
					</div>
				</li>
			{/each}
		</ul>
	{/if}
</section>
<Modal
	ariaLabel={$i18n.t('Add subscription connection')}
	size="sm"
	bind:show={showConnectionModal}
>
	<div class="px-5 pt-4 pb-4 text-gray-800 dark:text-gray-100">
		<div class="mb-4 flex items-center justify-between gap-3">
			<h2 class="text-base font-medium">{$i18n.t('Add subscription connection')}</h2>
			<button
				type="button"
				class="rounded-lg p-1 text-gray-500 hover:bg-black/5 hover:text-gray-900 dark:text-gray-400 dark:hover:bg-white/5 dark:hover:text-white"
				aria-label={$i18n.t('Close modal')}
				on:click={() => (showConnectionModal = false)}
			>
				<XMark className="size-5" />
			</button>
		</div>
		<form
			class="space-y-3"
			on:submit|stopPropagation={(event) => {
				event.preventDefault();
				if (!pending && busy === '') run('authorize', connect);
			}}
		>
			<label class="block text-xs text-gray-600 dark:text-gray-300">
				{$i18n.t('Provider')}
				<select
					bind:value={provider}
					disabled={busy !== '' || pending !== null}
					class="mt-1 h-9 w-full rounded-lg border border-gray-200 bg-white px-2 text-xs disabled:opacity-50 dark:border-gray-700 dark:bg-gray-900"
				>
					<option value="codex">Codex</option>
					<option value="claude">Claude</option>
				</select>
			</label>
			<label class="block text-xs text-gray-600 dark:text-gray-300">
				{$i18n.t('Connection name')}
				<input
					bind:value={name}
					disabled={busy !== '' || pending !== null}
					class="mt-1 h-9 w-full rounded-lg border border-gray-200 bg-transparent px-2 text-xs disabled:opacity-50 dark:border-gray-700"
					placeholder={provider === 'codex' ? $i18n.t('Codex connection') : $i18n.t('Claude connection')}
				/>
			</label>
			<p class="text-[0.6875rem] leading-relaxed text-gray-500 dark:text-gray-400">
				{provider === 'codex'
					? $i18n.t('Sign in to your ChatGPT account in the provider window, then paste the redirect URL here.')
					: $i18n.t('Sign in to your Claude account in the provider window, then paste the redirect URL here.')}
			</p>
			{#if pending}
				<div class="rounded-lg border border-blue-200 bg-blue-50/50 p-3 dark:border-blue-900 dark:bg-blue-950/20" aria-labelledby="subscription-callback-label">
					<p id="subscription-callback-label" class="text-xs font-medium text-gray-700 dark:text-gray-200">{$i18n.t('Finish authorization')}</p>
					<p class="mt-1 break-all text-[0.6875rem] text-gray-500 dark:text-gray-400">{$i18n.t('After approving access, copy the full redirect URL from the address bar, or enter code#state. Redirect:')} <code>{pending.redirect_uri}</code></p>
					<label class="mt-2 block text-xs text-gray-600 dark:text-gray-300">{$i18n.t('Callback URL or code#state')}
						<input
							bind:value={callback}
							autocomplete="off"
							disabled={busy !== ''}
							on:keydown={(event) => {
								if (event.key === 'Enter') {
									event.preventDefault();
									if (!busy) complete();
								}
							}}
							class="mt-1 h-9 w-full rounded-lg border border-gray-200 bg-white px-2 text-xs disabled:opacity-50 dark:border-gray-700 dark:bg-gray-900"
						/>
					</label>
					<div class="mt-3 flex flex-wrap gap-2">
						<button
							type="button"
							class="h-8 rounded-lg bg-gray-900 px-3 text-xs text-white disabled:opacity-50 dark:bg-gray-100 dark:text-gray-900"
							disabled={!callback.trim() || busy !== ''}
							on:click={complete}
						>{busy === 'complete' ? $i18n.t('Connecting…') : $i18n.t('Complete')}</button>
						<button
							type="button"
							class="h-8 rounded-lg border border-gray-200 px-3 text-xs disabled:opacity-50 dark:border-gray-700"
							disabled={busy !== ''}
							on:click={() => {
								pending = null;
								callback = '';
								error = '';
							}}
						>{$i18n.t('Cancel')}</button>
					</div>
				</div>
			{:else}
				<div class="flex justify-end">
					<button
						type="submit"
						class="h-8 rounded-lg bg-gray-900 px-3 text-xs text-white disabled:opacity-50 dark:bg-gray-100 dark:text-gray-900"
						disabled={busy !== ''}
					>{busy === 'authorize' ? $i18n.t('Opening…') : admin ? $i18n.t('Create shared') : $i18n.t('Connect')}</button>
				</div>
			{/if}
			{#if error}<p class="text-xs text-red-600 dark:text-red-400" role="alert">{error}</p>{/if}
		</form>
	</div>
</Modal>

<AccessControlModal
	bind:show={showGrants}
	bind:accessGrants={grantDraft}
	accessRoles={['read']}
	share={true}
	sharePublic={false}
	shareOpen={false}
	shareUsers={true}
	allowGroups={true}
	onChange={saveGrants}
/>

