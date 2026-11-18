import { WEBUI_API_BASE_URL } from '$lib/constants';

export type SubscriptionGrant = {
	principal_type: 'user' | 'group';
	principal_id: string;
	permission: 'read';
};

export type SubscriptionConnection = {
	id: string;
	provider: 'codex' | 'claude';
	name: string;
	scope: 'personal' | 'shared';
	enabled: boolean;
	priority: number;
	status: 'connected' | 'reauth_required';
	expires_at: number | null;
	models: { id: string; name: string }[];
	can_manage: boolean;
	access_grants: SubscriptionGrant[];
};

export type SubscriptionAuthorization = {
	state: string;
	authorization_url: string;
	redirect_uri: string;
	expires_at: number;
};

export class SubscriptionApiError extends Error {
	connection_id?: string;

	constructor(message: string, connectionId?: string) {
		super(message);
		this.name = 'SubscriptionApiError';
		this.connection_id = connectionId;
	}
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
	typeof value === 'object' && value !== null && !Array.isArray(value);

const errorMessage = (value: unknown, fallback: string): string => {
	if (typeof value === 'string' && value.trim()) return value.trim();
	if (Array.isArray(value)) {
		const messages = value
			.map((item) => (typeof item === 'string' ? item : isRecord(item) ? item.msg : undefined))
			.filter((message): message is string => typeof message === 'string' && !!message.trim());
		if (messages.length) return messages.join('; ');
	}
	if (isRecord(value)) {
		if (typeof value.message === 'string' && value.message.trim()) return value.message.trim();
		if (value.detail !== undefined) return errorMessage(value.detail, fallback);
	}
	return fallback;
};

const request = async <T>(token: string, path: string, method = 'GET', body?: unknown): Promise<T> => {
	const res = await fetch(`${WEBUI_API_BASE_URL}/subscriptions${path}`, {
		method,
		headers: {
			Accept: 'application/json',
			'Content-Type': 'application/json',
			authorization: `Bearer ${token}`
		},
		...(body === undefined ? {} : { body: JSON.stringify(body) })
	});
	if (!res.ok) {
		const payload: unknown = await res.json().catch(() => null);
		const detail = isRecord(payload) ? payload.detail : undefined;
		const detailRecord = isRecord(detail) ? detail : undefined;
		const payloadRecord = isRecord(payload) ? payload : undefined;
		const connectionId =
			typeof detailRecord?.connection_id === 'string'
				? detailRecord.connection_id
				: typeof payloadRecord?.connection_id === 'string'
					? payloadRecord.connection_id
					: undefined;
		throw new SubscriptionApiError(
			errorMessage(detail ?? payloadRecord?.message, `Request failed (${res.status})`),
			connectionId
		);
	}
	return res.status === 204 ? (undefined as T) : res.json();
};

export const getSubscriptionConnections = (token: string) =>
	request<SubscriptionConnection[]>(token, '/');

export const authorizeSubscriptionConnection = (
	token: string,
	body: { provider: 'codex' | 'claude'; name: string; scope: 'personal' | 'shared'; access_grants: SubscriptionGrant[] }
) => request<SubscriptionAuthorization>(token, '/authorize', 'POST', body);

export const completeSubscriptionConnection = (token: string, state: string, callback: string) =>
	request<SubscriptionConnection>(token, '/complete', 'POST', { state, callback });

export const updateSubscriptionConnection = (
	token: string,
	id: string,
	body: { name?: string; enabled?: boolean; priority?: number; access_grants?: SubscriptionGrant[] }
) => request<SubscriptionConnection>(token, `/${encodeURIComponent(id)}`, 'PATCH', body);

export const refreshSubscriptionModels = (token: string, id: string) =>
	request<SubscriptionConnection>(token, `/${encodeURIComponent(id)}/models`, 'POST');

export const deleteSubscriptionConnection = (token: string, id: string) =>
	request<{ status: boolean }>(token, `/${encodeURIComponent(id)}`, 'DELETE');
