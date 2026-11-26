import { expect, it, vi } from 'vitest';
import { RealtimeCall } from './realtime';

it.each(['openai', 'openai_subscription'])(
	'sends one result per completed or cancelled delegation on %s',
	async (provider) => {
		const messages = new Map<string, { content: string; done?: boolean; bridgeCancelled?: boolean }>();
		const stop = vi.fn(async (id: string): Promise<void> => {
			messages.get(id)!.bridgeCancelled = true;
			call.update(); // Chat history can update before stop returns.
		});
		const error = vi.fn();
		const call = new RealtimeCall({
			context: () => ({ provider, modelId: 'chat-model', messages: () => [] }),
			addMessage: async (_role, text) => {
				messages.set(text, { content: text });
				return text;
			},
			submit: async (text) => {
				const assistantMessageId = `${text}-answer`;
				messages.set(assistantMessageId, { content: 'The answer.' });
				return { status: 'submitted', assistantMessageId };
			},
			stop,
			message: (id) => messages.get(id),
			visibleText: (message) => message.content,
			saveVoice: vi.fn().mockResolvedValue(undefined),
			change: vi.fn(),
			error
		});
		// Start after media setup and capture outbound commands without a provider connection.
		const send = vi.fn();
		const subscription = provider === 'openai_subscription';
		call['send'] = send;
		call.connected = true;
		if (subscription)
			call['subscription'] = { contextIds: new Set() } as NonNullable<RealtimeCall['subscription']>;
		const delegate = (id: string) => {
			call['recordInput'](id, id);
			call['delegate'](id, id);
			return call['delegation'];
		};

		await delegate('first');
		messages.get('first-answer')!.done = true;
		call.update();
		call.update();

		await delegate('second');
		await delegate('third'); // Supersedes the still-running second turn, not the completed first.
		await Promise.all([call.stopBackend(), call.stopBackend()]);
		call.update();
		call.update();

		const result = (call_id: string, status: string, answer: string) =>
			subscription
				? { type: 'subscription.result', call_id, text: answer }
				: { type: 'bridge.result', call_id, status, answer };
		const results = send.mock.calls
			.map(([event]) => event)
			.filter(({ type }) => type === 'subscription.result' || type === 'bridge.result');
		expect(results).toEqual([
			result('first', 'completed', 'The answer.'),
			result('second', 'cancelled', 'Superseded by a new request.'),
			result('third', 'cancelled', 'The backend request was cancelled. Completed actions have not been undone.')
		]);
		expect(stop.mock.calls).toEqual([['second-answer'], ['third-answer']]);
		expect(error).not.toHaveBeenCalled();
	}
);
