import { once } from 'node:events';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { setTimeout as delay } from 'node:timers/promises';
import { describe, expect, it } from 'vitest';
import { fetchModel } from './modelRequest';

describe('direct model streaming', () => {
	it('returns at headers and retries body failures only before output', async () => {
		const chunk = 'data: {"choices":[{"delta":{"content":"hello"}}]}\n\n';
		const counts = new Map<string, number>();
		const slowBody = Promise.withResolvers<void>();
		const earlyFailure = Promise.withResolvers<void>();
		const lateFailure = Promise.withResolvers<void>();
		const server = createServer(async (request, response) => {
			request.resume();
			const path = request.url!;
			const attempt = (counts.get(path) ?? 0) + 1;
			counts.set(path, attempt);
			response.writeHead(200, { 'Content-Type': 'text/event-stream' });
			response.flushHeaders();
			if (path === '/slow') {
				await slowBody.promise;
				response.end(chunk);
			} else if (path === '/early' && attempt === 1) {
				await earlyFailure.promise;
				response.destroy();
			} else if (path === '/late') {
				response.write(chunk);
				await lateFailure.promise;
				response.destroy();
			} else {
				response.end(chunk);
			}
		});
		server.listen(0, '127.0.0.1');
		await once(server, 'listening');
		const baseURL = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
		const controller = new AbortController();
		const payload = { stream: true, messages: [{ role: 'user', content: 'hello' }] };
		const init: RequestInit = {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(payload),
			signal: controller.signal
		};
		const deadline = new AbortController();
		try {
			// Withhold every body byte until the response is returned, not merely for a short delay.
			// This integration check uses real Fetch scheduling; fake clocks cannot cover header delivery.
			const slow = await Promise.race([
				fetchModel(`${baseURL}/slow`, init, payload),
				delay(1000, undefined, { signal: deadline.signal }).then(() => {
					throw new Error('Response waited for its first body chunk');
				})
			]);
			deadline.abort();
			expect(slow.status).toBe(200);
			slowBody.resolve();
			expect(await slow.text()).toBe(chunk);
			expect(counts.get('/slow')).toBe(1);

			const early = await fetchModel(`${baseURL}/early`, init, payload);
			earlyFailure.resolve();
			expect(await early.text()).toBe(chunk);
			expect(counts.get('/early')).toBe(2);

			const late = await fetchModel(`${baseURL}/late`, init, payload);
			const reader = late.body!.getReader();
			const decoder = new TextDecoder();
			let output = '';
			while (output.length < chunk.length) {
				const { done, value } = await reader.read();
				expect(done).toBe(false);
				output += decoder.decode(value, { stream: true });
			}
			expect(output).toBe(chunk);
			lateFailure.resolve();
			await expect(reader.read()).rejects.toThrow();
			reader.releaseLock();
			expect(counts.get('/late')).toBe(1);
		} finally {
			deadline.abort();
			slowBody.resolve();
			earlyFailure.resolve();
			lateFailure.resolve();
			controller.abort();
			const closed = once(server, 'close');
			server.close();
			server.closeAllConnections();
			await closed;
		}
	}, 10000);
});
