const retryDelay = (headers: Headers) => {
	for (const [name, scale] of [['Retry-After', 1000], ['retry-after-ms', 1]] as const) {
		const value = headers.get(name);
		if (!value?.trim()) continue;
		const number = Number(value);
		const delay = Number.isFinite(number)
			? number * scale
			: name === 'Retry-After'
				? Math.max(0, Date.parse(value) - Date.now())
				: NaN;
		if (Number.isFinite(delay) && delay >= 0) return delay;
	}
	return 1000;
};

const waitForRetry = async (delay: number, signal: AbortSignal) => {
	// Browser timers overflow above 2^31-1 ms; never shorten a provider's Retry-After.
	while (delay > 0) {
		signal.throwIfAborted();
		const interval = Math.min(delay, 2147483647);
		const { promise, resolve, reject } = Promise.withResolvers<void>();
		const timer = setTimeout(resolve, interval);
		const abort = () => {
			clearTimeout(timer);
			reject(signal.reason);
		};
		signal.addEventListener('abort', abort, { once: true });
		try {
			await promise;
		} finally {
			signal.removeEventListener('abort', abort);
		}
		delay -= interval;
	}
	signal.throwIfAborted();
};

/** Direct provider requests only. Server-side chat and tool execution must not be replayed. */
export const fetchModel = async (
	url: string,
	init: RequestInit,
	payload: Record<string, unknown>
): Promise<Response> => {
	// Construct before the retry loop so URL/header/body validation errors fail immediately.
	const request = new Request(typeof location === 'undefined' ? url : new URL(url, location.href), init);
	const endpoint = new URL(request.url);
	let pathname = endpoint.pathname;
	try {
		pathname = decodeURIComponent(pathname);
	} catch {
		// Preserve malformed escapes for fetch/the server to handle, as before.
	}
	const webuiProxy =
		typeof location !== 'undefined' &&
		endpoint.origin === location.origin &&
		/^\/(?:api(?:\/v1)?|openai|ollama\/v1)\/chat\/completions\/?$/.test(pathname);
	const tools = payload.tools ?? [];
	if (
		webuiProxy ||
		payload.mcp_servers ||
		!Array.isArray(tools) ||
		!tools.every(
			(tool) =>
				tool &&
				typeof tool === 'object' &&
				(tool.type === undefined || tool.type === 'function')
		)
	) {
		return fetch(request);
	}

	const cancellation = new AbortController();
	const signal = AbortSignal.any([request.signal, cancellation.signal]);

	for (let attempt = 0; ; attempt++) {
		let response: Response | undefined;
		let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
		let buffer: Blob | undefined;
		try {
			response = await fetch(request.clone(), { signal });
			if (
				attempt === 0 &&
				(response.status === 429 || (response.status >= 500 && response.status < 600))
			) {
				const delay = retryDelay(response.headers);
				await response.body?.cancel().catch(() => {});
				await waitForRetry(delay, signal);
				continue;
			}
			if (!response.ok || !response.body) return response;
			if (payload.stream || response.headers.get('Content-Type')?.includes('text/event-stream')) {
				reader = response.body.getReader();
			} else {
				buffer = await response.blob();
			}
		} catch (error) {
			await (reader ? reader.cancel() : response?.body?.cancel())?.catch(() => {});
			reader?.releaseLock();
			// Browsers also report TLS/CORS/policy failures as TypeError; never bypass those checks.
			if (
				attempt !== 0 ||
				signal.aborted ||
				!(
					error instanceof TypeError ||
					(error instanceof DOMException && ['NetworkError', 'TimeoutError'].includes(error.name))
				)
			) {
				throw error;
			}
			await waitForRetry(1000, signal);
			continue;
		}

		if (!reader) return new Response(buffer, response);
		let streamReader: ReadableStreamDefaultReader<Uint8Array> | undefined = reader;
		let outputStarted = false;
		// Return at headers; the first body read can still spend the one remaining retry.
		return new Response(new ReadableStream<Uint8Array>({
			async pull(controller) {
				while (streamReader) {
					const currentReader = streamReader;
					try {
						const { done, value } = await currentReader.read();
						if (done) {
							streamReader = undefined;
							currentReader.releaseLock();
							controller.close();
						} else {
							outputStarted = true;
							controller.enqueue(value);
						}
						return;
					} catch (error) {
						await currentReader.cancel().catch(() => {});
						currentReader.releaseLock();
						streamReader = undefined;
						if (
							outputStarted ||
							attempt !== 0 ||
							signal.aborted ||
							!(
								error instanceof TypeError ||
								(error instanceof DOMException && ['NetworkError', 'TimeoutError'].includes(error.name))
							)
						) {
							throw error;
						}
						attempt++;
						await waitForRetry(1000, signal);
						const retried = await fetch(request.clone(), { signal });
						if (!retried.ok) throw await retried.json();
						if (!retried.body) {
							controller.close();
							return;
						}
						streamReader = retried.body.getReader();
					}
				}
			},
			async cancel(reason) {
				cancellation.abort(reason);
				const currentReader = streamReader;
				streamReader = undefined;
				if (currentReader) {
					try {
						await currentReader.cancel(reason);
					} finally {
						currentReader.releaseLock();
					}
				}
			}
		}, { highWaterMark: 0 }), response);
	}
};
