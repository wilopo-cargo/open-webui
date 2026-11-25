import { v4 as uuidv4 } from 'uuid';
import { AUDIO_API_BASE_URL } from '$lib/constants';
import { SubscriptionMedia } from './realtime-subscription-media';

export type BridgeSubmission = {
	status: 'submitted' | 'deferred' | 'rejected';
	chatId?: string;
	userMessageId?: string;
	assistantMessageId?: string;
	taskIds?: string[];
};

type ContextMessage = {
	id: string;
	role: string;
	content: string;
	completed: boolean;
	voiceCallId?: string;
};
type CallContext = {
	chatId?: string;
	modelId: string;
	terminalId?: string;
	provider?: string;
	voiceModel?: string;
	voice?: string;
	messages: () => ContextMessage[];
};
type SubscriptionTranscript = {
	draft: string;
	lastFinal?: string;
	inputId?: string;
	interrupted?: boolean;
};
type SubscriptionState = {
	media: SubscriptionMedia;
	hasAnswer: boolean;
	serverReady: boolean;
	events: Set<string>;
	contextIds: Set<string>;
	user: SubscriptionTranscript;
	assistant: SubscriptionTranscript;
	waiting?: { callId: string; timer: ReturnType<typeof setTimeout> };
};
type SubscriptionEvent = {
	type: string;
	event_id?: unknown;
	item?: { id?: unknown; type?: unknown; target?: unknown; text?: unknown; content?: unknown };
	turn?: { id?: unknown; role?: unknown; transcript?: unknown };
};
type VoiceSpeech = {
	id: string;
	metadata?: { input_item_id?: string; call_id?: string; status?: string };
	statusCallId?: string;
	delegated?: boolean;
	speech: Map<string, string>;
	messageId?: string;
};

const callConfiguration = (context: CallContext) =>
	JSON.stringify([
		context.provider,
		context.modelId,
		context.terminalId,
		context.voiceModel,
		context.voice
	]);

type VoiceTurn = {
	callId: string;
	inputId: string;
	userId: string;
	assistantId?: string;
	taskIds?: string[];
	modelId: string;
	finished: boolean;
	approval: boolean;
};
type Options = {
	context: () => CallContext;
	addMessage: (role: string, text: string, voice: any, parentId?: string) => Promise<string>;
	submit: (
		text: string,
		options: { _raw: boolean; bridge: { userMessageId: string; modelId: string } }
	) => Promise<BridgeSubmission>;
	stop: (messageId: string) => Promise<void>;
	message: (messageId: string) => any;
	visibleText: (message: any) => string;
	saveVoice: (messageId: string, voice: any) => Promise<void>;
	change: () => void;
	error: (message: string) => void;
};

export function getBridgeTurnState(message: any) {
	if (!message) return 'working';
	if (message.error) return 'failed';
	if (message.bridgeCancelled) return 'cancelled';
	if (message.bridgeStopping) return 'working';
	const output = message.output ?? [];
	const results = new Set(
		output
			.filter((item: any) => item.type === 'function_call_output')
			.map((item: any) => item.call_id)
	);
	const pending = output.filter(
		(item: any) =>
			item.type === 'function_call' &&
			!results.has(item.call_id ?? item.id) &&
			!['rejected', 'cancelled', 'failed'].includes(item.status)
	);
	if (
		pending.some(
			(item: any) =>
				['pending', 'queued', 'requires_approval'].includes(item.status) || item.name === 'ask_user'
		)
	)
		return 'approval';
	if (pending.length || !message.done) return 'working';
	return 'completed';
}

/** One browser call, with no provider credentials and no alternate chat execution path. */
export class RealtimeCall {
	connected = false;
	connecting = false;
	muted = false;
	speaking = false;
	playbackActive = false;
	userSpeaking = false;
	working = false;
	approval = false;
	model = '';
	voice = '';
	inputLevel = 0;
	outputLevel = 0;
	error = '';
	animationPlayer?: (name: string) => string;
	animationInterruption = 0;
	private animationCalls = new Set<string>();
	private ws?: WebSocket;
	private subscription?: SubscriptionState;
	private context?: AudioContext;
	private stream?: MediaStream;
	private source?: MediaStreamAudioSourceNode;
	private audio?: AudioWorkletNode;
	private timer?: ReturnType<typeof setInterval>;
	private session = 0;
	private callId = '';
	private token = '';
	private configuration = '';
	private chatContext = '';
	private cancelRequested = false;
	private receivingSpeech = '';
	private savingHistory = 0;
	private lastPong = 0;
	private activeResponse = '';
	private responseRequested = false;
	private speakingResponses = new Set<string>();
	private interrupted = new Set<string>();
	private responses = new Map<string, any>();
	private inputs = new Map<string, { text: string; userId: Promise<string>; modelId: string }>();
	private calls = new Map<string, VoiceTurn>();
	private pending?: VoiceTurn;
	private commands: any[] = [];
	private delegation = Promise.resolve();
	private recording = Promise.resolve();
	private metadata = Promise.resolve();
	private clearId = 0;
	private sentSamples = 0;
	private clears = new Map<number, Set<string>>();

	constructor(private options: Options) {}

	async connect(token: string) {
		if (this.connected || this.connecting) return;
		this.error = '';
		this.connecting = true;
		this.options.change();
		const session = ++this.session;
		this.callId = uuidv4();
		try {
			const context = this.options.context();
			this.token = token;
			this.configuration = callConfiguration(context);
			if (!context.modelId) throw new Error('Select one chat model or agent.');
			if (context.provider === 'openai_subscription') {
				this.subscription = {
					media: new SubscriptionMedia({
						ready: () => {
							if (session === this.session) this.ready();
						},
						levels: (input, output, playing) => {
							if (session === this.session) this.subscriptionLevels(input, output, playing);
						},
						error: (message) => {
							if (session === this.session) this.fail(message);
						}
					}),
					hasAnswer: false,
					serverReady: false,
					events: new Set(),
					contextIds: new Set(),
					user: { draft: '' },
					assistant: { draft: '' }
				};
			} else {
				this.context = new AudioContext({ sampleRate: 24000 });
				if (this.context.sampleRate !== 24000 || !this.context.audioWorklet) {
					throw new Error(
						'This browser cannot use 24 kHz AudioWorklet audio. Use Standard call mode.'
					);
				}
				await this.context.resume();
				if (session !== this.session) return;
			}
			const stream = await navigator.mediaDevices.getUserMedia({
				audio: { channelCount: 1, echoCancellation: true, noiseSuppression: true }
			});
			if (session !== this.session) {
				stream.getTracks().forEach((track) => track.stop());
				return;
			}
			this.stream = stream;
			const media = this.subscription?.media;
			let sdp: string | undefined;
			if (media) {
				sdp = await media.offer(stream);
				if (session !== this.session) return;
				if (!sdp) throw new Error('Could not create a voice media offer.');
			} else {
				const audioContext = this.context!;
				await audioContext.audioWorklet.addModule(
					new URL('./realtime-audio.js', import.meta.url)
				);
				if (session !== this.session) return;
				this.audio = new AudioWorkletNode(audioContext, 'realtime-audio', {
					outputChannelCount: [1]
				});
				this.source = audioContext.createMediaStreamSource(stream);
				this.source.connect(this.audio);
				this.audio.connect(audioContext.destination);
				this.audio.port.onmessage = ({ data }) => this.audioEvent(data);
			}
			const url = new URL(`${AUDIO_API_BASE_URL}/realtime`, location.href);
			url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
			const ws = new WebSocket(url);
			this.ws = ws;
			const started = Date.now();
			this.lastPong = started;
			ws.onopen = () => {
				if (session !== this.session) return;
				this.send({
					type: 'auth',
					token,
					chat_id: context.chatId,
					model_id: context.modelId,
					terminal_id: context.terminalId
				});
				if (sdp) this.send({ type: 'subscription.offer', sdp });
			};
			ws.onmessage = ({ data }) => {
				if (session !== this.session) return;
				try {
					this.event(JSON.parse(data));
				} catch {
					this.fail('Invalid voice event. The call has ended.');
				}
			};
			ws.onerror = () => {
				if (session === this.session) this.fail('Voice connection failed.');
			};
			ws.onclose = () => {
				if (session === this.session) this.fail('Voice connection closed.');
			};
			this.timer = setInterval(() => {
				if (!this.connected && Date.now() - started > 45000)
					this.fail('Voice connection timed out.');
				else if (this.connected) {
					if (Date.now() - this.lastPong > 45000) this.fail('Voice connection stopped responding.');
					else this.send({ type: 'bridge.ping' });
				}
			}, 10000);
		} catch (error) {
			if (session === this.session)
				this.fail(error instanceof Error ? error.message : 'Could not start microphone.');
		}
	}

	private ready() {
		const subscription = this.subscription;
		if (
			this.connected ||
			(subscription && (!subscription.serverReady || !subscription.media.connected))
		)
			return;
		this.connected = true;
		this.connecting = false;
		this.lastPong = Date.now();
		this.syncContext();
		if (subscription) {
			this.stream?.getAudioTracks().forEach((track) => (track.enabled = !this.muted));
			subscription.media.startMeters();
		} else this.audio?.port.postMessage({ type: 'capture', enabled: !this.muted });
		this.options.change();
	}

	private subscriptionLevels(input: number, output: number, playing: boolean) {
		const userSpeaking = !this.muted && input > 0.015;
		if (
			this.speaking === playing &&
			this.userSpeaking === userSpeaking &&
			Math.abs(this.inputLevel - input) <= 0.002 &&
			Math.abs(this.outputLevel - output) <= 0.002
		)
			return;
		this.speaking = this.playbackActive = playing;
		this.userSpeaking = userSpeaking;
		this.inputLevel = input;
		this.outputLevel = output;
		this.options.change();
	}

	private send(event: any) {
		if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(event));
	}

	private audioEvent(data: any) {
		if (data.type === 'input' && this.connected && !this.muted) {
			// PCM expands by 4/3 in JSON base64; one second is 64 KB on the wire.
			if ((this.ws?.bufferedAmount ?? 0) > 64000) {
				this.fail('Microphone connection is more than one second behind.');
				return;
			}
			this.send({
				type: 'input_audio_buffer.append',
				audio: btoa(String.fromCharCode(...new Uint8Array(data.pcm)))
			});
		} else if (data.type === 'playback') {
			// Ignore reports queued before an interruption cleared the audio.
			if ((data.clearId ?? 0) < this.clearId) return;
			const playbackActive = !!data.playbackActive;
			const playbackChanged = this.playbackActive !== playbackActive;
			this.playbackActive = playbackActive;
			const inputLevel = this.muted ? 0 : (data.inputLevel ?? 0);
			const outputLevel = data.outputLevel ?? 0;
			const levelsChanged =
				Math.abs(inputLevel - this.inputLevel) > 0.002 ||
				Math.abs(outputLevel - this.outputLevel) > 0.002;
			const nextSpeaking = data.queued > 0 || this.sentSamples > data.received;
			const speakingChanged = this.speaking !== nextSpeaking;
			this.speaking = nextSpeaking;
			if (!this.speaking && !this.activeResponse && !this.responseRequested)
				this.speakingResponses.clear();
			if (levelsChanged || speakingChanged || playbackChanged) {
				this.inputLevel = inputLevel;
				this.outputLevel = outputLevel;
				this.options.change();
			}
			this.flush();
		} else if (data.type === 'cleared') {
			const responses = this.clears.get(data.id);
			this.clears.delete(data.id);
			const latency = (this.context?.baseLatency ?? 0) + (this.context?.outputLatency ?? 0);
			for (const item of data.rendered) {
				if (!responses?.has(item.response_id)) continue;
				this.send({
					type: 'conversation.item.truncate',
					item_id: item.item_id,
					content_index: item.content_index,
					audio_end_ms: Math.max(0, Math.floor(item.samples / 24 - latency * 1000))
				});
			}
			// An item may have been queued but never rendered.
			for (const responseId of responses ?? []) {
				for (const item of this.responses.get(responseId)?.audio?.values() ?? []) {
					if (
						!data.rendered.some(
							(entry: any) =>
								entry.item_id === item.item_id && entry.content_index === item.content_index
						)
					) {
						this.send({ type: 'conversation.item.truncate', ...item, audio_end_ms: 0 });
					}
				}
			}
			this.flush();
		}
	}

	private enqueue(command: any) {
		if (
			command.type === 'bridge.status' &&
			this.commands.some((item) => item.type === 'bridge.status' && item.status === command.status)
		)
			return;
		if (command.type === 'bridge.respond' && command.item_id) {
			// The newest reply hears every transcript, so older unsent ones are redundant.
			this.commands = [command, ...this.commands.filter((item) => !item.item_id)];
		} else this.commands.push(command);
		this.flush();
	}

	private flush() {
		if (
			!this.connected ||
			this.receivingSpeech ||
			this.savingHistory ||
			this.activeResponse ||
			this.responseRequested ||
			this.speaking ||
			this.clears.size
		)
			return;
		this.syncModel();
		if (!this.connected) return;
		const command = this.commands.shift();
		if (command) {
			this.syncContext();
			this.responseRequested = true;
			this.send(command);
		}
	}

	syncModel() {
		if (
			!this.connected ||
			this.receivingSpeech ||
			(this.subscription && (this.userSpeaking || this.subscription.waiting)) ||
			this.savingHistory ||
			this.working ||
			this.speaking ||
			this.activeResponse ||
			this.responseRequested ||
			this.commands.length
		)
			return;
		const context = this.options.context();
		if (this.configuration !== callConfiguration(context)) {
			const token = this.token;
			this.end();
			void this.connect(token);
		}
	}

	private syncContext() {
		if (!this.connected) return;
		const subscription = this.subscription;
		// Codex only accepts appends. PCM replaces its bounded snapshot as the chat changes.
		let budget = 64000;
		const messages = this.options
			.context()
			.messages()
			.slice(-100)
			.filter((message) => {
				if (!subscription) return true;
				if (
					!message.completed ||
					message.voiceCallId === this.callId ||
					subscription.contextIds.has(message.id)
				)
					return false;
				subscription.contextIds.add(message.id);
				return true;
			})
			.reverse()
			.flatMap((message) => {
				if (!['user', 'assistant'].includes(message.role) || budget <= 0) return [];
				const content = message.content.slice(0, Math.min(32000, budget));
				budget -= content.length;
				return content ? [{ role: message.role, content }] : [];
			})
			.reverse();
		if (subscription) {
			if (messages.length)
				this.send({
					type: 'subscription.context',
					text: messages
						.map(({ role, content }) => `${role}: ${content}`)
						.join('\n')
						.replace(/[\uD800-\uDFFF]/gu, '\uFFFD')
				});
			return;
		}
		const snapshot = JSON.stringify(messages);
		if (snapshot === this.chatContext) return;
		this.chatContext = snapshot;
		this.send({ type: 'bridge.context', messages });
	}

	private event(event: any) {
		const type = event.type;
		if (type === 'bridge.error') {
			if (event.fatal === false) this.options.error(event.message);
			else this.fail(event.message);
			return;
		}
		if (type === 'bridge.pong') {
			this.lastPong = Date.now();
			return;
		}
		const subscription = this.subscription;
		if (type === 'subscription.answer' && subscription) {
			if (subscription.hasAnswer) return;
			if (typeof event.sdp !== 'string' || !event.sdp.trim())
				throw new Error('Missing voice media answer');
			subscription.hasAnswer = true;
			void subscription.media.accept(event.sdp).catch(() => {
				if (this.subscription === subscription) this.fail('Could not apply the voice media answer.');
			});
			return;
		}
		if (type === 'bridge.ready') {
			if (!subscription && event.sample_rate !== 24000) throw new Error('Wrong sample rate');
			this.model = event.model;
			this.voice = event.voice;
			if (subscription) subscription.serverReady = true;
			this.ready();
			return;
		}
		if (subscription) {
			this.subscriptionEvent(subscription, event);
			return;
		}
		if (type === 'input_audio_buffer.speech_started') {
			if (this.muted) return;
			this.receivingSpeech = event.item_id;
			this.userSpeaking = !this.muted;
			// VAD is provisional: retain the audio so a noise-only turn can resume it.
			this.audio?.port.postMessage({ type: 'pause', paused: true });
		} else if (type === 'input_audio_buffer.speech_stopped') {
			if (this.receivingSpeech === event.item_id) this.userSpeaking = false;
		} else if (
			type === 'conversation.item.input_audio_transcription.failed' ||
			type === 'conversation.item.input_audio_transcription.completed'
		) {
			// Transcription events from older segments can arrive during newer speech.
			if (this.receivingSpeech === event.item_id) {
				this.receivingSpeech = '';
				this.userSpeaking = false;
			}
			if (this.inputs.has(event.item_id)) return;
			const failed = type === 'conversation.item.input_audio_transcription.failed';
			const text = failed ? '' : event.transcript?.trim();
			if (text) this.stopSpeaking();
			if (!this.receivingSpeech) this.audio?.port.postMessage({ type: 'pause', paused: false });
			if (!text) {
				// Empty VAD segments are not requests and must not produce assistant replies.
				if (failed)
					this.options.error('A voice segment could not be transcribed. Please try again.');
				this.options.change();
				this.flush();
				return;
			}
			this.recordInput(event.item_id, text, true);
		} else if (type === 'response.created') {
			this.responseRequested = false;
			this.activeResponse = event.response.id;
			if (this.cancelRequested) {
				this.cancelRequested = false;
				this.interrupted.add(event.response.id);
				this.send({ type: 'response.cancel', response_id: event.response.id });
			}
			this.responses.set(event.response.id, {
				...event.response,
				statusCallId: this.pending?.callId,
				speech: new Map(),
				audio: new Map(),
				delegated: false
			});
		} else if (type === 'response.output_audio.delta') {
			if (this.interrupted.has(event.response_id)) return;
			const bytes = Uint8Array.from(atob(event.delta), (char) => char.charCodeAt(0));
			if (bytes.length % 2) throw new Error('Invalid PCM');
			const view = new DataView(bytes.buffer);
			const samples = new Float32Array(bytes.length / 2);
			for (let i = 0; i < samples.length; i++) samples[i] = view.getInt16(i * 2, true) / 32768;
			this.responses.get(event.response_id)?.audio.set(`${event.item_id}:${event.content_index}`, {
				item_id: event.item_id,
				content_index: event.content_index
			});
			this.sentSamples += samples.length;
			this.speakingResponses.add(event.response_id);
			this.speaking = true;
			this.audio?.port.postMessage(
				{
					type: 'audio',
					samples,
					response_id: event.response_id,
					item_id: event.item_id,
					content_index: event.content_index
				},
				[samples.buffer]
			);
		} else if (type === 'response.output_audio_transcript.delta') {
			const response = this.responses.get(event.response_id);
			if (response)
				response.speech.set(
					event.item_id,
					(response.speech.get(event.item_id) ?? '') + event.delta
				);
		} else if (type === 'response.output_audio_transcript.done') {
			this.responses.get(event.response_id)?.speech.set(event.item_id, event.transcript);
		} else if (
			type === 'response.output_item.done' &&
			event.item?.type === 'function_call' &&
			event.item.status === 'completed'
		) {
			const response = this.responses.get(event.response_id);
			if (event.item.name === 'play_animation') {
				if (this.animationCalls.has(event.item.call_id)) return;
				this.animationCalls.add(event.item.call_id);
				let status = 'unavailable';
				if (!response || this.interrupted.has(event.response_id)) status = 'cancelled';
				else {
					try {
						const args = JSON.parse(event.item.arguments);
						if (
							event.animation_valid !== false &&
							Object.keys(args).length === 1 &&
							typeof args.name === 'string'
						)
							status = this.animationPlayer?.(args.name) ?? 'unavailable';
					} catch {
						/* Bad or unavailable gestures never interrupt a voice call. */
					}
					response.animation = true;
					response.animationFailed ||= status !== 'started' && status !== 'cancelled';
				}
				this.send({ type: 'bridge.animation.result', call_id: event.item.call_id, status });
				return;
			}
			if (
				!response ||
				event.item.name !== 'generate_chat_completion' ||
				this.calls.has(event.item.call_id)
			)
				return;
			const args = JSON.parse(event.item.arguments);
			if (typeof args.request !== 'string' || !args.request.trim())
				throw new Error('Invalid function arguments');
			this.delegate(event.item.call_id, response.metadata?.input_item_id, response);
		} else if (type === 'response.done') {
			if (this.activeResponse === event.response.id) this.activeResponse = '';
			this.responseRequested = false;
			this.audio?.port.postMessage({ type: 'done', response_id: event.response.id });
			const response = this.responses.get(event.response.id);
			if (response) {
				response.done = true;
				this.saveSpeech(response);
				if (
					response.animation &&
					// A successful gesture is already the answer. Resume only for failure feedback
					// or a chat-model result that has not produced any spoken answer yet.
					(response.animationFailed ||
						(response.metadata?.call_id &&
							![...response.speech.values()].some((text) => text.trim()))) &&
					!response.delegated &&
					event.response.status === 'completed' &&
					!this.interrupted.has(response.id)
				) {
					this.enqueue({ type: 'bridge.animation.respond', response_id: response.id });
				}
			}
			if (['failed', 'incomplete'].includes(event.response.status))
				this.options.error('The voice response did not complete.');
			this.flush();
		}
		this.options.change();
	}

	private subscriptionEvent(subscription: SubscriptionState, event: SubscriptionEvent) {
		if (!this.connected) return;
		const { user, assistant } = subscription;
		const { type, item, turn } = event;
		const rawId = (type === 'turn.done' ? turn?.id : item?.id) ?? event.event_id;
		const id = typeof rawId === 'string' && rawId ? rawId : undefined;
		if (id) {
			const key = `${type}:${type === 'turn.done' ? turn?.role : ''}:${id}`;
			if (subscription.events.has(key)) return;
			subscription.events.add(key);
		}

		if (type === 'input_transcript.added' || type === 'output_transcript.added') {
			const text = item?.text;
			if (typeof text !== 'string' || !text) return;
			const transcript = type === 'input_transcript.added' ? user : assistant;
			if (!transcript.draft) transcript.inputId = transcript === user ? undefined : user.inputId;
			if (text.startsWith(transcript.draft)) transcript.draft = text;
			else if (!transcript.draft.endsWith(text)) transcript.draft += text;
			return;
		}

		if (type === 'turn.done') {
			const role = turn?.role;
			if (!turn || (role !== 'user' && role !== 'assistant') || typeof turn.transcript !== 'string')
				return;
			const transcript = role === 'user' ? user : assistant;
			const text = turn.transcript.trim() || (role === 'assistant' ? transcript.draft.trim() : '');
			if (!id && !transcript.draft && transcript.lastFinal === text) return;
			transcript.draft = '';
			transcript.lastFinal = text;
			if (role === 'assistant') {
				if (text) this.saveSubscriptionSpeech(subscription, text, id);
				assistant.inputId = undefined;
				assistant.interrupted = false;
				return;
			}

			const { waiting } = subscription;
			subscription.waiting = undefined;
			clearTimeout(waiting?.timer);
			user.inputId = text ? (id ?? uuidv4()) : undefined;
			if (user.inputId) {
				this.recordInput(user.inputId, text);
				if (waiting) this.delegate(waiting.callId, user.inputId);
			} else if (waiting) this.noSubscriptionInput(waiting.callId);
			return;
		}

		if (
			type !== 'delegation.created' ||
			item?.type !== 'delegation' ||
			item.target !== 'client' ||
			typeof item.id !== 'string' ||
			!item.id ||
			!Array.isArray(item.content)
		)
			return;
		// The provider may request delegation before transcription finishes. Never use its request text.
		if (user.inputId) this.delegate(item.id, user.inputId);
		else if (subscription.waiting) this.noSubscriptionInput(item.id);
		else {
			const callId = item.id;
			subscription.waiting = {
				callId,
				timer: setTimeout(() => {
					if (this.subscription !== subscription) return;
					subscription.waiting = undefined;
					this.noSubscriptionInput(callId);
				}, 5000)
			};
		}
	}

	private noSubscriptionInput(callId: string) {
		this.send({
			type: 'subscription.result',
			call_id: callId,
			text: 'No complete spoken request was received. Ask the user to repeat it.'
		});
	}

	private saveSubscriptionSpeech(subscription: SubscriptionState, text: string, id?: string) {
		const itemId = id || uuidv4();
		if (subscription.assistant.interrupted) this.interrupted.add(itemId);
		this.saveSpeech({
			id: itemId,
			metadata: { input_item_id: subscription.assistant.inputId ?? subscription.user.inputId },
			speech: new Map([[itemId, text]])
		});
	}

	private recordInput(inputId: string, text: string, respond = false) {
		const session = this.session;
		const modelId = this.options.context().modelId;
		this.savingHistory++;
		const userId = this.recording.then(() => {
			if (session !== this.session) return '';
			return this.options.addMessage('user', text, {
				call_id: this.callId,
				input_item_id: inputId,
				model: this.model
			});
		});
		this.inputs.set(inputId, { text, userId, modelId });
		this.recording = userId.then(
			() => {
				if (session !== this.session) return;
				this.savingHistory--;
				if (respond) this.enqueue({ type: 'bridge.respond', item_id: inputId });
			},
			() => {
				if (session === this.session) this.fail('Could not save the spoken message.');
			}
		);
	}

	private delegate(callId: string, inputId: string, response?: VoiceSpeech) {
		if (this.calls.has(callId)) return;
		const input = this.inputs.get(inputId);
		if (!input) throw new Error('Function has no transcribed input');
		if (response) response.delegated = true;
		if ([...this.calls.values()].some((call) => call.inputId === inputId)) {
			const text = 'This input has already been delegated.';
			this.send(
				this.subscription
					? { type: 'subscription.result', call_id: callId, text }
					: { type: 'bridge.result', call_id: callId, status: 'cancelled', answer: text }
			);
			return;
		}
		const turn: VoiceTurn = {
			callId,
			inputId,
			userId: '',
			modelId: input.modelId,
			finished: false,
			approval: false
		};
		this.calls.set(callId, turn);
		// A replacement delegation suppresses any old result still waiting to be spoken.
		this.commands = this.commands.filter((command) => !command.call_id);
		const session = this.session;
		this.delegation = this.delegation
			.then(async () => {
				if (session !== this.session) return;
				const previous = this.pending;
				if (previous?.assistantId && !previous.finished) {
					previous.finished = true;
					await this.options.stop(previous.assistantId);
					if (session !== this.session) return;
					this.result(previous, 'cancelled', 'Superseded by a new request.', false);
					await this.options.saveVoice(previous.assistantId, { superseded: true });
				}
				turn.userId = await input.userId;
				// The next model request must include the previous spoken reply in DB history.
				await this.metadata;
				if (session !== this.session) return;
				if (!turn.userId || !this.options.message(turn.userId)) {
					this.result(turn, 'failed', 'The spoken message is no longer available in this chat.');
					return;
				}
				this.pending = turn;
				this.working = true;
				this.options.change();
				const result = await this.options.submit(input.text, {
					_raw: true,
					bridge: { userMessageId: turn.userId, modelId: turn.modelId }
				});
				if (session !== this.session) return;
				if (result.status !== 'submitted' || !result.assistantMessageId) {
					this.result(
						turn,
						result.status === 'deferred' ? 'deferred' : 'failed',
						'Complete the required steps in chat and try again.'
					);
					return;
				}
				turn.assistantId = result.assistantMessageId;
				turn.taskIds = result.taskIds;
				if (response) this.saveSpeech(response);
				this.update();
			})
			.catch(() => {
				if (session === this.session)
					this.fail('Could not submit or stop the selected chat model. Review the request in chat.');
			});
	}

	update() {
		this.syncContext();
		const turn = this.pending;
		if (!turn?.assistantId || turn.finished || !this.connected) return;
		const message = this.options.message(turn.assistantId);
		const state = getBridgeTurnState(message);
		this.approval = state === 'approval';
		if (!this.subscription && state === 'approval' && !turn.approval)
			this.enqueue({ type: 'bridge.status', status: 'approval' });
		turn.approval = this.approval;
		if (['completed', 'failed', 'cancelled'].includes(state)) {
			this.result(
				turn,
				state,
				state === 'completed'
					? this.options.visibleText(message)
					: 'The backend request did not complete successfully.'
			);
		}
		this.options.change();
	}

	private result(turn: VoiceTurn, status: string, answer: string, speak = true) {
		// JSON can use six bytes per character; stay below the subscription bridge's frame limit.
		const limit = this.subscription ? 80000 : 100000;
		const text =
			answer.length <= limit
				? answer
				: 'The complete answer is ready in chat. It is too long to read in this call.';
		if (this.subscription) {
			this.send({
				type: 'subscription.result',
				call_id: turn.callId,
				text: text.replace(/[\uD800-\uDFFF]/gu, '\uFFFD') || 'The request completed. See it in chat.'
			});
		} else {
			this.send({ type: 'bridge.result', call_id: turn.callId, status, answer: text });
			this.commands = this.commands.filter((command) => command.type !== 'bridge.status');
			if (speak) this.enqueue({ type: 'bridge.respond', call_id: turn.callId });
		}
		if (this.pending === turn) {
			this.working = false;
			this.approval = false;
		}
		this.options.change();
	}

	private saveSpeech(response: VoiceSpeech) {
		if (!response.speech.size) return;
		const inputId = response.metadata?.input_item_id;
		const turn = response.metadata?.status
			? this.calls.get(response.statusCallId ?? '')
			: response.metadata?.call_id
				? this.calls.get(response.metadata.call_id)
				: [...this.calls.values()].find((call) => call.inputId === inputId);
		if (response.delegated && !turn?.assistantId) return;
		const input = this.inputs.get(inputId ?? turn?.inputId ?? '');
		const speech = [...response.speech.entries()].map(([item_id, transcript]) => ({
			item_id,
			transcript,
			response_id: response.id,
			model: this.model,
			interrupted: this.interrupted.has(response.id)
		}));
		const metadata = {
			call_id: this.callId,
			input_item_id: inputId ?? turn?.inputId,
			function_call_id: turn?.callId,
			task_ids: turn?.taskIds,
			model: this.model,
			speech
		};
		const savedChatId = this.options.context().chatId;
		const session = this.session;
		this.savingHistory++;
		this.metadata = this.metadata
			.then(async () => {
				const parentId = input ? await input.userId : undefined;
				if (input && (!parentId || !this.options.message(parentId))) return;
				if (
					savedChatId !== this.options.context().chatId &&
					(savedChatId || session !== this.session || !parentId)
				)
					return;
				if (!input && !savedChatId && session !== this.session) return;
				if (turn?.assistantId) await this.options.saveVoice(turn.assistantId, metadata);
				else if (!response.messageId) {
					response.messageId = await this.options.addMessage(
						'assistant',
						speech.map((item) => item.transcript).join('\n'),
						metadata,
						parentId
					);
				} else await this.options.saveVoice(response.messageId, metadata);
			})
			.catch(() => {
				if (session === this.session) this.options.error('Could not save the voice transcript.');
			})
			.finally(() => {
				if (session !== this.session) return;
				this.savingHistory--;
				if (this.subscription) this.syncContext();
				else this.flush();
			});
	}

	stopSpeaking() {
		this.animationInterruption++;
		if (this.subscription) {
			this.subscription.media.interrupt();
			if (this.subscription.assistant.draft) this.subscription.assistant.interrupted = true;
			this.speaking = this.playbackActive = false;
			this.outputLevel = 0;
			this.options.change();
			return;
		}
		if (this.responseRequested) this.cancelRequested = true;
		const responses = new Set(
			[...this.speakingResponses].filter((id) => !this.interrupted.has(id))
		);
		if (this.activeResponse && !this.interrupted.has(this.activeResponse)) {
			responses.add(this.activeResponse);
			this.send({ type: 'response.cancel', response_id: this.activeResponse });
		}
		for (const id of responses) {
			this.interrupted.add(id);
			const response = this.responses.get(id);
			if (response?.done) this.saveSpeech(response);
		}
		this.speakingResponses.clear();
		this.speaking = false;
		this.playbackActive = false;
		this.outputLevel = 0;
		const id = ++this.clearId;
		this.clears.set(id, responses);
		this.audio?.port.postMessage({ type: 'clear', id });
		this.commands = this.commands.filter(
			(command) => command.type !== 'bridge.status' && command.type !== 'bridge.animation.respond'
		);
		this.options.change();
	}

	async stopBackend() {
		const turn = this.pending;
		if (!turn?.assistantId || turn.finished) return;
		const session = this.session;
		turn.finished = true;
		try {
			await this.options.stop(turn.assistantId);
			if (session === this.session && this.connected)
				this.result(
					turn,
					'cancelled',
					'The backend request was cancelled. Completed actions have not been undone.'
				);
		} catch {
			if (session === this.session)
				this.fail('Backend cancellation was not confirmed. Review the request in chat.');
		}
	}

	mute() {
		this.muted = !this.muted;
		if (this.muted) {
			this.inputLevel = 0;
			this.userSpeaking = false;
			this.receivingSpeech = '';
			this.audio?.port.postMessage({ type: 'pause', paused: false });
		}
		this.stream?.getAudioTracks().forEach((track) => {
			track.enabled = !this.muted && (!this.subscription || this.connected);
		});
		this.audio?.port.postMessage({ type: 'capture', enabled: this.connected && !this.muted });
		if (this.muted && !this.subscription) {
			this.send({ type: 'input_audio_buffer.clear' });
			this.flush();
		}
		this.options.change();
	}

	private fail(message: string) {
		this.error = message;
		this.options.error(message);
		this.end();
	}

	end() {
		const subscription = this.subscription;
		if (subscription) {
			clearTimeout(subscription.waiting?.timer);
			const partial = subscription.assistant.draft.trim();
			if (this.connected && partial) {
				subscription.assistant.interrupted = true;
				this.saveSubscriptionSpeech(subscription, partial);
			}
			subscription.media.close();
			this.subscription = undefined;
		} else if (this.connected) {
			this.stopSpeaking();
			for (const response of this.responses.values()) {
				if (!response.done) this.saveSpeech(response);
			}
		}
		++this.session;
		clearInterval(this.timer);
		if (this.ws) {
			this.ws.onopen = null;
			this.ws.onclose = null;
			this.ws.onerror = null;
			this.ws.onmessage = null;
			this.ws.close();
		}
		this.ws = undefined;
		this.stream?.getTracks().forEach((track) => track.stop());
		this.source?.disconnect();
		this.stream = undefined;
		this.source = undefined;
		if (this.audio) {
			this.audio.port.onmessage = null;
			this.audio.disconnect();
			this.audio.port.close();
		}
		void this.context?.close();
		this.context = undefined;
		this.audio = undefined;
		this.connected = this.connecting = this.speaking = this.working = this.approval = false;
		this.inputLevel = this.outputLevel = 0;
		this.playbackActive = false;
		this.responseRequested = false;
		this.cancelRequested = false;
		this.receivingSpeech = '';
		this.userSpeaking = false;
		this.activeResponse = '';
		this.pending = undefined;
		this.chatContext = '';
		this.savingHistory = 0;
		this.delegation = this.recording = this.metadata = Promise.resolve();
		this.sentSamples = 0;
		this.commands = [];
		this.speakingResponses.clear();
		this.interrupted.clear();
		this.responses.clear();
		this.inputs.clear();
		this.calls.clear();
		this.animationCalls.clear();
		this.clears.clear();
		this.clearId = 0;
		this.options.change();
	}
}
