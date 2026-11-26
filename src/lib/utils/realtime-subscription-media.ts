type MediaEvents = {
	ready: () => void;
	levels: (input: number, output: number, playing: boolean) => void;
	error: (message: string) => void;
};

export class SubscriptionMedia {
	private readonly peer = new RTCPeerConnection();
	private readonly playback = new Audio();
	private readonly context = new AudioContext();
	private readonly samples = new Float32Array(256);
	private input?: AnalyserNode;
	private output?: AnalyserNode;
	private microphone?: MediaStreamTrack;
	private finishIce?: () => void;
	private meter?: ReturnType<typeof setInterval>;
	private disconnectTimer?: ReturnType<typeof setTimeout>;
	private lastSound = -Infinity;
	private closed = false;

	constructor(private events: MediaEvents) {
		// Playback uses the native WebRTC stream; the audio context only drives the meters.
		void this.context.resume().catch(() => {});
		this.peer.ontrack = ({ track }) => {
			if (track.kind !== 'audio') return;
			const stream = new MediaStream([track]);
			this.output = this.analyser(stream);
			this.playback.srcObject = stream;
			void this.playback.play().catch(() => {
				if (!this.closed) this.events.error('Could not play voice audio. Retry the call.');
			});
		};
		this.peer.onconnectionstatechange = () => {
			clearTimeout(this.disconnectTimer);
			const state = this.peer.connectionState;
			if (state === 'connected') this.events.ready();
			else if (state === 'failed') this.events.error('Voice media connection failed.');
			else if (state === 'disconnected') {
				this.disconnectTimer = setTimeout(
					() => this.events.error('Voice media connection was lost.'),
					10000
				);
			}
		};
	}

	get connected() {
		return this.peer.connectionState === 'connected';
	}

	async offer(stream: MediaStream) {
		const track = stream.getAudioTracks()[0];
		if (!track) throw new Error('Could not start microphone.');
		this.microphone = track;
		track.enabled = false;
		track.onended = () => this.events.error('Microphone disconnected.');
		this.input = this.analyser(stream);
		// Only media negotiates here. All commands go through the authenticated app socket.
		this.peer.addTransceiver(track, { direction: 'sendrecv', streams: [stream] });
		await this.peer.setLocalDescription(await this.peer.createOffer());
		if (!this.closed && this.peer.iceGatheringState !== 'complete') {
			// Signaling is not trickled; send the SDP with the candidates gathered so far.
			await new Promise<void>((resolve) => {
				const finish = () => {
					clearTimeout(timer);
					this.peer.onicegatheringstatechange = null;
					this.finishIce = undefined;
					resolve();
				};
				const timer = setTimeout(finish, 5000);
				this.finishIce = finish;
				this.peer.onicegatheringstatechange = () => {
					if (this.peer.iceGatheringState === 'complete') finish();
				};
			});
		}
		return this.peer.localDescription?.sdp;
	}

	accept(answer: string) {
		return this.peer.setRemoteDescription({ type: 'answer', sdp: answer });
	}

	startMeters() {
		this.meter = setInterval(() => {
			const input = this.microphone?.enabled ? this.level(this.input) : 0;
			const output = this.level(this.output);
			const now = performance.now();
			if (output > 0.005) this.lastSound = now;
			const playing = now - this.lastSound < 250;
			if (this.playback.muted && now - this.lastSound >= 500) this.playback.muted = false;
			this.events.levels(input, this.playback.muted ? 0 : output, !this.playback.muted && playing);
		}, 40);
	}

	interrupt() {
		// Frameless Bidi has no client response.cancel. Silence this local burst until it drains.
		this.playback.muted = true;
		this.lastSound = performance.now();
	}

	private analyser(stream: MediaStream) {
		const node = this.context.createAnalyser();
		node.fftSize = this.samples.length;
		this.context.createMediaStreamSource(stream).connect(node);
		return node;
	}

	private level(node?: AnalyserNode) {
		if (!node) return 0;
		node.getFloatTimeDomainData(this.samples);
		let energy = 0;
		for (const sample of this.samples) energy += sample * sample;
		return Math.sqrt(energy / this.samples.length);
	}

	close() {
		this.closed = true;
		clearInterval(this.meter);
		clearTimeout(this.disconnectTimer);
		this.finishIce?.();
		if (this.microphone) this.microphone.onended = null;
		this.peer.ontrack = this.peer.onconnectionstatechange = null;
		this.peer.close();
		this.playback.pause();
		this.playback.srcObject = null;
		void this.context.close().catch(() => {});
	}
}
