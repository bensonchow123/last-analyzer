import type { ChatEvent, ChatMessage } from '$lib/types/chat';

// fetch + reader instead of EventSource cause EventSource cannot POST
export async function* streamChat(messages: ChatMessage[]): AsyncGenerator<ChatEvent> {
	let res: Response;
	try {
		res = await fetch('/api/chat', {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify({ messages })
		});
	} catch (e) {
		yield { type: 'error', message: `could not reach the chat api: ${e}` };
		return;
	}
	if (!res.ok || !res.body) {
		yield { type: 'error', message: `chat api returned ${res.status}` };
		return;
	}

	const reader = res.body.getReader();
	const decoder = new TextDecoder();
	let buffer = '';
	try {
		while (true) {
			const { done, value } = await reader.read();
			if (done) break;
			buffer += decoder.decode(value, { stream: true });
			const frames = buffer.split('\n\n');
			buffer = frames.pop() ?? '';
			for (const frame of frames) {
				if (frame.startsWith('data: ')) yield JSON.parse(frame.slice(6)) as ChatEvent;
			}
		}
		if (buffer.startsWith('data: ')) yield JSON.parse(buffer.slice(6)) as ChatEvent;
	} catch (e) {
		yield { type: 'error', message: `stream broke: ${e}` };
	}
}
