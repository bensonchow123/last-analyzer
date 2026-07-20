export type ChatRole = 'user' | 'assistant';

export type ChatMessage = { role: ChatRole; content: string };

export type Conversation = {
	id: string;
	title: string;
	createdAt: number;
	updatedAt: number;
	messages: ChatMessage[];
};

// Mirror of the SSE events /chat/stream emits, see SAD 5.3.1
export type ChatEvent =
	| { type: 'delta'; text: string }
	| { type: 'tool_call'; name: string; arguments: string }
	| { type: 'tool_result'; name: string }
	| { type: 'done' }
	| { type: 'error'; message: string };
