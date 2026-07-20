import { browser } from '$app/environment';
import type { Conversation } from '$lib/types/chat';

const KEY = 'chat-conversations-v1';

export function loadConversations(): Conversation[] {
	if (!browser) return [];
	try {
		return JSON.parse(localStorage.getItem(KEY) ?? '[]');
	} catch {
		return [];
	}
}

export function saveConversations(conversations: Conversation[]): void {
	if (!browser) return;
	try {
		localStorage.setItem(KEY, JSON.stringify(conversations));
	} catch {
		// quota hit, drop the oldest conversation and try once more
		const trimmed = [...conversations].sort((a, b) => b.updatedAt - a.updatedAt).slice(0, -1);
		try {
			localStorage.setItem(KEY, JSON.stringify(trimmed));
		} catch (e) {
			console.error('could not persist conversations', e);
		}
	}
}

// randomUUID does not exist on plain http origins (phone over vpn)
export function newId(): string {
	return crypto.randomUUID?.() ?? Date.now().toString(36) + Math.random().toString(36).slice(2);
}
