<!-- chat UI for the last llm service, same theme as /summary -->
<script lang="ts">
	import type { Conversation } from '$lib/types/chat';
	import { loadConversations, saveConversations, newId } from '$lib/chat/storage';
	import { streamChat } from '$lib/chat/client';
	import Sidebar from '$lib/components/chat/sidebar.svelte';
	import Composer from '$lib/components/chat/composer.svelte';
	import MessageList from '$lib/components/chat/message-list.svelte';

	let conversations = $state<Conversation[]>([]);
	let activeId = $state<string | null>(null);
	let sidebarOpen = $state(true);
	let streaming = $state(false);
	let activity = $state<{ name: string; done: boolean }[]>([]);

	// localStorage only exists in the browser, ssr renders the empty shell
	$effect(() => {
		conversations = loadConversations();
	});

	const active = $derived(conversations.find((c) => c.id === activeId) ?? null);
	const sorted = $derived([...conversations].sort((a, b) => b.updatedAt - a.updatedAt));

	function deleteConversation(id: string) {
		conversations = conversations.filter((c) => c.id !== id);
		if (activeId === id) activeId = null;
		saveConversations(conversations);
	}

	async function send(text: string) {
		// a conversation only comes into existence on the first send
		let conversation = active;
		if (!conversation) {
			conversations.unshift({
				id: newId(),
				title: text.length > 40 ? text.slice(0, 40) + '…' : text,
				createdAt: Date.now(),
				updatedAt: Date.now(),
				messages: []
			});
			conversation = conversations[0]; // reread so we hold the reactive proxy
			activeId = conversation.id;
		}

		conversation.messages.push({ role: 'user', content: text });
		// capture the wire payload before the placeholder, roles only user|assistant
		const payload = conversation.messages.map(({ role, content }) => ({ role, content }));
		saveConversations(conversations);

		conversation.messages.push({ role: 'assistant', content: '' });
		const reply = conversation.messages[conversation.messages.length - 1];
		streaming = true;
		activity = [];

		try {
			for await (const event of streamChat(payload)) {
				if (event.type === 'delta') {
					reply.content += event.text;
				} else if (event.type === 'tool_call') {
					activity = [...activity.map((a) => ({ ...a, done: true })), { name: event.name, done: false }];
				} else if (event.type === 'tool_result') {
					activity = activity.map((a) => (a.name === event.name ? { ...a, done: true } : a));
				} else if (event.type === 'error') {
					reply.content += `\n\n> something went wrong: ${event.message}`;
					break;
				} else if (event.type === 'done') {
					break;
				}
			}
		} finally {
			streaming = false;
			activity = [];
			conversation.updatedAt = Date.now();
			saveConversations(conversations);
		}
	}
</script>

<div class="h-full flex bg-[#0f0f0f] text-white">
	<!-- conversation list, collapsible -->
	<Sidebar conversations={sorted} bind:activeId bind:open={sidebarOpen} ondelete={deleteConversation} />

	<main class="flex-1 flex flex-col min-w-0">
		<!-- header: sidebar toggle plus the active conversation title -->
		<header class="flex items-center gap-3 p-4 pb-2">
			<button
				class="p-1 text-white/50 hover:text-white"
				onclick={() => (sidebarOpen = !sidebarOpen)}
				aria-label="toggle sidebar"
			>
				<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="4" x2="20" y1="6" y2="6"/><line x1="4" x2="20" y1="12" y2="12"/><line x1="4" x2="20" y1="18" y2="18"/></svg>
			</button>
			<h1 class="text-lg truncate">{active ? active.title : 'Last-analyser chat'}</h1>
		</header>

		{#if active}
			<MessageList messages={active.messages} {activity} {streaming} />
		{:else}
			<!-- empty state before the first message of a new chat -->
			<div class="flex-1 flex items-center justify-center text-white/40 text-sm">
				Ask about your listening history
			</div>
		{/if}

		<Composer disabled={streaming} onsend={send} />
	</main>
</div>
