<script lang="ts">
	import type { ChatMessage } from '$lib/types/chat';
	import Message from './message.svelte';
	import ToolActivity from './tool-activity.svelte';

	let {
		messages,
		activity,
		streaming
	}: {
		messages: ChatMessage[];
		activity: { name: string; done: boolean }[];
		streaming: boolean;
	} = $props();

	let container: HTMLElement | null = $state(null);

	// length of the streaming message, the effect below keys on it
	const lastLength = $derived(messages.at(-1)?.content.length ?? 0);

	// keep the scroll pinned to the bottom while content grows
	$effect(() => {
		void lastLength;
		void messages.length;
		if (container) container.scrollTop = container.scrollHeight;
	});
</script>

<div bind:this={container} class="flex-1 overflow-y-auto px-4">
	<div class="max-w-3xl mx-auto flex flex-col gap-3 py-4">
		{#each messages as message, i}
			<!-- activity chips sit above the reply that is being streamed -->
			{#if streaming && i === messages.length - 1}
				<ToolActivity {activity} showThinking={!message.content && activity.every((a) => a.done)} />
			{/if}
			<Message {message} />
		{/each}
	</div>
</div>
