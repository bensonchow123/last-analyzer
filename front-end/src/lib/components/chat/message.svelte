<script lang="ts">
	import type { ChatMessage } from '$lib/types/chat';
	import Markdown from './markdown.svelte';

	let { message }: { message: ChatMessage } = $props();
</script>

{#if message.role === 'user'}
	<div class="self-end max-w-[80%] bg-violet-500/20 rounded-lg px-4 py-2 text-sm whitespace-pre-wrap">
		{message.content}
	</div>
{:else if message.content}
	<div class="self-start w-full text-sm">
		<Markdown text={message.content} />
	</div>
{/if}
