<script lang="ts">
	import type { Conversation } from '$lib/types/chat';

	let {
		conversations,
		activeId = $bindable(),
		open = $bindable(),
		ondelete
	}: {
		conversations: Conversation[];
		activeId: string | null;
		open: boolean;
		ondelete: (id: string) => void;
	} = $props();
</script>

{#if open}
	<aside class="w-64 shrink-0 flex flex-col border-r border-white/10 bg-[#131311]">
		<div class="p-3">
			<button
				class="w-full px-4 py-2 rounded-lg text-sm bg-violet-500 hover:bg-violet-400 text-white"
				onclick={() => (activeId = null)}
			>
				New chat
			</button>
		</div>
		<nav class="flex-1 overflow-y-auto px-3">
			{#each conversations as conversation (conversation.id)}
				<div class="group flex items-center gap-1 mb-1">
					<button
						class="
							flex-1 text-left px-3 py-2 rounded-lg text-sm truncate
							{
								activeId === conversation.id
								? 'bg-white/10 text-white'
								: 'text-white/50 hover:text-white hover:bg-white/5'
							}
						"
						onclick={() => (activeId = conversation.id)}
					>
						{conversation.title}
					</button>
					<button
						class="hidden group-hover:block p-1 text-white/30 hover:text-red-400"
						onclick={() => ondelete(conversation.id)}
						aria-label="delete conversation"
					>
						<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 6 6 18"/><path d="m6 6 12 12"/></svg>
					</button>
				</div>
			{/each}
		</nav>
	</aside>
{/if}
