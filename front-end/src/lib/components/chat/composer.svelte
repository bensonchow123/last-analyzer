<script lang="ts">
	let { disabled, onsend }: { disabled: boolean; onsend: (text: string) => void } = $props();

	let text = $state('');

	function submit() {
		const trimmed = text.trim();
		if (!trimmed || disabled) return;
		onsend(trimmed);
		text = '';
	}
</script>

<div class="p-4 pt-2">
	<div class="max-w-3xl mx-auto flex items-center gap-2 bg-[#131311] border border-violet-500/50 rounded-lg p-3">
		<textarea
			bind:value={text}
			rows="1"
			placeholder="Ask about your listening..."
			class="flex-1 bg-transparent resize-none outline-none text-sm text-white placeholder:text-white/30 max-h-40"
			onkeydown={(e) => {
				// Enter sends, Shift+Enter makes a newline
				if (e.key === 'Enter' && !e.shiftKey) {
					e.preventDefault();
					submit();
				}
			}}
		></textarea>
		<button
			class="
				px-4 py-2 rounded-lg text-sm
				{
					disabled
					? 'bg-white/5 text-white/30'
					: 'bg-violet-500 hover:bg-violet-400 text-white'
				}
			"
			onclick={submit}
			{disabled}
		>
			Send
		</button>
	</div>
</div>
