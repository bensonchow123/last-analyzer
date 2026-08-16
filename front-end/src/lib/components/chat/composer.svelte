<script lang="ts">
	let { disabled, onsend }: { disabled: boolean; onsend: (text: string) => void } = $props();

	let text = $state('');
	let box: HTMLTextAreaElement | undefined = $state();

	// Grow with the content until max-height kicks in and it scrolls
	$effect(() => {
		if (!box) return;
		text;
		box.style.height = 'auto';
		box.style.height = `${box.scrollHeight}px`;
	});

	function submit() {
		const trimmed = text.trim();
		if (!trimmed || disabled) return;
		onsend(trimmed);
		text = '';
	}

	// Anywhere in the box counts as clicking the input itself
	function focusBox() {
		box?.focus();
	}
</script>

<div class="p-4 pt-2">
	<div
		class="max-w-3xl mx-auto flex flex-col gap-2 bg-[#131311] border border-violet-500/50 rounded-lg p-3 cursor-text"
		role="presentation"
		onclick={focusBox}
	>
		<textarea
			bind:this={box}
			bind:value={text}
			rows="1"
			placeholder="Ask about your listening..."
			class="w-full bg-transparent resize-none outline-none text-sm text-white placeholder:text-white/30 max-h-40 overflow-y-auto block"
			onkeydown={(e) => {
				// Enter sends, Shift+Enter makes a newline
				if (e.key === 'Enter' && !e.shiftKey) {
					e.preventDefault();
					submit();
				}
			}}
		></textarea>
		<div class="flex justify-end">
			<button
				class="
					w-7 h-7 rounded-full flex items-center justify-center
					{
						disabled || !text.trim()
						? 'bg-white/5 text-white/30'
						: 'bg-violet-500 hover:bg-violet-400 text-white'
					}
				"
				aria-label="Send"
				onclick={submit}
				disabled={disabled || !text.trim()}
			>
				<svg class="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
					<path d="M12 19V5" />
					<path d="m5 12 7-7 7 7" />
				</svg>
			</button>
		</div>
	</div>
</div>
