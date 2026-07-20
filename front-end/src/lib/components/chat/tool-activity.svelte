<script lang="ts">
	let {
		activity,
		showThinking
	}: {
		activity: { name: string; done: boolean }[];
		showThinking: boolean;
	} = $props();

	const labels: Record<string, string> = {
		describe_schema: 'reading the schema',
		query_music_db: 'querying the music db',
		get_music_summary: 'fetching the listening summary',
		search_music: 'searching by vibe'
	};
</script>

<div class="self-start flex flex-wrap gap-2">
	{#if showThinking}
		<span class="px-3 py-1 rounded-lg text-xs bg-white/5 text-white/50 animate-pulse">thinking...</span>
	{/if}
	{#each activity as step}
		<span
			class="
				px-3 py-1 rounded-lg text-xs
				{
					step.done
					? 'bg-white/5 text-white/30'
					: 'bg-violet-500/25 text-violet-400 animate-pulse'
				}
			"
		>
			{step.done ? '✓ ' : ''}{labels[step.name] ?? step.name}
		</span>
	{/each}
</div>
