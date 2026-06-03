<!-- scrobble vault /summary endpoint frontend -->
<script lang="ts">
	import { timeAgo } from '$lib/utils';

	let { data } = $props(); // runs the load function in page.server.ts
	let selectedPeriod = $state(0); // reactive variable, default is 7 days

	// derived detects if `data.summary.generated_at` and recomputes
	const lastSyncedAt = $derived(
		data.summary.last_synced_at ? timeAgo(data.summary.last_synced_at) : null
	);
</script>

<!-- main div, with 100vh and background colour of #0f0f0f, pad on all sides by 6 * 4px, so 24px -->
<div class="min-h-screen bg-[#0f0f0f] p-6"> 
	<!-- header, shows last syned -->
	<div>
		<h1 class="text-2xl text-white">Your Music Summary</h1>
		{#if lastSyncedAt}
			<p class="text-sm text-white/50">Last synced {lastSyncedAt}</p>
		{:else}
  			<p class="text-sm text-white/50">Not synced to Last.fm</p>
		{/if}
	</div>

	<!-- time period tab buttons, on click update the `selectedPeriod` variable -->
	<div class="flex gap-2 mt-4">
		{#each data.summary.periods as period, i}
			<button
				class="
					px-4 py-2 rounded-lg text-sm
					{	
						selectedPeriod === i
						? 'bg-violet-600 text-white' // activated if button active
						: 'text-white/40 hover:text-white/70 bg-white/5 hover:bg-white/20' // activate if not active
					}
				"
				onclick={() => selectedPeriod = i}
			>
				{period.label}
			</button>
		{/each}
	</div>

	<!-- everything below uses the `selectedPeriod` variable -->
	<div>
		<!-- stats row -->
		<!-- highlight cards -->
		<!-- top tracks + artists -->
		<!-- top albums -->
		<!-- new discoveries -->
		<!-- listening clock -->
		<!-- recent tracks -->
	</div>
</div>