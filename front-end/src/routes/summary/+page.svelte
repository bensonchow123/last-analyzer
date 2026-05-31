<!-- scrobble vault /summary endpoint frontend -->
<script lang="ts">
	import { timeAgo } from '$lib/utils';

	let { data } = $props(); // runs the load function in page.server.ts
	let selectedPeriod = $state(0); // reactive variable, default is 7 days

	// derived detects if `data.summary.generated_at` and recomputes
	const lastSyncedAt = $derived(
		data.summary.last_synced_at ? timeAgo(data.summary.last_synced_at) : ', you never synced'
	);
</script>

<!-- main div, with 100vh and background colour of #0f0f0f, pad on all sides by 6 * 4px, so 24px -->
<div class="min-h-screen bg-[#0f0f0f] p-6"> 
	<!-- header, shows last syned -->
	<div>
		<h1 class="text-2xl text-white">Your Music Summary</h1>
		<p class="text-sm text-white/50">Last synced {lastSyncedAt}</p>

	</div>

	<!-- time period tab buttons, on click update the `selectedPeriod` variable -->
	<div>
		{#each data.summary.periods as period, i}
			<button onclick={() => selectedPeriod = i}>
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