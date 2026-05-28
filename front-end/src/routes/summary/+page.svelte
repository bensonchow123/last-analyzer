<!-- scrobble vault /summary endpoint frontend -->
<script lang="ts">
	let { data } = $props(); // runs the load function in page.server.ts
	let selectedPeriod = $state(0); // reactive variable, default is 7 days
</script>

<!-- main div, with 100vh and background colour of #0f0f0f -->
<div class="min-h-screen bg-[#0f0f0f]"> 
	<!-- header, shows last updated -->
	<div>
		<h1>Your music summary</h1>
		<p>{data.summary.generated_at}</p>
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