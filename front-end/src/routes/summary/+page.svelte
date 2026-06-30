<!-- scrobble vault /summary endpoint frontend -->
<script lang="ts">
	import type { PageData } from './$types';

	// import section components
	import Header from './sections/header.svelte';
	import RecentTracks from './sections/recent-tracks.svelte';
	import PeriodTabs from './sections/period-tabs.svelte';
	import Overview from './sections/overview.svelte';
	import TopCharts from './sections/top-charts.svelte';
	import NewDiscoveries from './sections/new-discoveries.svelte';
	import ListeningPatterns from './sections/listening-patterns.svelte';

	// handle data
	let { data }: { data: PageData } = $props(); // runs the load function in page.server.ts
	let selectedPeriod = $state(0); // reactive variable, default is 7 days

	// derived detects when `selectedPeriod` changes and recomputes
	const period = $derived(data.summary.periods[selectedPeriod]);
	const stats = $derived(period.stats);

	// shift UTC timezone to user local timezone
	const utcOffset = Math.round(new Date().getTimezoneOffset() / -60);
</script>

<!-- Main div, with 100vh and background colour of #0f0f0f, pad on all sides by 6 * 4px, so 24px -->
<!-- I aim to use text-lg for section headers, text-sm for text within sections, mt-4 padding between sections-->
<div class="min-h-screen bg-[#0f0f0f] p-6">
	<!-- header, shows last syned -->
	<Header lastSyncedAt={data.summary.last_synced_at} />

	<!-- recent tracks, most recent 15 overall so always read off periods[0] -->
	<RecentTracks tracks={data.summary.periods[0].stats.recent_tracks} />

	<!-- The white seperator -->
	<hr class="my-4 border-t border-white/20"/>

	<!-- time period tab buttons, on click update the `selectedPeriod` variable -->
	<PeriodTabs periods={data.summary.periods} bind:selected={selectedPeriod} />

	<!-- Everything below uses the `selectedPeriod` variable -->
	<Overview {stats} {utcOffset} />

	<!-- The white seperator -->
	<hr class="my-4 border-t border-white/20"/>

	<TopCharts {stats} />

	<!-- New discoveries section -->
	<!-- This section doesn't exist for the `all_time` time period so need this if statement -->
	{#if stats.new_in_timeframe}
		<!-- The white seperator -->
		<hr class="my-4 border-t border-white/20"/>

		<NewDiscoveries discoveries={stats.new_in_timeframe} periodLabel={period.label} />
	{/if}

	<!-- The white seperator -->
	<hr class="my-4 border-t border-white/20"/>

	<ListeningPatterns {stats} {utcOffset} />
</div>
