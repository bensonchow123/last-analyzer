<script lang="ts">
	import type { PeriodStats } from '$lib/types/summary';
	import BarChart from '$lib/components/summary/barchart.svelte';
	import { formatHour } from '$lib/utils';

	let { stats, utcOffset }: { stats: PeriodStats; utcOffset: number } = $props();
</script>

<!-- Listening patterns section -->
<section>
	<!--Title-->
	<p class="text-white text-lg">LISTENING PATTERNS</p>

	<!--Hour of day chart-->
	<p class="text-sm text-white/40 mt-2 mb-4">Hour of day (avg tracks per hour)</p>
	{#if stats.listening_clock}
		{@const clock = stats.listening_clock}
		<BarChart
			bars={clock.hours.map((_, i) => {
				const h = clock.hours[(i - utcOffset + 24) % 24];
				return {
					value: h.average_scrobbles,
					label: formatHour(i),
					sublabel: `${h.average_scrobbles} avg · ${h.average_listening_string}`,
					isPeak: ((clock.peak_hour?.hour ?? -1) + utcOffset + 24) % 24 === i
				};
			})}
			xLabels={[[0,'12am'],[6,'6am'],[12,'12pm'],[18,'6pm'],[23,'11pm']]}
		/>
	{:else}
		<p class="text-base text-white/50">—</p>
	{/if}

	<!--Day of week chart-->
	<p class="text-sm text-white/40 mt-4 mb-4">Day of week (avg tracks per weekday)</p>
	{#if stats.listening_weekday}
		{@const weekday = stats.listening_weekday}
		<BarChart
			bars={weekday.days.map(d => ({
				value: d.average_scrobbles,
				label: d.weekday,
				sublabel: `${d.average_scrobbles} avg · ${d.average_listening_string}`,
				isPeak: d.weekday_index === weekday.peak_day?.weekday_index
			}))}
			xLabels={weekday.days.map((d, i) => [i, d.weekday.slice(0, 3)])}
		/>
	{:else}
		<p class="text-base text-white/50">—</p>
	{/if}
</section>
