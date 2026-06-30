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
	<BarChart
		bars={stats.listening_clock.hours.map((_, i) => {
			const h = stats.listening_clock.hours[(i - utcOffset + 24) % 24];
			return {
				value: h.average_scrobbles,
				label: formatHour(i),
				sublabel: `${h.average_scrobbles} avg · ${h.average_listening_string}`,
				isPeak: ((stats.listening_clock.peak_hour?.hour ?? -1) + utcOffset + 24) % 24 === i
			};
		})}
		xLabels={[[0,'12am'],[6,'6am'],[12,'12pm'],[18,'6pm'],[23,'11pm']]}
	/>

	<!--Day of week chart-->
	<p class="text-sm text-white/40 mt-4 mb-4">Day of week (avg tracks per weekday)</p>
	<BarChart
		bars={stats.listening_weekday.days.map(d => ({
			value: d.average_scrobbles,
			label: d.weekday,
			sublabel: `${d.average_scrobbles} avg · ${d.average_listening_string}`,
			isPeak: d.weekday_index === stats.listening_weekday.peak_day?.weekday_index
		}))}
		xLabels={stats.listening_weekday.days.map((d, i) => [i, d.weekday.slice(0, 3)])}
	/>
</section>
