<script lang="ts">
	import type { Period } from '$lib/types/summary';

	let { periods, selected = $bindable() }: { periods: Period[]; selected: number } = $props();
</script>

<!-- time period tab buttons, on click update the `selected` variable -->
<!-- Use CSS flexbox as the periods doesn't have uniformed length -->
<section class="flex gap-2 mt-4">
	{#each periods as period, i}
		<button
			class="
				px-4 py-2 rounded-lg text-sm
				{
					selected === i
					? 'bg-violet-500 text-white' // activated if button active
					: 'text-white/40 hover:text-white/50 bg-white/5 hover:bg-white/20' // activate if not active
				}
			"
			onclick={() => selected = i}
		>
			{period.label}
		</button>
	{/each}
</section>
