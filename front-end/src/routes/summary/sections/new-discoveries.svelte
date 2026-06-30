<script lang="ts">
	import type { NewInTimeframe } from '$lib/types/summary';
	import RankChart from '$lib/components/summary/rankchart.svelte';
	import { timeAgo } from '$lib/utils';

	let { discoveries, periodLabel }: { discoveries: NewInTimeframe; periodLabel: string } = $props();
</script>

<!-- New discoveries section -->
<section>
	<!-- Title -->
	<p class="text-white text-lg">NEW DISCOVERIES</p>

	<!-- Time period sub heading -->
	<p class="text-sm text-white/40 mt-1">
		First-time listens in {periodLabel.toLowerCase()}
	</p>

	<div class="grid grid-cols-1 md:grid-cols-3 gap-8">
		<!-- New artists, only show the first 10 but still display how many there are in total -->
		<div>
			<p class="text-sm text-white/40 mt-2 mb-2">
				Artists
				{#if discoveries.artists_count > 10}
					<span class="text-white/25"> · showing 10 of {discoveries.artists_count}</span>
				{/if}
			</p>
			<!-- There might not be any new in timeframe artists / albums / tracks -->
			{#if discoveries.artists.length > 0}
				<RankChart rows={discoveries.artists.slice(0, 10).map(a => ({
					name: a.artist_name,
					image: a.artist_image_extralarge ?? a.artist_image_large ?? a.artist_image_medium ?? a.artist_image_small,
					plays: a.plays,
					sublabel: `Discovered ${timeAgo(a.first_listened_at)}`
				}))} />
			{:else}
				<p class="text-sm text-white/30">No new artists this period</p>
			{/if}
		</div>

		<!-- New albums, see new artists for documentation -->
		<div>
			<p class="text-sm text-white/40 mt-2 mb-2">
				Albums
				{#if discoveries.albums_count > 10}
					<span class="text-white/25"> · showing 10 of {discoveries.albums_count}</span>
				{/if}
			</p>
			{#if discoveries.albums.length > 0}
				<RankChart rows={discoveries.albums.slice(0, 10).map(a => ({
					name: a.album_name,
					subname: a.artist_name,
					image: a.album_image_extralarge ?? a.album_image_large ?? a.album_image_medium ?? a.album_image_small,
					plays: a.plays,
					sublabel: `discovered ${timeAgo(a.first_listened_at)}`
				}))} />
			{:else}
				<p class="text-sm text-white/30">No new albums this period</p>
			{/if}
		</div>
		<!-- New tracks, see new artists for documentation -->
		<div>
			<p class="text-sm text-white/40 mt-2 mb-2">
				Tracks
				{#if discoveries.tracks_count > 10}
					<span class="text-white/25"> · showing 10 of {discoveries.tracks_count}</span>
				{/if}
			</p>
			{#if discoveries.tracks.length > 0}
				<RankChart rows={discoveries.tracks.slice(0, 10).map(t => ({
					name: t.track_name,
					subname: t.artist_name,
					image: t.album_image_extralarge ?? t.album_image_large ?? t.album_image_medium ?? t.album_image_small ?? t.artist_image_extralarge ?? t.artist_image_large ?? t.artist_image_medium ?? t.artist_image_small,
					plays: t.plays,
					sublabel: `discovered ${timeAgo(t.first_listened_at)}`
				}))} />
			{:else}
				<p class="text-sm text-white/30">No new tracks this period</p>
			{/if}
		</div>
	</div>
</section>
