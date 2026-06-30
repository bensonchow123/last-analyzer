<script lang="ts">
	import type { PeriodStats } from '$lib/types/summary';
	import RankChart from '$lib/components/summary/rankchart.svelte';

	let { stats }: { stats: PeriodStats } = $props();
</script>

<!-- Top artists & albumns & tracks -->
<section>
	<p class="text-white text-lg">TOP CHARTS</p>

	<div class="grid grid-cols-1 md:grid-cols-3 gap-8">
		<!-- Top artists, no subname as there is no extra info to show -->
		<div>
			<p class="text-sm text-white/40 mt-2 mb-2">Artists</p>
			<RankChart rows={stats.top_artists.map(a => ({
				name: a.artist_name,
				image: a.artist_image_extralarge ?? a.artist_image_large ?? a.artist_image_medium ?? a.artist_image_small,
				plays: a.plays
			}))} />
		</div>
		<!-- Top albums, subname shows the artist name under the album -->
		<div>
			<p class="text-sm text-white/40 mt-2 mb-2">Albums</p>
			<RankChart rows={stats.top_albums.map(a => ({
				name: a.album_name,
				subname: a.artist_name,
				image: a.album_image_extralarge ?? a.album_image_large ?? a.album_image_medium ?? a.album_image_small,
				plays: a.plays
			}))} />
		</div>
		<!-- Top tracks, subname shows the artist name, prefer album image then fall back to artist image -->
		<div>
			<p class="text-sm text-white/40 mt-2 mb-2">Tracks</p>
			<RankChart rows={stats.top_tracks.map(t => ({
				name: t.track_name,
				subname: t.artist_name,
				image: t.album_image_extralarge ?? t.album_image_large ?? t.album_image_medium ?? t.album_image_small ?? t.artist_image_extralarge ?? t.artist_image_large ?? t.artist_image_medium ?? t.artist_image_small,
				plays: t.plays
			}))} />
		</div>
	</div>
</section>
