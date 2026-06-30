<script lang="ts">
	import type { RecentTrack } from '$lib/types/summary';

	let { tracks }: { tracks: RecentTrack[] } = $props();
</script>

<!-- recent tracks -->
<!-- this is the most recent 15 tracks overall, not tied to the selected period, so always read off periods[0] -->
<section class="mt-4">
	<p class="text-white text-lg">RECENT TRACKS</p>

	{#if tracks.length > 0}
		<!-- grid-cols-1 on mobile, 3 columns on desktop, same pattern as TOP CHARTS / NEW DISCOVERIES -->
		<div class="grid grid-cols-1 md:grid-cols-3 gap-2 mt-2">
			{#each tracks as track, i}
				{@const trackImage =
					track.album_image_extralarge ??
					track.album_image_large ??
					track.album_image_medium ??
					track.album_image_small ??
					track.artist_image_extralarge ??
					track.artist_image_large ??
					track.artist_image_medium ??
					track.artist_image_small}
				{@const listenedAt = new Date(track.listened_at * 1000)}

				<div class="flex items-center gap-3 bg-[#131311] border border-violet-500/50 rounded-lg p-3">
					<!--The ranking numbers, w-4 makes the double digit numbers rows not inconsistent-->
					<span class="text-xs w-4 text-right shrink-0 {i < 3 ? 'text-violet-400' : 'text-white/30'}">
						{i + 1}
					</span>

					<!-- The track image -->
					{#if trackImage}
						<img
							src={trackImage} alt={track.track_name}
							referrerpolicy="no-referrer"
							class="w-10 h-10 rounded-md object-cover shrink-0"
						/>
					{:else}
						<div class="w-10 h-10 rounded-md bg-violet-500/10 shrink-0 flex items-center justify-center text-violet-400 text-sm font-medium">
							{track.track_name[0].toUpperCase()}
						</div>
					{/if}

					<!-- Track & artist name, truncate if too long, and when it was listened to in the user's local time -->
					<div class="min-w-0 flex-1">
						<p class="text-sm text-white truncate">{track.track_name}</p>
						<p class="text-xs text-white/40 truncate">{track.artist_name}</p>
						<p class="text-xs text-white/30 truncate">
							{listenedAt.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })} at {listenedAt.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' })}
						</p>
					</div>
				</div>
			{/each}
		</div>
	{:else}
		<p class="text-sm text-white/30 mt-2">No recent tracks</p>
	{/if}
</section>
