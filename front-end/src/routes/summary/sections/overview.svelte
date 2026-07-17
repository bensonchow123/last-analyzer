<script lang="ts">
	import type { PeriodStats } from '$lib/types/summary';
	import { formatHour } from '$lib/utils';

	// import icons
	import Clock from '$lib/icons/clock.svelte';
	import Calender from '$lib/icons/calendar.svelte';
	import Bulb from '$lib/icons/bulb.svelte';

	let { stats, utcOffset }: { stats: PeriodStats; utcOffset: number } = $props();
</script>

<!-- Overview section -->
<section class="mt-4">
	<!-- Title -->
	<p class="text-white text-lg">OVERVIEW</p>

	<!-- Overview row 1 & 2, use grid for uniformed sized cards -->
	<div class="grid grid-cols-2 md:grid-cols-4 mt-2 gap-2">
		<!--Scrobble stats-->
		<div class="bg-[#131311] border border-violet-500/50 rounded-lg p-3">
			<p class="text-sm text-white/70">Tracks listened</p>
			<p class="text-base text-white mt-1">{stats.total_scrobbles.toLocaleString()}</p>
			<p class="text-xs text-white/50">{stats.active_days} active days</p>
		</div>
		<!--Total unique artists, inviside third row so card size consistant-->
		<div class="bg-[#131311] border border-violet-500/50 rounded-lg p-3">
			<p class="text-sm text-white/70">Unique artists</p>
			<p class="text-base text-white mt-1">{stats.unique_artists_count.toLocaleString()}</p>
			<p class="text-xs invisible">.</p>
		</div>
		<!--Total unique albumns, inviside third row so card size consistant-->
		<div class="bg-[#131311] border border-violet-500/50 rounded-lg p-3">
			<p class="text-sm text-white/70">Unique albums</p>
			<p class="text-base text-white mt-1">{stats.unique_albums_count.toLocaleString()}</p>
			<p class="text-xs invisible">.</p>
		</div>
		<!--Total unique albumns, inviside third row so card size consistant-->
		<div class="bg-[#131311] border border-violet-500/50 rounded-lg p-3">
			<p class="text-sm text-white/70">Unique tracks</p>
			<p class="text-base text-white mt-1">{stats.unique_tracks_count.toLocaleString()}</p>
			<p class="text-xs invisible">.</p>
		</div>
		<!--Listening time-->
		<div class="bg-[#131311] border border-violet-500/50 rounded-lg p-3">
			<p class="text-sm text-white/70">Listening time</p>
			{#if stats.listening_time && stats.listening_time.total_string}
				<p class="text-base text-white mt-1">{stats.listening_time.total_string}</p>
				<p class="text-xs text-white/50">
					Missing dur: {stats.listening_time.missing_duration_count}
					{stats.listening_time.missing_duration_count === 1 ? 'track' : 'tracks'}
				</p>
			{:else}
				<p class="text-base text-white/50 mt-1">—</p>
				<p class="text-xs invisible">.</p>
			{/if}
		</div>
		<!--First scrobble,inviside third row so card size consistant-->
		<div class="bg-[#131311] border border-violet-500/50 rounded-lg p-3">
			<p class="text-sm text-white/70">First track recorded</p>
			{#if stats.first_listened_at}
				<p class="text-base text-white mt-1">
					{new Date(stats.first_listened_at * 1000).toLocaleDateString()}
				</p>
				<p class="text-xs text-white/50">
					{new Date(stats.first_listened_at * 1000).toLocaleTimeString()}
				</p>
			{:else}
				<p class="text-base text-white/50 mt-1">—</p>
				<p class="text-xs invisible">.</p>
			{/if}
		</div>
		<!--Last scrobble, inviside third row so card size consistant-->
		<div class="bg-[#131311] border border-violet-500/50 rounded-lg p-3">
			<p class="text-sm text-white/70">Last track recorded</p>
			{#if stats.last_listened_at}
				<p class="text-base text-white mt-1">
					{new Date(stats.last_listened_at * 1000).toLocaleDateString()}
				</p>
				<p class="text-xs text-white/50">
					{new Date(stats.last_listened_at * 1000).toLocaleTimeString()}
				</p>
			{:else}
				<p class="text-base text-white/50 mt-1">—</p>
				<p class="text-xs invisible">.</p>
			{/if}
		</div>
		<!--Most active day, inviside third row so card size consistant-->
		<div class="bg-[#131311] border border-violet-500/50 rounded-lg p-3">
			<p class="text-sm text-white/70">Most active day</p>
			{#if stats.most_active_day}
				<p class="text-base text-white mt-1">
					{stats.most_active_day.day}
				</p>
				<p class="text-xs text-white/50">
					{stats.most_active_day.total_listening_string} ·
					{stats.most_active_day.scrobbles}
					{stats.most_active_day.scrobbles === 1 ? 'track' : 'tracks'}
				</p>
			{:else}
				<p class="text-base text-white/50 mt-1">—</p>
				<p class="text-xs invisible">.</p>
			{/if}
		</div>
	</div>

	<!-- Overview row 3 (the highlight cards)-->
	<div class="grid grid-cols-1 sm:grid-cols-3 gap-2 mt-2">
		<!--Peak hour stats-->
		<div class="bg-[#131311] border border-violet-500/50 rounded-lg p-3">
			<div class="text-violet-400  [&>svg]:w-4 [&>svg]:h-4">
				<Clock/>
			</div>
			<p class="text-sm text-white/70 mt-1">Average peak hour</p>
			{#if stats.listening_clock?.peak_hour}
				<p class="text-base text-white mt-1">{formatHour((stats.listening_clock.peak_hour.hour + utcOffset + 24) % 24)}</p>
				<p class="text-xs text-white/50">
					{stats.listening_clock.peak_hour.average_listening_string} avg ·
					{stats.listening_clock.peak_hour.average_scrobbles} avg
					{stats.listening_clock.peak_hour.average_scrobbles === 1 ? 'track' : 'tracks'}
				</p>
			{:else}
				<p class="text-base text-white mt-1">--</p>
				<p class="text-xs invisible">.</p>
			{/if}
		</div>
		<!--Peak hour stats-->
		<div class="bg-[#131311] border border-violet-500/50 rounded-lg p-3">
			<div class="text-violet-400  [&>svg]:w-4 [&>svg]:h-4">
				<Calender/>
			</div>
			<p class="text-sm text-white/70 mt-1">Average peak weekday</p>
			{#if stats.listening_weekday?.peak_day}
				<p class="text-base text-white mt-1">{stats.listening_weekday.peak_day.weekday}</p>
				<p class="text-xs text-white/50">
					{stats.listening_weekday.peak_day.average_listening_string} avg ·
					{stats.listening_weekday.peak_day.average_scrobbles} avg
					{stats.listening_weekday.peak_day.average_scrobbles === 1 ? 'track' : 'tracks'}
				</p>
			{:else}
				<p class="text-base text-white mt-1">--</p>
				<p class="text-xs invisible">.</p>
			{/if}
		</div>
		<!--Discovery stats-->
		<div class="bg-[#131311] border border-violet-500/50 rounded-lg p-3">
			<div class="text-violet-400 [&>svg]:w-4 [&>svg]:h-4">
				<Bulb/>
			</div>
			<p class="text-sm text-white/70 mt-1">New discoveries</p>
			{#if stats.new_in_timeframe}
				<p class="text-base text-white mt-1">
					{stats.new_in_timeframe.artists_count} new
					{stats.new_in_timeframe.artists_count === 1 ? 'artist' : 'artists'}
				</p>
				<p class="text-xs text-white/50">
					{stats.new_in_timeframe.albums_count}
					{stats.new_in_timeframe.albums_count === 1 ? 'album' : 'albums'} ·
					{stats.new_in_timeframe.tracks_count}
					{stats.new_in_timeframe.tracks_count === 1 ? 'track' : 'tracks'}
				</p>
			{:else}
				<p class="text-base text-white/50 mt-1">—</p>
				<p class="text-xs invisible">.</p>
			{/if}
		</div>
	</div>
</section>
