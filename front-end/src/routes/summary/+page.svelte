<!-- scrobble vault /summary endpoint frontend -->
<script lang="ts">
	import BarChart from '$lib/components/summary/barchart.svelte';
	import RankChart from '$lib/components/summary/rankchart.svelte';
	// import utils used
    import { timeAgo, formatHour } from '$lib/utils';

	// import icons
	import Clock from '$lib/icons/clock.svelte';
	import Calender from '$lib/icons/calendar.svelte';
	import Bulb from '$lib/icons/bulb.svelte';
	
	// handle data
    let { data } = $props(); // runs the load function in page.server.ts
    let selectedPeriod = $state(0); // reactive variable, default is 7 days

    // derived detects if `data.summary.generated_at` and recomputes
    const lastSyncedAt = $derived(
        data.summary.last_synced_at ? timeAgo(data.summary.last_synced_at) : null
    );
	const period = $derived(data.summary.periods[selectedPeriod]);
  	const stats = $derived(period.stats);

	// shift UTC timezone to user local timezone
	const utcOffset = Math.round(new Date().getTimezoneOffset() / -60);
</script>

<!-- Main div, with 100vh and background colour of #0f0f0f, pad on all sides by 6 * 4px, so 24px -->
<!-- I aim to use text-lg for section headers, text-sm for text within sections, mt-4 padding between sections-->
<div class="min-h-screen bg-[#0f0f0f] p-6"> 
    <!-- header, shows last syned -->
    <section>
        <h1 class="text-2xl text-white">Your Music Summary</h1>
        {#if lastSyncedAt}
            <p class="text-sm text-white/50">Last synced {lastSyncedAt}</p>
        {:else}
              <p class="text-sm text-white/50">Not synced to Last.fm</p>
        {/if}
    </section>

	<!-- recent tracks -->
    <!-- this is the most recent 15 tracks overall, not tied to the selected period, so always read off periods[0] -->
    <section class="mt-4">
        <p class="text-white text-lg">RECENT TRACKS</p>

        {#if data.summary.periods[0].stats.recent_tracks.length > 0}
            <!-- grid-cols-1 on mobile, 3 columns on desktop, same pattern as TOP CHARTS / NEW DISCOVERIES -->
            <div class="grid grid-cols-1 md:grid-cols-3 gap-2 mt-2">
                {#each data.summary.periods[0].stats.recent_tracks as track, i}
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

    <!-- The white seperator -->
    <hr class="my-4 border-t border-white/20"/>

    <!-- time period tab buttons, on click update the `selectedPeriod` variable -->
    <!-- Use CSS flexbox as the periods doesn't have uniformed length -->
    <section class="flex gap-2 mt-4">
        {#each data.summary.periods as period, i}
            <button
                class="
                    px-4 py-2 rounded-lg text-sm
                    {	
                        selectedPeriod === i
                        ? 'bg-violet-500 text-white' // activated if button active
                        : 'text-white/40 hover:text-white/50 bg-white/5 hover:bg-white/20' // activate if not active
                    }
                "
                onclick={() => selectedPeriod = i}
            >
                {period.label}
            </button>
        {/each}
    </section>


    <!-- Everything below uses the `selectedPeriod` variable -->
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
                {:else}
                    <p class="text-base text-white/50 mt-1">—</p>
                {/if}
			<p class="text-xs text-white/50">
				Missing dur: {stats.listening_time.missing_duration_count} 
				{stats.listening_time.missing_duration_count === 1 ? 'track' : 'tracks'}
			</p>
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
				{#if stats.listening_clock.peak_hour}
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
				{#if stats.listening_clock.peak_hour}
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

	<!-- The white seperator -->
	<hr class="my-4 border-t border-white/20"/>

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

	<!-- New discoveries section -->
	<!-- This section doesn't exist for the `all_time` time period so need this if statement -->
	{#if stats.new_in_timeframe}

		<!-- The white seperator -->
		<hr class="my-4 border-t border-white/20"/>

		<!-- Actual beginning of the section -->
		<section>
			<!-- Title -->
			<p class="text-white text-lg">NEW DISCOVERIES</p>

			<!-- Time period sub heading -->
			<p class="text-sm text-white/40 mt-1">
				First-time listens in {period.label.toLowerCase()}
			</p>

			<div class="grid grid-cols-1 md:grid-cols-3 gap-8">
				<!-- New artists, only show the first 10 but still display how many there are in total -->
				<div>
					<p class="text-sm text-white/40 mt-2 mb-2">
						Artists
						{#if stats.new_in_timeframe.artists_count > 10}
							<span class="text-white/25"> · showing 10 of {stats.new_in_timeframe.artists_count}</span>
						{/if}
					</p>
					<!-- There might not be any new in timeframe artists / albums / tracks -->
					{#if stats.new_in_timeframe.artists.length > 0}
						<RankChart rows={stats.new_in_timeframe.artists.slice(0, 10).map(a => ({
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
						{#if stats.new_in_timeframe.albums_count > 10}
							<span class="text-white/25"> · showing 10 of {stats.new_in_timeframe.albums_count}</span>
						{/if}
					</p>
					{#if stats.new_in_timeframe.albums.length > 0}
						<RankChart rows={stats.new_in_timeframe.albums.slice(0, 10).map(a => ({
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
						{#if stats.new_in_timeframe.tracks_count > 10}
							<span class="text-white/25"> · showing 10 of {stats.new_in_timeframe.tracks_count}</span>
						{/if}
					</p>
					{#if stats.new_in_timeframe.tracks.length > 0}
						<RankChart rows={stats.new_in_timeframe.tracks.slice(0, 10).map(t => ({
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
	{/if}
    <!-- The white seperator -->
	<hr class="my-4 border-t border-white/20"/>
	
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
</div>