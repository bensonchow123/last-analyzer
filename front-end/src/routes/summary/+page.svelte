<!-- scrobble vault /summary endpoint frontend -->
<script lang="ts">
	import BarChart from '$lib/components/summary/barchart.svelte';
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
			<!-- Top artists -->
			<div>
				<p class="text-sm text-white/40 mt-2 mb-2">Artists</p>
				{#each stats.top_artists as artist, i}
					{@const maxPlays = stats.top_artists[0].plays}
					{@const artistImage = artist.artist_image_extralarge ?? artist.artist_image_large ?? artist.artist_image_medium ?? artist.artist_image_small}
					
					<!-- The bar is an absolute positioned element to take up no space, relative make it anchored to this div instead -->
					<!-- Item center makes all items except the bar centered vertically-->
					<div class="relative flex items-center gap-3 py-3">
						<!--The ranking numbers, w-4 makes the double digit numbers rows not inconsistent-->
						<span class="text-xs w-4 text-right {i < 3 ? 'text-violet-400' : 'text-white/30'}">
							{i + 1}
						</span>
						
						<!-- The artist image -->
						{#if artistImage}
							<!--The classes set perfered size, round corners, fill perfer size then prevent flex from squishing it-->
							<img 
								src={artistImage} alt={artist.artist_name}
								referrerpolicy="no-referrer"
								class="w-9 h-9 rounded-md object-cover shrink-0"
							/>
						{:else}
							<!--If no image display the first letter of artist name-->
							<!--Same as the image, but instead of img, create another flex container and center in both axis, in it display the text -->
							<div class="w-9 h-9 rounded-md bg-violet-500/10 shrink-0 flex items-center justify-center text-violet-400 text-sm font-medium">
								{artist.artist_name[0].toUpperCase()}
							</div>
						{/if}

						<!-- Artist name, truncate if too long, take up all remaining space, min-w-0 prevent overflow and prevent the play count off screen-->
						<div class="min-w-0 flex-1">
							<p class="text-sm text-white truncate">{artist.artist_name}</p>
						</div>

						<!-- Play count-->
						<span class="text-xs text-white/30">{artist.plays.toLocaleString()} plays</span>

						<!-- Play count comparison bar, pin to very bottom edge of parent, set to 1px height and use background colour to show unfilled portion. -->
						<div class="absolute bottom-0 left-0 right-0 h-px bg-white/5">
							<!-- Filled portion, fill entiere 1 px, and round the bar corners -->
							<div class="h-full bg-violet-500 rounded-full"
								style="width: {(artist.plays / maxPlays) * 100}%">
							</div>
						</div>
					</div>
				{/each}
			</div>
			<!-- Top albums, see artist chart for documentation-->
			<div>
				<p class="text-sm text-white/40 mt-2 mb-2">Albums</p>
				{#each stats.top_albums as album, i}
					{@const maxPlays = stats.top_albums[0].plays}
					{@const albumImage = album.album_image_extralarge ?? album.album_image_large ?? album.album_image_medium ?? album.album_image_small}

					<div class="relative flex items-center gap-3 py-3">
						<span class="text-xs w-4 text-right {i < 3 ? 'text-violet-400' : 'text-white/30'}">
							{i + 1}
						</span>

						{#if albumImage}
							<img 
								src={albumImage} alt={album.album_name}
								referrerpolicy="no-referrer"
								class="w-9 h-9 rounded-md object-cover shrink-0"
							/>
						{:else}
							<div class="w-9 h-9 rounded-md bg-violet-500/10 shrink-0 flex items-center justify-center text-violet-400 text-sm font-medium">
								{album.album_name[0].toUpperCase()}
							</div>
						{/if}

						<div class="min-w-0 flex-1">
							<p class="text-sm text-white truncate">{album.album_name}</p>
							<p class="text-xs text-white/40 truncate">{album.artist_name}</p>
						</div>

						<span class="text-xs text-white/30">{album.plays.toLocaleString()} plays</span>

						<div class="absolute bottom-0 left-0 right-0 h-px bg-white/5">
							<div class="h-full bg-violet-500 rounded-full"
								style="width: {(album.plays / maxPlays) * 100}%">
							</div>
						</div>
					</div>
				{/each}
			</div>
			<!-- Top tracks, see artist chart for documentation -->
			<div>
				<p class="text-sm text-white/40 mt-2 mb-2">Tracks</p>
				{#each stats.top_tracks as track, i}
					{@const maxPlays = stats.top_tracks[0].plays}
					{@const trackImage = 
					track.album_image_extralarge ?? 
					track.album_image_large ?? 
					track.album_image_medium ?? 
					track.album_image_small ?? 
					track.artist_image_extralarge ?? 
					track.artist_image_large ?? 
					track.artist_image_medium ?? 
					track.artist_image_small}

					<div class="relative flex items-center gap-3 py-3">
						<span class="text-xs w-4 text-right {i < 3 ? 'text-violet-400' : 'text-white/30'}">
							{i + 1}
						</span>

						{#if trackImage}
							<img 
								src={trackImage} alt={track.track_name}
								referrerpolicy="no-referrer"
								class="w-9 h-9 rounded-md object-cover shrink-0"
							/>
						{:else}
							<div class="w-9 h-9 rounded-md bg-violet-500/10 shrink-0 flex items-center justify-center text-violet-400 text-sm font-medium">
								{track.track_name[0].toUpperCase()}
							</div>
						{/if}

						<!-- Also show artist name -->
						<div class="min-w-0 flex-1">
							<p class="text-sm text-white truncate">{track.track_name}</p>
							<p class="text-xs text-white/40 truncate">{track.artist_name}</p>
						</div>

						<span class="text-xs text-white/30">{track.plays.toLocaleString()} plays</span>

						<div class="absolute bottom-0 left-0 right-0 h-px bg-white/5">
							<div class="h-full bg-violet-500 rounded-full"
								style="width: {(track.plays / maxPlays) * 100}%">
							</div>
						</div>
					</div>
				{/each}
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
				<!-- New artists -->
				<div>
					<!-- Only show the first 10 artists/ track /album but still display how many is there in total-->
					<p class="text-sm text-white/40 mt-2 mb-2">
						Artists
						{#if stats.new_in_timeframe.artists_count > 10}
							<span class="text-white/25"> · showing 10 of {stats.new_in_timeframe.artists_count}</span>
						{/if}
					</p>
					<!-- There might not be any new in timeframe artist / albums / tracks-->
					{#if stats.new_in_timeframe.artists.length > 0}
						{#each stats.new_in_timeframe.artists.slice(0, 10) as artist, i}
							<!-- The styling is basicly the same as the top tracks section -->
							{@const maxPlays = stats.new_in_timeframe.artists[0].plays}
							{@const artistImage = artist.artist_image_extralarge ?? artist.artist_image_large ?? artist.artist_image_medium ?? artist.artist_image_small}

							<div class="relative flex items-center gap-3 py-3">
								<span class="text-xs w-4 text-right {i < 3 ? 'text-violet-400' : 'text-white/30'}">
									{i + 1}
								</span>

								{#if artistImage}
									<img
										src={artistImage} alt={artist.artist_name}
										referrerpolicy="no-referrer"
										class="w-9 h-9 rounded-md object-cover shrink-0"
									/>
								{:else}
									<div class="w-9 h-9 rounded-md bg-violet-500/10 shrink-0 flex items-center justify-center text-violet-400 text-sm font-medium">
										{artist.artist_name[0].toUpperCase()}
									</div>
								{/if}

								<div class="min-w-0 flex-1">
									<p class="text-sm text-white truncate">{artist.artist_name}</p>
									<p class="text-xs text-white/40">Discovered {timeAgo(artist.first_listened_at)}</p>
								</div>

								<span class="text-xs text-white/30">{artist.plays.toLocaleString()} plays</span>

								<div class="absolute bottom-0 left-0 right-0 h-px bg-white/5">
									<div class="h-full bg-violet-500 rounded-full"
										style="width: {(artist.plays / maxPlays) * 100}%">
									</div>
								</div>
							</div>
						{/each}
					{:else}
						<p class="text-sm text-white/30">No new artists this period</p>
					{/if}
				</div>

				<!-- New albums -->
				<!-- See artist chart for documentation-->
				<div>
					<p class="text-sm text-white/40 mt-2 mb-2">
						Albums
						{#if stats.new_in_timeframe.albums_count > 10}
							<span class="text-white/25"> · showing 10 of {stats.new_in_timeframe.albums_count}</span>
						{/if}
					</p>
					{#if stats.new_in_timeframe.albums.length > 0}
						{#each stats.new_in_timeframe.albums.slice(0, 10) as album, i}
							{@const maxPlays = stats.new_in_timeframe.albums[0].plays}
							{@const albumImage = album.album_image_extralarge ?? album.album_image_large ?? album.album_image_medium ?? album.album_image_small}

							<div class="relative flex items-center gap-3 py-3">
								<span class="text-xs w-4 text-right {i < 3 ? 'text-violet-400' : 'text-white/30'}">
									{i + 1}
								</span>

								{#if albumImage}
									<img
										src={albumImage} alt={album.album_name}
										referrerpolicy="no-referrer"
										class="w-9 h-9 rounded-md object-cover shrink-0"
									/>
								{:else}
									<div class="w-9 h-9 rounded-md bg-violet-500/10 shrink-0 flex items-center justify-center text-violet-400 text-sm font-medium">
										{album.album_name[0].toUpperCase()}
									</div>
								{/if}

								<div class="min-w-0 flex-1">
									<p class="text-sm text-white truncate">{album.album_name}</p>
									<p class="text-xs text-white/40 truncate">{album.artist_name} · discovered {timeAgo(album.first_listened_at)}</p>
								</div>

								<span class="text-xs text-white/30">{album.plays.toLocaleString()} plays</span>

								<div class="absolute bottom-0 left-0 right-0 h-px bg-white/5">
									<div class="h-full bg-violet-500 rounded-full"
										style="width: {(album.plays / maxPlays) * 100}%">
									</div>
								</div>
							</div>
						{/each}
					{:else}
						<p class="text-sm text-white/30">No new albums this period</p>
					{/if}
				</div>
				<!-- New tracks -->
				<!-- See artist chart for documentation -->
				<div>
					<p class="text-sm text-white/40 mt-2 mb-2">
						Tracks
						{#if stats.new_in_timeframe.tracks_count > 10}
							<span class="text-white/25"> · showing 10 of {stats.new_in_timeframe.tracks_count}</span>
						{/if}
					</p>
					{#if stats.new_in_timeframe.tracks.length > 0}
						{#each stats.new_in_timeframe.tracks.slice(0, 10) as track, i}
							{@const maxPlays = stats.new_in_timeframe.tracks[0].plays}
							{@const trackImage =
								track.album_image_extralarge ??
								track.album_image_large ??
								track.album_image_medium ??
								track.album_image_small ??
								track.artist_image_extralarge ??
								track.artist_image_large ??
								track.artist_image_medium ??
								track.artist_image_small}

							<div class="relative flex items-center gap-3 py-3">
								<span class="text-xs w-4 text-right {i < 3 ? 'text-violet-400' : 'text-white/30'}">
									{i + 1}
								</span>

								{#if trackImage}
									<img
										src={trackImage} alt={track.track_name}
										referrerpolicy="no-referrer"
										class="w-9 h-9 rounded-md object-cover shrink-0"
									/>
								{:else}
									<div class="w-9 h-9 rounded-md bg-violet-500/10 shrink-0 flex items-center justify-center text-violet-400 text-sm font-medium">
										{track.track_name[0].toUpperCase()}
									</div>
								{/if}

								<div class="min-w-0 flex-1">
									<p class="text-sm text-white truncate">{track.track_name}</p>
									<p class="text-xs text-white/40 truncate">{track.artist_name} · discovered {timeAgo(track.first_listened_at)}</p>
								</div>

								<span class="text-xs text-white/30">{track.plays.toLocaleString()} plays</span>

								<div class="absolute bottom-0 left-0 right-0 h-px bg-white/5">
									<div class="h-full bg-violet-500 rounded-full"
										style="width: {(track.plays / maxPlays) * 100}%">
									</div>
								</div>
							</div>
						{/each}
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