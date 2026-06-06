<!-- scrobble vault /summary endpoint frontend -->
<script lang="ts">
    import { timeAgo } from '$lib/utils';

    let { data } = $props(); // runs the load function in page.server.ts
    let selectedPeriod = $state(0); // reactive variable, default is 7 days

    // derived detects if `data.summary.generated_at` and recomputes
    const lastSyncedAt = $derived(
        data.summary.last_synced_at ? timeAgo(data.summary.last_synced_at) : null
    );
	const period = $derived(data.summary.periods[selectedPeriod]);
  	const stats = $derived(period.stats);
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

    <!-- time period tab buttons, on click update the `selectedPeriod` variable -->
    <!-- Use CSS flexbox as the periods doesn't have uniformed length -->
    <section class="flex gap-2 mt-4">
        {#each data.summary.periods as period, i}
            <button
                class="
                    px-4 py-2 rounded-lg text-sm
                    {	
                        selectedPeriod === i
                        ? 'bg-violet-600 text-white' // activated if button active
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
			<div class="bg-[#131311] border border-violet-600/50 rounded-lg p-3">
				<p class="text-sm text-white/70">Tracks listened</p>
				<p class="text-base text-white mt-1">{stats.total_scrobbles.toLocaleString()}</p>
				<p class="text-xs text-white/50">{stats.active_days} active days</p>
			</div>
			<!--Total unique artists, inviside third row so card size consistant-->
			<div class="bg-[#131311] border border-violet-600/50 rounded-lg p-3">
				<p class="text-sm text-white/70">Unique artists</p>
				<p class="text-base text-white mt-1">{stats.unique_artists_count.toLocaleString()}</p>
				<p class="text-xs invisible">.</p>
			</div>
			<!--Total unique albumns, inviside third row so card size consistant-->
			<div class="bg-[#131311] border border-violet-600/50 rounded-lg p-3">
				<p class="text-sm text-white/70">Unique albums</p>
				<p class="text-base text-white mt-1">{stats.unique_albums_count.toLocaleString()}</p>
				<p class="text-xs invisible">.</p>
			</div>
			<!--Total unique albumns, inviside third row so card size consistant-->
			<div class="bg-[#131311] border border-violet-600/50 rounded-lg p-3">
				<p class="text-sm text-white/70">Unique tracks</p>
				<p class="text-base text-white mt-1">{stats.unique_tracks_count.toLocaleString()}</p>
				<p class="text-xs invisible">.</p>
			</div>
			<!--Listening time-->
			<div class="bg-[#131311] border border-violet-600/50 rounded-lg p-3">
				<p class="text-sm text-white/70">Listening time</p>
                {#if stats.listening_time && stats.listening_time.total_string}
                    <p class="text-base text-white mt-1">{stats.listening_time.total_string}</p>
                {:else}
                    <p class="text-base text-white/50 mt-1">—</p>
                {/if}
				<p class="text-xs text-white/50">Missing dur: {stats.listening_time.missing_duration_count} tracks</p>
			</div>
			<!--First scrobble,inviside third row so card size consistant-->
			<div class="bg-[#131311] border border-violet-600/50 rounded-lg p-3">
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
			<div class="bg-[#131311] border border-violet-600/50 rounded-lg p-3">
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
			<div class="bg-[#131311] border border-violet-600/50 rounded-lg p-3">
				<p class="text-sm text-white/70">Most active day</p>
				{#if stats.most_active_day}
					<p class="text-base text-white mt-1">
						{stats.most_active_day.day}
					</p>
					<p class="text-xs text-white/50">
						{stats.most_active_day.total_listening_string} on {stats.most_active_day.scrobbles} tracks
					</p>
				{:else}
					<p class="text-base text-white/50 mt-1">—</p>
					<p class="text-xs invisible">.</p>
				{/if}
			</div>
        </div>
        <!-- Overview row 3 (the highlight cards)-->
		<div>
			
		</div>
    </section>
    

    <!-- top tracks + artists -->
    <!-- top albums -->
    <!-- new discoveries -->
    <!-- listening clock -->
    <!-- recent tracks -->
</div>