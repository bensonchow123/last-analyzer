<script lang="ts">
	import { timeAgo } from '$lib/utils';

	let { lastSyncedAt }: { lastSyncedAt: number | null } = $props();

	// derived detects if `lastSyncedAt` is set and recomputes
	const lastSynced = $derived(lastSyncedAt ? timeAgo(lastSyncedAt) : null);
</script>

<!-- header, shows last syned -->
<section>
	<h1 class="text-2xl text-white">Your Music Summary</h1>
	{#if lastSynced}
		<p class="text-sm text-white/50">Last synced {lastSynced}</p>
	{:else}
		<p class="text-sm text-white/50">Not synced to Last.fm</p>
	{/if}
</section>
