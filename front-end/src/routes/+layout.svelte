<script lang="ts">
	import favicon from '$lib/assets/favicon.svg';
	import { page } from '$app/state';

	let { children } = $props();
	import "../app.css";

	// top panel tabs, the current path decides which one is lit
	const tabs = [
		{ href: '/', label: 'Chat' },
		{ href: '/summary', label: 'Summary' }
	];
</script>

<svelte:head>
	<link rel="icon" href={favicon} />
</svelte:head>

<div class="h-screen flex flex-col bg-[#0f0f0f]">
	<!-- top panel for switching between pages -->
	<nav class="shrink-0 flex items-center gap-2 px-4 py-2 border-b border-white/10">
		{#each tabs as tab}
			<a
				href={tab.href}
				class="
					px-4 py-1.5 rounded-lg text-sm
					{
						page.url.pathname === tab.href
						? 'bg-violet-500 text-white'
						: 'text-white/40 hover:text-white/50 bg-white/5 hover:bg-white/20'
					}
				"
			>
				{tab.label}
			</a>
		{/each}
	</nav>

	<!-- pages fill the rest, the chat fits it exactly and the summary scrolls -->
	<div class="flex-1 min-h-0 overflow-y-auto">
		{@render children()}
	</div>
</div>
