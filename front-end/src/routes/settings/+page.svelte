<script lang="ts">
	import { untrack } from 'svelte';
	import type { PageData } from './$types';
	import type { ServicePanel } from '$lib/types/settings';
	import { isSecretValue } from '$lib/types/settings';

	let { data }: { data: PageData } = $props();

	// Seeded once on purpose. The component remounts when you navigate back here,
	// and after a save the service's own response replaces this copy.
	const loaded = untrack(() => data.panels);

	let panels = $state<ServicePanel[]>(loaded);
	let drafts = $state<Record<string, Record<string, string>>>(seedAll(loaded));
	let busy = $state<Record<string, boolean>>({});
	let status = $state<Record<string, string>>({});
	let failed = $state<Record<string, boolean>>({});

	function seed(panel: ServicePanel): Record<string, string> {
		const draft: Record<string, string> = {};
		if (!panel.settings) return draft;
		for (const field of panel.settings.fields) {
			const value = panel.settings.values[field.key];
			// secret boxes start empty, blank on save means leave it alone
			draft[field.key] = field.secret || value === null || value === undefined ? '' : String(value);
		}
		return draft;
	}

	function seedAll(list: ServicePanel[]): Record<string, Record<string, string>> {
		return Object.fromEntries(list.map((panel) => [panel.id, seed(panel)]));
	}

	async function patch(panel: ServicePanel, body: Record<string, string | null>) {
		busy[panel.id] = true;
		status[panel.id] = '';
		failed[panel.id] = false;
		try {
			const response = await fetch(`/api/settings/${panel.id}`, {
				method: 'PATCH',
				headers: { 'content-type': 'application/json' },
				body: JSON.stringify(body)
			});
			const result = await response.json();
			if (!response.ok) {
				failed[panel.id] = true;
				status[panel.id] = result?.detail ?? `Save failed with ${response.status}`;
				return;
			}
			// show what the service actually stored, secrets come back masked
			panel.settings = result;
			drafts[panel.id] = seed(panel);
			status[panel.id] = 'Saved, live on the next request';
		} catch (error) {
			failed[panel.id] = true;
			status[panel.id] = error instanceof Error ? error.message : 'Save failed';
		} finally {
			busy[panel.id] = false;
		}
	}

	function save(panel: ServicePanel) {
		if (!panel.settings) return;
		const draft = drafts[panel.id];
		const body: Record<string, string | null> = {};
		for (const field of panel.settings.fields) {
			const value = draft[field.key] ?? '';
			if (field.secret && value.trim() === '') continue; // untouched, not cleared
			body[field.key] = value;
		}
		patch(panel, body);
	}

	// null drops the override so the value falls back to .env again
	const revert = (panel: ServicePanel, key: string) => patch(panel, { [key]: null });

	// Anything a reachable service says is still unset, so a fresh install gets
	// told what to fill in rather than silently doing nothing
	const setupNeeded = $derived(
		panels.filter((panel) => (panel.settings?.missing?.length ?? 0) > 0)
	);

	// A reachable service with no admin token. Fine on localhost, not once a
	// *_BIND_IP points somewhere else, so say so rather than staying quiet.
	const unprotected = $derived(
		panels.filter((panel) => panel.settings && !panel.settings.auth_required)
	);

	function labelFor(panel: ServicePanel, key: string): string {
		return panel.settings?.fields.find((field) => field.key === key)?.label ?? key;
	}
</script>

<svelte:head><title>Settings</title></svelte:head>

<div class="mx-auto max-w-3xl px-4 py-6">
	<h1 class="text-white text-xl">Settings</h1>
	<p class="mt-1 text-sm text-white/50">
		Changes apply straight away, nothing needs restarting.
	</p>
	<p class="mt-1 text-xs text-white/30">
		Ports, addresses and database passwords are not here. Those are read once when a service
		starts, so they only change in a config file, which a single machine setup never needs.
	</p>

	{#if setupNeeded.length}
		<div class="mt-4 rounded-lg border border-violet-500 bg-violet-500/10 p-4">
			<h2 class="text-white">Finish setting up</h2>
			<p class="mt-1 text-sm text-white/70">
				Nothing will sync or answer until these are filled in below.
			</p>
			<ul class="mt-2 space-y-1 text-sm text-white/50">
				{#each setupNeeded as panel (panel.id)}
					<li>
						<span class="text-white/70">{panel.name}:</span>
						{panel.settings?.missing.map((key) => labelFor(panel, key)).join(', ')}
					</li>
				{/each}
			</ul>
		</div>
	{/if}

	{#if unprotected.length}
		<div class="mt-4 rounded-lg border border-white/10 bg-black/30 p-3">
			<p class="text-sm text-white/50">
				No admin token set on {unprotected.map((panel) => panel.name).join(' and ')}, so anyone who
				can reach {unprotected.length > 1 ? 'them' : 'it'} can change these settings. Fine while everything
				stays on <code class="text-white/70">127.0.0.1</code>. Set
				<code class="text-white/70">ADMIN_API_TOKEN</code> in
				<code class="text-white/70">.env</code> before pointing any
				<code class="text-white/70">*_BIND_IP</code> at a VPN address.
			</p>
		</div>
	{/if}

	{#each panels as panel (panel.id)}
		<section class="mt-6 rounded-lg border border-violet-500/50 bg-[#131311] p-4">
			<div class="flex items-baseline gap-2">
				<h2 class="text-white">{panel.name}</h2>
				{#if panel.settings}
					<span class="font-mono text-xs text-white/30">{panel.settings.version}</span>
				{/if}
			</div>

			{#if panel.error || !panel.settings}
				<p class="mt-2 text-sm text-white/50">
					Offline or not configured. This is fine when the service runs on another machine that is
					not up.
				</p>
				<p class="mt-2 rounded border border-white/10 bg-black/30 p-2 font-mono text-xs text-white/40">
					{panel.error ?? 'no settings returned'}
				</p>
			{:else}
				{#each panel.settings.fields as field (field.key)}
					{@const source = panel.settings.sources[field.key]}
					{@const value = panel.settings.values[field.key]}
					<div class="mt-4 border-t border-white/10 pt-4">
						<div class="flex items-center gap-2">
							<label class="text-sm text-white/70" for="{panel.id}-{field.key}">{field.label}</label>
							{#if panel.settings.missing.includes(field.key)}
								<!-- unset, so there is no source worth naming -->
								<span class="rounded bg-violet-500 px-1.5 py-0.5 text-[11px] text-white">needed</span>
							{:else if source === 'settings'}
								<span class="rounded bg-violet-500/20 px-1.5 py-0.5 text-[11px] text-violet-400">
									saved here
								</span>
								<button
									class="text-[11px] text-white/30 underline hover:text-white/50"
									onclick={() => revert(panel, field.key)}
									disabled={busy[panel.id]}
								>
									reset
								</button>
							{:else}
								<!-- could be a config file or the built in default, and a fresh
								     install has neither, so do not name one -->
								<span class="rounded bg-white/5 px-1.5 py-0.5 text-[11px] text-white/30">
									default
								</span>
							{/if}
						</div>

						{#if field.help}
							<p class="mt-1 text-xs text-white/40">{field.help}</p>
						{/if}

						{#if field.type === 'bool'}
							<input
								id="{panel.id}-{field.key}"
								type="checkbox"
								class="mt-2 h-4 w-4 accent-violet-500"
								checked={drafts[panel.id][field.key] === 'true'}
								onchange={(e) =>
									(drafts[panel.id][field.key] = e.currentTarget.checked ? 'true' : 'false')}
							/>
						{:else}
							<input
								id="{panel.id}-{field.key}"
								type={field.secret ? 'password' : field.type === 'int' ? 'number' : 'text'}
								min={field.min}
								max={field.max}
								placeholder={field.secret && isSecretValue(value)
									? value.set
										? `saved, ends ${value.preview}`
										: 'not set'
									: ''}
								value={drafts[panel.id][field.key]}
								oninput={(e) => (drafts[panel.id][field.key] = e.currentTarget.value)}
								class="mt-2 w-full rounded border border-white/10 bg-black/30 px-2 py-1.5 text-sm
									text-white placeholder:text-white/30 focus:border-violet-500 focus:outline-none"
							/>
						{/if}
					</div>
				{/each}

				<div class="mt-5 flex items-center gap-3">
					<button
						class="rounded-lg bg-violet-500 px-4 py-1.5 text-sm text-white
							hover:bg-violet-400 disabled:opacity-40"
						onclick={() => save(panel)}
						disabled={busy[panel.id]}
					>
						{busy[panel.id] ? 'Saving' : 'Save'}
					</button>
					{#if status[panel.id]}
						<span class="text-xs {failed[panel.id] ? 'text-red-400' : 'text-white/50'}">
							{status[panel.id]}
						</span>
					{/if}
				</div>
			{/if}
		</section>
	{/each}
</div>
