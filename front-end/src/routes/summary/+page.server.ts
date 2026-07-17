import { env } from '$env/dynamic/private';
import type { PageServerLoad } from './$types';
import type { MusicSummary } from '$lib/types/summary';

export const load: PageServerLoad = async ({ fetch }) => {
    // $env/dynamic (not static) so the vault address is read at runtime,
    // letting Docker inject it instead of baking it into the build.
    const response = await fetch(`http://${env.SCROBBLE_VAULT_IPV4}:${env.SCROBBLE_VAULT_PORT}/music-summary`);
    if (!response.ok) {
        throw new Error(`music-summary failed: ${response.status} ${response.statusText}`);
    }
    const summary: MusicSummary = await response.json();
    return { summary };
};