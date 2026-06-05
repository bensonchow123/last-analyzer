import { SCROBBLE_VAULT_IPV4, SCROBBLE_VAULT_PORT } from '$env/static/private';
import type { PageServerLoad } from './$types';

export const load: PageServerLoad = async ({ fetch }) => {
    const response = await fetch(`http://${SCROBBLE_VAULT_IPV4}:${SCROBBLE_VAULT_PORT}/music-summary`);
    if (!response.ok) {
        throw new Error(`music-summary failed: ${response.status} ${response.statusText}`);
    }
    const summary = await response.json();
    return { summary };
};