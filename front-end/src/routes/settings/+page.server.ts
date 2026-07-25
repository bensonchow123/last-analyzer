import type { PageServerLoad } from './$types';
import type { ServiceId, ServicePanel } from '$lib/types/settings';

// Each service serves its own settings, so the page fans out to one endpoint per
// service. They may be on different machines, one being down must degrade to an
// offline card rather than failing the whole page.
const SERVICES: { id: ServiceId; name: string }[] = [
	{ id: 'vault', name: 'Scrobble vault' },
	{ id: 'llm', name: 'Last LLM service' }
];

export const load: PageServerLoad = async ({ fetch }) => {
	const panels = await Promise.all(
		SERVICES.map(async ({ id, name }): Promise<ServicePanel> => {
			// the internal proxy route owns the address and the token, no duplication here
			const response = await fetch(`/api/settings/${id}`);
			const body = await response.json().catch(() => null);
			if (!response.ok) {
				return { id, name, settings: null, error: body?.detail ?? `${response.status} ${response.statusText}` };
			}
			return { id, name, settings: body, error: null };
		})
	);

	return { panels };
};
