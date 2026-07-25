import { env } from '$env/dynamic/private';
import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';

// Server side proxy, same shape as /api/chat: the api address and the admin
// token are read at runtime and neither one ever reaches the browser, so
// LAST_LLM_API_BIND_IP can stay on localhost and the token stays server side.
const url = () =>
	`http://${env.LAST_LLM_API_IPV4 ?? '127.0.0.1'}:${env.LAST_LLM_API_PORT ?? '8002'}/settings`;

const headers = () => ({
	'content-type': 'application/json',
	authorization: `Bearer ${env.ADMIN_API_TOKEN ?? ''}`
});

async function forward(init: RequestInit) {
	try {
		const upstream = await fetch(url(), { headers: headers(), ...init });
		return json(await upstream.json(), { status: upstream.status });
	} catch {
		// the api may not be running on this machine, unreachable is a normal outcome
		return json({ detail: `Cannot reach the last llm api at ${url()}` }, { status: 502 });
	}
}

export const GET: RequestHandler = async () => forward({});

export const PATCH: RequestHandler = async ({ request }) =>
	forward({ method: 'PATCH', body: await request.text() });
