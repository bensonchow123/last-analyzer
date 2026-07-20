import { env } from '$env/dynamic/private';
import type { RequestHandler } from './$types';

// Server side proxy, mirrors the summary page pattern: the chat api address is
// read at runtime and never reaches the browser, so LAST_LLM_API_BIND_IP can
// stay on localhost and remote devices only need to reach the frontend.
export const POST: RequestHandler = async ({ request }) => {
	const host = env.LAST_LLM_API_IPV4 ?? '127.0.0.1';
	const port = env.LAST_LLM_API_PORT ?? '8002';
	const upstream = await fetch(`http://${host}:${port}/chat/stream`, {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: await request.text() // text body sidesteps undici's duplex requirement
	});
	return new Response(upstream.body, {
		status: upstream.status,
		headers: { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' }
	});
};
