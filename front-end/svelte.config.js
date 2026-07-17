import adapter from '@sveltejs/adapter-node';

/** @type {import('@sveltejs/kit').Config} */
const config = {
	kit: {
		env: {
			dir: '..'
		},
		adapter: adapter()
	}
};

export default config;
