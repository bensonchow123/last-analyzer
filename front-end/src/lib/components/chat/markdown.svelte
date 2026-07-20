<script lang="ts">
	import { marked } from 'marked';
	import DOMPurify from 'dompurify';

	let { text }: { text: string } = $props();

	// sanitize cause this goes through {@html} and model output is untrusted
	const html = $derived(DOMPurify.sanitize(marked.parse(text, { async: false }) as string));
</script>

<div class="markdown">{@html html}</div>

<style>
	/* tailwind preflight flattens element styles, restore the ones models emit */
	.markdown :global(p) { margin: 0.5rem 0; }
	.markdown :global(p:first-child) { margin-top: 0; }
	.markdown :global(p:last-child) { margin-bottom: 0; }
	.markdown :global(ul) { list-style: disc; margin: 0.5rem 0; padding-left: 1.25rem; }
	.markdown :global(ol) { list-style: decimal; margin: 0.5rem 0; padding-left: 1.25rem; }
	.markdown :global(h1), .markdown :global(h2), .markdown :global(h3) { font-weight: 600; margin: 0.75rem 0 0.25rem; }
	.markdown :global(h1) { font-size: 1.125rem; }
	.markdown :global(h2) { font-size: 1rem; }
	.markdown :global(code) { background: rgb(255 255 255 / 0.1); border-radius: 0.25rem; padding: 0.1rem 0.3rem; font-size: 0.85em; }
	.markdown :global(pre) { background: rgb(255 255 255 / 0.1); border-radius: 0.5rem; padding: 0.75rem; overflow-x: auto; margin: 0.5rem 0; }
	.markdown :global(pre code) { background: none; padding: 0; }
	.markdown :global(table) { border-collapse: collapse; margin: 0.5rem 0; }
	.markdown :global(th), .markdown :global(td) { border: 1px solid rgb(255 255 255 / 0.2); padding: 0.25rem 0.6rem; }
	.markdown :global(th) { background: rgb(255 255 255 / 0.05); }
	.markdown :global(a) { color: #a78bfa; text-decoration: underline; }
	.markdown :global(blockquote) { border-left: 2px solid rgb(139 92 246 / 0.5); padding-left: 0.75rem; color: rgb(255 255 255 / 0.7); margin: 0.5rem 0; }
	.markdown :global(strong) { font-weight: 600; }
</style>
