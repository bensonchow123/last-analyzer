<script lang="ts">
	let { data } = $props();
</script>

<h1>Summary</h1>
<pre>{JSON.stringify(data.summary)}</pre>
