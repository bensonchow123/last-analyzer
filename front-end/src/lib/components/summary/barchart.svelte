<script lang="ts">
    // define `Bar` data type
    type Bar = { value: number; label: string; sublabel?: string; isPeak: boolean };
    
    // assign values to `bar` and `xLabels` from the incoming `$props()`
    let { bars, xLabels }: { bars: Bar[]; xLabels: [number, string][] } = $props();

    // calculates maximum Y values for the axis so it is nice
    function niceMax(v: number) {
        // Guard clause so it never negative
        if (v <= 0) return 1;
        
        // find magnitude, so if it is like 23, magnitude is 10
        const magnitude = Math.pow(10, Math.floor(Math.log10(v)));

        // cadidate steps, multiplies the nice numbers by magnitude to get realistic candidates for axis ceiling
        const steps = [1, 2, 2.5, 5, 10].map(s => s * magnitude);
        
        // pick the first step that fits
        for (const s of steps)
            if (Math.ceil(v / s) * s >= v && Math.ceil(v / s) <= 6) return Math.ceil(v / s) * s;

        // fail back, if nothing fit, round up to the next power of 10
        return Math.ceil(v / (magnitude * 10)) * magnitude * 10;
    }
    // maxVal accroess bars
    const maxVal = $derived(Math.max(...bars.map(b => b.value)));
    // get pretty ceiling with nice max
    const yMax = $derived(niceMax(maxVal));
    // 5 decorative tick marks
    const yTicks = $derived(Array.from({ length: 5 }, (_, i) => Math.round((yMax / 4) * i * 10) / 10));
</script>

<!--relative makes positional context and pl-7 make space for the y labels-->
<div class="relative pl-7">
    <!--Draw horizontal grid lines and Y axis numbers on the left for each tick-->
    <div class="absolute inset-x-0 top-0 bottom-5 pl-7 flex flex-col-reverse justify-between pointer-events-none">
        {#each yTicks as t}
            <div class="relative w-full">
                <span class="absolute -left-7 -translate-y-1/2 text-[10px] text-white/30 w-6 text-right leading-none">{t}</span>
                <div class="w-full border-t border-white/[0.06]"></div>
            </div>
        {/each}
    </div>
    <!--Renders the actual bars-->
    <div class="flex items-end gap-px h-32">
        {#each bars as bar}
            <div
                class="flex-1 rounded-t-sm {bar.isPeak ? 'bg-violet-400' : 'bg-violet-500/25'}"
                style="height: {bar.value > 0 ? (bar.value / yMax) * 100 : 2}%"
                title="{bar.label}{bar.sublabel ? `: ${bar.sublabel}` : ''}"
            ></div>
        {/each}
    </div>
    <!--Renders x axis labels-->
    <div class="relative h-5">
        {#each xLabels as [i, label]}
            <span
                class="absolute -translate-x-1/2 top-1 text-[10px] {bars[i]?.isPeak ? 'text-violet-400' : 'text-white/25'}"
                style="left: {((i + 0.5) / bars.length) * 100}%"
            >{label}</span>
        {/each}
    </div>
</div>