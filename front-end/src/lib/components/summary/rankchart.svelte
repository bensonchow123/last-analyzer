<script lang="ts">
    type Row = {
        name: string;
        subname?: string;
        image?: string | null;
        plays: number;
        sublabel?: string;
    };

    let { rows }: { rows: Row[] } = $props();

    const maxPlays = $derived(rows[0]?.plays ?? 1);
</script>

{#each rows as row, i}
    <!-- The bar is an absolute positioned element to take up no space, relative make it anchored to this div instead -->
    <!-- Item center makes all items except the bar centered vertically-->
    <div class="relative flex items-center gap-3 py-3">
        <!--The ranking numbers, w-4 makes the double digit numbers rows not inconsistent-->
        <span class="text-xs w-4 text-right shrink-0 {i < 3 ? 'text-violet-400' : 'text-white/30'}">
            {i + 1}
        </span>

        <!-- The image -->
        {#if row.image}
            <!--The classes set perfered size, round corners, fill perfer size then prevent flex from squishing it-->
            <img
                src={row.image} alt={row.name}
                referrerpolicy="no-referrer"
                class="w-9 h-9 rounded-md object-cover shrink-0"
            />
        {:else}
            <!--If no image display the first letter of the name-->
            <!--Same as the image, but instead of img, create another flex container and center in both axis, in it display the text -->
            <div class="w-9 h-9 rounded-md bg-violet-500/10 shrink-0 flex items-center justify-center text-violet-400 text-sm font-medium">
                {row.name[0].toUpperCase()}
            </div>
        {/if}

        <!-- Name, truncate if too long, take up all remaining space, min-w-0 prevent overflow and prevent the play count off screen-->
        <div class="min-w-0 flex-1">
            <p class="text-sm text-white truncate">{row.name}</p>
            {#if row.subname || row.sublabel}
                <p class="text-xs text-white/40 truncate">
                    {[row.subname, row.sublabel].filter(Boolean).join(' · ')}
                </p>
            {/if}
        </div>

        <!-- Play count-->
        <span class="text-xs text-white/30 shrink-0">{row.plays.toLocaleString()} plays</span>

        <!-- Play count comparison bar, pin to very bottom edge of parent, set to 1px height and use background colour to show unfilled portion. -->
        <div class="absolute bottom-0 left-0 right-0 h-px bg-white/5">
            <!-- Filled portion, fill entiere 1 px, and round the bar corners -->
            <div class="h-full bg-violet-500 rounded-full" style="width: {(row.plays / maxPlays) * 100}%"></div>
        </div>
    </div>
{/each}
