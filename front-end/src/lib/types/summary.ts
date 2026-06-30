type ImageFields = {
    artist_image_extralarge: string | null;
    artist_image_large: string | null;
    artist_image_medium: string | null;
    artist_image_small: string | null;
};

type AlbumImageFields = {
    album_image_extralarge: string | null;
    album_image_large: string | null;
    album_image_medium: string | null;
    album_image_small: string | null;
};

export type RecentTrack = ImageFields & AlbumImageFields & {
    listened_at: number;
    artist_name: string;
    track_name: string;
    album_name: string | null;
    duration_ms: number;
    duration_seconds: number;
    duration_string: string;
};

export type TopArtist = ImageFields & {
    artist_name: string;
    plays: number;
    first_listened_at: number;
};

export type TopAlbum = AlbumImageFields & {
    artist_name: string;
    album_name: string;
    plays: number;
    first_listened_at: number;
};

export type TopTrack = ImageFields & AlbumImageFields & {
    artist_name: string;
    track_name: string;
    plays: number;
    first_listened_at: number;
    album_name: string | null;
    duration_ms: number;
    duration_seconds: number;
    duration_string: string;
};

type HourSlot = {
    hour: number;
    average_scrobbles: number;
    average_listening_seconds: number;
    average_listening_string?: string;
};

type WeekdaySlot = {
    weekday_index: number;
    weekday: string;
    average_scrobbles: number;
    average_listening_seconds: number;
    average_listening_string: string;
};

type NewInTimeframe = {
    artists_count: number;
    artists: (TopArtist & { first_listened_at: number })[];
    albums_count: number;
    albums: (TopAlbum & { first_listened_at: number })[];
    tracks_count: number;
    tracks: (TopTrack & { first_listened_at: number })[];
};

type PeriodStats = {
    total_scrobbles: number;
    active_days: number;
    first_listened_at: number | null;
    last_listened_at: number | null;
    unique_artists_count: number;
    unique_tracks_count: number;
    unique_albums_count: number;
    listening_time: {
        total_seconds: number;
        total_string: string;
        missing_duration_count: number;
    };
    listening_clock: {
        peak_hour: HourSlot | null;
        hours: HourSlot[];
    };
    listening_weekday: {
        peak_day: WeekdaySlot;
        days: WeekdaySlot[];
    };
    most_active_day: {
        day: string;
        scrobbles: number;
        total_listening_seconds: number;
        total_listening_string: string;
    } | null;
    top_artists: TopArtist[];
    top_albums: TopAlbum[];
    top_tracks: TopTrack[];
    recent_tracks: RecentTrack[];
    new_in_timeframe: NewInTimeframe | null;
};

export type Period = {
    period: string;
    label: string;
    stats: PeriodStats;
};

export type MusicSummary = {
    generated_at: number;
    last_synced_at: number | null;
    periods: Period[];
};
