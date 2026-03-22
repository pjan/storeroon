"""
storeroon.reports.models — data models for all Sprint 2 reports.

Every query function returns one of these dataclasses. Renderers accept
them as input. This module is the single source of truth for the shape of
report data flowing between the query and renderer layers.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# =========================================================================
# Shared / common
# =========================================================================


@dataclass(frozen=True, slots=True)
class BucketCount:
    """A single bucket in a distribution histogram."""

    label: str
    count: int
    percentage: float  # 0.0–100.0



@dataclass(frozen=True, slots=True)
class AliasUsageRow:
    """Consistency check for a canonical/alias tag pair.

    For files that have the alias key set, checks whether the canonical
    key is also present with the same value.  A healthy collection has
    consistency_pct at 100%.
    """

    canonical_key: str
    alias_key: str
    consistency_pct: (
        float  # % of files with alias that also have canonical with same value
    )


@dataclass(frozen=True, slots=True)
class TagInventoryRow:
    """One row in the full tag key inventory."""

    tag_key_upper: str
    file_count: int
    coverage_pct: float
    classification: str  # required / recommended / other / alias / standard_optional / strip / unknown


@dataclass(frozen=True, slots=True)
class KeyInventoryFullData:
    """Complete data for the key inventory report."""

    total_files: int
    inventory: list[TagInventoryRow]


# =========================================================================
# Collection overview
# =========================================================================


@dataclass(frozen=True, slots=True)
class OverviewTotals:
    """Top-level collection totals."""

    total_album_artists: int  # distinct ALBUMARTIST tag values
    total_albums: int  # distinct album folders
    total_tracks: int
    total_duration_seconds: float
    total_size_bytes: int




@dataclass(frozen=True, slots=True)
class OverviewSummaryData:
    """Summary data for the overview in the summary command."""

    totals: OverviewTotals


# =========================================================================
# Collection overview (with scan issues)
# =========================================================================


@dataclass(frozen=True, slots=True)
class AlbumBreakdown:
    """Album folder with issue counts and health score."""

    album_dir: str
    display_name: str  # "{YYYY} - {album} [{catalognumber}]"
    track_count: int
    total_size_bytes: int
    total_duration_seconds: float
    health_score: int  # 0-100, based on per-track issue severity
    critical_count: int
    error_count: int
    warning_count: int
    info_count: int


@dataclass(frozen=True, slots=True)
class ReleaseTypeBreakdown:
    """Release type with aggregated issue counts."""

    release_type: str
    album_count: int
    track_count: int
    total_size_bytes: int
    total_duration_seconds: float
    critical_count: int
    error_count: int
    warning_count: int
    info_count: int
    albums: list[AlbumBreakdown]


@dataclass(frozen=True, slots=True)
class ArtistBreakdown:
    """Artist with aggregated issue counts."""

    artist: str
    album_count: int
    track_count: int
    total_size_bytes: int
    total_duration_seconds: float
    critical_count: int
    error_count: int
    warning_count: int
    info_count: int
    release_types: list[ReleaseTypeBreakdown]


@dataclass(frozen=True, slots=True)
class IssuesTotals:
    """Aggregate issue counts for the dashboard."""

    albums_with_issues: int
    files_with_issues: int
    total_issues: int


@dataclass(frozen=True, slots=True)
class OverviewFullData:
    """Complete data for collection overview 2."""

    totals: OverviewTotals
    issues_totals: IssuesTotals
    by_artist: list[ArtistBreakdown]


# =========================================================================
# Album consistency (used by album_consistency query helpers)
# =========================================================================


@dataclass(frozen=True, slots=True)
class FieldConsistencyViolation:
    """A field that has inconsistent values across tracks in an album."""

    album_dir: str
    field_name: str
    distinct_values: list[str]
    track_counts_per_value: dict[str, int]
    null_track_count: int


@dataclass(frozen=True, slots=True)
class TrackNumberingViolation:
    """A track numbering issue within an album."""

    album_dir: str
    check_type: str  # missing_track, missing_disc, duplicate_track, totaltracks_mismatch, exceeds_total
    description: str


# =========================================================================
# Scan issues
# =========================================================================




@dataclass(frozen=True, slots=True)
class FileIssueDetail:
    """A single issue for a specific file within an album."""

    file_path: str
    file_name: str
    issue_type: str
    severity: str
    description: str
    details: str | None  # JSON string


@dataclass(frozen=True, slots=True)
class AlbumIssuesDetail:
    """Complete issue details for a single album."""

    artist: str
    album: str
    catalog_number: str | None
    album_dir: str
    total_files: int
    files_with_issues: int
    error_count: int
    warning_count: int
    info_count: int
    issues: list[
        FileIssueDetail
    ]  # All issues for this album, sorted by severity then file


@dataclass(frozen=True, slots=True)
class TrackIssue:
    """A single issue on a track, with bucket classification."""

    issue_type: str
    severity: str  # critical, error, warning, info
    description: str
    field: str | None  # tag field name if applicable
    bucket: str  # "blocker", "metadata", "optimization"


@dataclass(frozen=True, slots=True)
class TrackDetail:
    """A single track with its issues."""

    file_id: int
    file_path: str
    file_name: str
    discnumber: int
    tracknumber: int
    title: str
    issues: list[TrackIssue]


@dataclass(frozen=True, slots=True)
class AlbumLevelIssue:
    """An album-level issue (not tied to a specific track)."""

    severity: str  # critical, error, warning, info
    description: str


@dataclass(frozen=True, slots=True)
class AlbumReportData:
    """Complete data for the album detail report page."""

    artist: str
    album: str
    original_date: str | None
    catalog_number: str | None
    album_dir: str
    total_tracks: int
    health_score: int  # 0-100
    critical_count: int
    error_count: int
    warning_count: int
    info_count: int
    bit_depth: int | None  # highest bits_per_sample across tracks
    sample_rate_hz: int | None  # highest sample_rate_hz across tracks
    channels: int | None  # highest channel count across tracks
    album_level_issues: list[AlbumLevelIssue]
    tracks: list[TrackDetail]


# =========================================================================
# Collection Issues Overview
# =========================================================================


@dataclass(frozen=True, slots=True)
class AlbumHealthBar:
    """Bar showing % of albums without a specific issue type."""

    issue_label: str
    albums_affected: int
    total_albums: int
    clean_pct: float  # % without this issue


@dataclass(frozen=True, slots=True)
class TrackHealthBar:
    """Bar showing % of files without a specific non-tag issue."""

    issue_label: str
    files_affected: int
    total_files: int
    clean_pct: float


@dataclass(frozen=True, slots=True)
class TagBar:
    """Stacked bar showing valid/invalid/misencoded/missing breakdown for a tag."""

    tag_key: str
    valid_count: int
    valid_pct: float
    invalid_count: int  # 0 if no validator for this tag
    invalid_pct: float
    misencoded_count: int  # 0 if no encoding issues
    misencoded_pct: float
    missing_count: int
    missing_pct: float


@dataclass(frozen=True, slots=True)
class CollectionIssuesFullData:
    """Complete data for the collection issues overview report."""

    total_albums: int
    total_files: int
    album_health: list[AlbumHealthBar]
    track_health: list[TrackHealthBar]
    required_tags: list[TagBar]
    recommended_tags: list[TagBar]
    other_tags: list[TagBar]
    alias_consistency: list[AliasUsageRow]


# =========================================================================
# Artist name consistency
# =========================================================================


@dataclass(frozen=True, slots=True)
class ArtistValueRow:
    """A distinct ALBUMARTIST or ARTIST value with counts."""

    value: str
    track_count: int
    album_count: int


@dataclass(frozen=True, slots=True)
class FuzzyPairRow:
    """A pair of similar values found by fuzzy matching."""

    name_a: str
    name_b: str
    similarity: float
    count_a: int
    count_b: int


@dataclass(frozen=True, slots=True)
class CaseVariantGroup:
    """A group of values that differ only in case/whitespace."""

    normalised: str
    variants: list[str]
    total_track_count: int


@dataclass(frozen=True, slots=True)
class ArtistsFullData:
    """Complete data for the artist consistency report."""

    albumartist_values: list[ArtistValueRow]
    albumartist_case_variants: list[CaseVariantGroup]
    albumartist_fuzzy_pairs: list[FuzzyPairRow]
    artist_values: list[ArtistValueRow]
    artist_case_variants: list[CaseVariantGroup]
    artist_fuzzy_pairs: list[FuzzyPairRow]


@dataclass(frozen=True, slots=True)
class ArtistsSummaryData:
    """Summary data for artist consistency in the summary command."""

    distinct_albumartist_count: int
    albumartist_case_variant_count: int
    albumartist_fuzzy_pair_count: int
    distinct_artist_count: int


# =========================================================================
# Genre analysis
# =========================================================================


@dataclass(frozen=True, slots=True)
class GenreValueRow:
    """A distinct GENRE value with counts."""

    value: str
    file_count: int
    file_pct: float


@dataclass(frozen=True, slots=True)
class GenreMissingByArtist:
    """An artist with missing genre tags."""

    artist: str
    missing_count: int


@dataclass(frozen=True, slots=True)
class MultiGenreCombo:
    """A distinct combination of multiple GENRE values on a file."""

    values: list[str]
    file_count: int


@dataclass(frozen=True, slots=True)
class GenresFullData:
    """Complete data for the genre analysis report."""

    total_files: int
    genre_values: list[GenreValueRow]
    fuzzy_pairs: list[FuzzyPairRow]
    no_genre_count: int
    no_genre_pct: float
    no_genre_by_artist: list[GenreMissingByArtist]
    multi_genre_count: int
    multi_genre_combos: list[MultiGenreCombo]


@dataclass(frozen=True, slots=True)
class GenresSummaryData:
    """Summary data for genres in the summary command."""

    distinct_genre_count: int
    fuzzy_pair_count: int
    no_genre_pct: float


# =========================================================================
# Lyrics coverage
# =========================================================================


@dataclass(frozen=True, slots=True)
class LyricsCoverageOverall:
    """Overall lyrics coverage stats."""

    with_lyrics_count: int
    with_lyrics_pct: float
    empty_lyrics_count: int
    empty_lyrics_pct: float
    no_lyrics_count: int
    no_lyrics_pct: float
    lyrics_key_count: int  # files using LYRICS
    unsyncedlyrics_key_count: int  # files using UNSYNCEDLYRICS


@dataclass(frozen=True, slots=True)
class LyricsCoverageByEntity:
    """Lyrics coverage for a single artist or album."""

    name: str
    with_lyrics: int
    total: int
    coverage_pct: float


@dataclass(frozen=True, slots=True)
class LyricsFullData:
    """Complete data for the lyrics coverage report."""

    total_files: int
    overall: LyricsCoverageOverall
    by_artist: list[LyricsCoverageByEntity]
    by_album: list[LyricsCoverageByEntity]


@dataclass(frozen=True, slots=True)
class LyricsSummaryData:
    """Summary data for lyrics in the summary command."""

    coverage_pct: float
    artists_with_zero_coverage: int


# =========================================================================
# ReplayGain coverage
# =========================================================================


@dataclass(frozen=True, slots=True)
class ReplayGainCoverageRow:
    """Coverage for a single ReplayGain tag."""

    tag_key: str
    valid_count: int
    valid_pct: float
    malformed_count: int
    malformed_pct: float
    absent_count: int
    absent_pct: float


@dataclass(frozen=True, slots=True)
class PartialRgAlbum:
    """An album with partial ReplayGain tagging."""

    album_dir: str
    tracks_with_rg: int
    tracks_without_rg: int
    total_tracks: int


@dataclass(frozen=True, slots=True)
class RgOutlier:
    """A track with an outlier ReplayGain value."""

    path: str
    tag_key: str
    value: str
    parsed_db: float


@dataclass(frozen=True, slots=True)
class ReplayGainFullData:
    """Complete data for the ReplayGain report."""

    total_files: int
    coverage: list[ReplayGainCoverageRow]
    partial_albums: list[PartialRgAlbum]
    gain_distribution: list[BucketCount]
    outliers: list[RgOutlier]


@dataclass(frozen=True, slots=True)
class ReplayGainSummaryData:
    """Summary data for ReplayGain in the summary command."""

    track_coverage_pct: float
    album_coverage_pct: float
    partial_album_count: int
    outlier_count: int


# =========================================================================
# Master summary (aggregation of all report summaries)
# =========================================================================


@dataclass(slots=True)
class MasterSummary:
    """All summary-mode data rolled into one object for ``report summary``."""

    overview: OverviewSummaryData | None = None
    artists: ArtistsSummaryData | None = None
    genres: GenresSummaryData | None = None
    lyrics: LyricsSummaryData | None = None
    replaygain: ReplayGainSummaryData | None = None
