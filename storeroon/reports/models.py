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
class TagCoverageRow:
    """Coverage stats for a single tag key within a group."""

    tag_key: str
    present_count: int
    present_pct: float
    missing_count: int  # empty + absent combined
    missing_pct: float


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
class FieldValidationRow:
    """Validation summary for a single field in the tag-formats report."""

    field_name: str
    valid_count: int
    valid_pct: float
    invalid_count: int
    invalid_pct: float
    absent_count: int
    absent_pct: float


@dataclass(frozen=True, slots=True)
class InvalidValueRow:
    """A distinct invalid value found during format validation."""

    value: str
    count: int


# =========================================================================
# Report 1 — Collection overview
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
class AlbumBreakdown:
    """A single album folder in the collection."""

    album_dir: str  # folder path (unique identifier)
    display_name: str  # "{originaldate} - {album} [{catalognumber}]"
    track_count: int
    total_size_bytes: int
    total_duration_seconds: float


@dataclass(frozen=True, slots=True)
class ReleaseTypeBreakdown:
    """A release type (album, ep, single, etc.) within an artist."""

    release_type: str  # RELEASETYPE tag value, or "unknown"
    album_count: int
    track_count: int
    total_size_bytes: int
    total_duration_seconds: float
    albums: list[AlbumBreakdown]


@dataclass(frozen=True, slots=True)
class ArtistBreakdown:
    """Top-level: breakdown per album artist."""

    artist: str  # ALBUMARTIST tag value
    album_count: int
    track_count: int
    total_size_bytes: int
    total_duration_seconds: float
    release_types: list[ReleaseTypeBreakdown]


@dataclass(frozen=True, slots=True)
class OverviewFullData:
    """Complete data for the collection overview report."""

    totals: OverviewTotals
    by_artist: list[ArtistBreakdown]


@dataclass(frozen=True, slots=True)
class OverviewSummaryData:
    """Summary data for the overview in the summary command."""

    totals: OverviewTotals


# =========================================================================
# Report 1b — Collection overview 2 (with scan issues)
# =========================================================================


@dataclass(frozen=True, slots=True)
class AlbumBreakdown2:
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
class ReleaseTypeBreakdown2:
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
    albums: list[AlbumBreakdown2]


@dataclass(frozen=True, slots=True)
class ArtistBreakdown2:
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
    release_types: list[ReleaseTypeBreakdown2]


@dataclass(frozen=True, slots=True)
class IssuesTotals:
    """Aggregate issue counts for the dashboard."""

    albums_with_issues: int
    files_with_issues: int
    total_issues: int


@dataclass(frozen=True, slots=True)
class Overview2FullData:
    """Complete data for collection overview 2."""

    totals: OverviewTotals
    issues_totals: IssuesTotals
    by_artist: list[ArtistBreakdown2]


# =========================================================================
# Report 2 — Audio technical quality
# =========================================================================


@dataclass(frozen=True, slots=True)
class DurationOutlier:
    """A track with unusually short or long duration."""

    path: str
    duration_seconds: float
    albumartist: str
    album: str
    title: str
    outlier_type: str  # "short" or "long"


@dataclass(frozen=True, slots=True)
class VendorInfo:
    """A distinct vendor_string with count and suspicion flag."""

    vendor_string: str
    count: int
    is_suspicious: bool


@dataclass(frozen=True, slots=True)
class MissingMd5Album:
    """An album with files missing audio_md5."""

    albumartist: str
    album: str
    album_dir: str
    missing_count: int
    total_count: int


@dataclass(frozen=True, slots=True)
class TechnicalFullData:
    """Complete data for the technical quality report."""

    total_files: int
    sample_rate_distribution: list[BucketCount]
    bit_depth_distribution: list[BucketCount]
    channel_distribution: list[BucketCount]
    bitrate_distribution: list[BucketCount]
    file_size_distribution: list[BucketCount]
    duration_distribution: list[BucketCount]
    duration_outliers: list[DurationOutlier]
    vendors: list[VendorInfo]
    missing_md5_count: int
    missing_md5_pct: float
    missing_md5_albums: list[MissingMd5Album]


@dataclass(frozen=True, slots=True)
class TechnicalSummaryData:
    """Summary data for technical quality in the summary command."""

    total_files: int
    sample_rate_distribution: list[BucketCount]
    bit_depth_distribution: list[BucketCount]
    bitrate_distribution: list[BucketCount]
    duration_outlier_count: int
    suspicious_vendor_count: int
    missing_md5_count: int
    missing_md5_pct: float


# =========================================================================
# Report 3 — Tag coverage and key inventory
# =========================================================================


@dataclass(frozen=True, slots=True)
class TagCoverageFullData:
    """Complete data for the tag coverage report."""

    total_files: int
    required_coverage: list[TagCoverageRow]
    recommended_coverage: list[TagCoverageRow]
    other_coverage: list[TagCoverageRow]
    alias_usage: list[AliasUsageRow]
    full_inventory: list[TagInventoryRow]
    unknown_keys: list[TagInventoryRow]


@dataclass(frozen=True, slots=True)
class TagCoverageSummaryData:
    """Summary data for tag coverage in the summary command."""

    total_files: int
    required_with_missing: list[TagCoverageRow]  # required tags with any missing
    recommended_high_missing: list[TagCoverageRow]  # recommended with >20% missing
    unknown_key_count: int
    top_unknown_keys: list[TagInventoryRow]  # top 5 unknown by file count


# =========================================================================
# Report 5 — Tag quality and integrity
# =========================================================================


@dataclass(frozen=True, slots=True)
class FieldFormatSection:
    """Validation results for a single field with a format validator."""

    field_name: str
    summary: FieldValidationRow
    invalid_values: list[InvalidValueRow]
    invalid_values_total: int  # total count if capped at 20


@dataclass(frozen=True, slots=True)
class DateQualityRow:
    """Date format quality for a single date field (DATE or ORIGINALDATE)."""

    field_name: str
    full_date_count: int
    year_only_count: int
    invalid_count: int
    missing_count: int


# =========================================================================
# Report 6 — Intra-album consistency
# =========================================================================


@dataclass(frozen=True, slots=True)
class FieldConsistencyViolation:
    """A single (album_dir, field) pair where inconsistency was found."""

    album_dir: str
    field_name: str
    distinct_values: list[str]
    track_counts_per_value: dict[str, int]  # value → count
    null_track_count: int  # tracks where the field is absent


@dataclass(frozen=True, slots=True)
class TrackNumberingViolation:
    """A single track numbering problem in an album directory."""

    album_dir: str
    check_type: str  # e.g. "missing_track", "duplicate_track", "gap", "exceeds_total", "totaltracks_mismatch", "disc_gap"
    description: str


@dataclass(frozen=True, slots=True)
class ConsistencyViolationSummary:
    """Count of albums affected by each check type."""

    check_type: str
    album_count: int


@dataclass(frozen=True, slots=True)
class AlbumConsistencyFullData:
    """Complete data for the album consistency report."""

    total_albums: int
    albums_with_violations: int
    field_violations: list[FieldConsistencyViolation]
    numbering_violations: list[TrackNumberingViolation]
    summary_by_type: list[ConsistencyViolationSummary]


@dataclass(frozen=True, slots=True)
class AlbumConsistencySummaryData:
    """Summary data for album consistency in the summary command."""

    total_albums: int
    albums_with_violations: int
    top_violation_types: list[ConsistencyViolationSummary]


@dataclass(frozen=True, slots=True)
class IdCoverageRow:
    """Coverage for a single ID tag key."""

    tag_key: str
    valid_count: int
    valid_pct: float
    malformed_count: int
    malformed_pct: float
    absent_count: int
    absent_pct: float


@dataclass(frozen=True, slots=True)
class PartialAlbumCoverage:
    """An album where an ID tag is present on some tracks but not all."""

    album_dir: str
    tracks_with_id: int
    tracks_without_id: int
    total_tracks: int


@dataclass(frozen=True, slots=True)
class DuplicateIdEntry:
    """A duplicate ID value across multiple files."""

    id_value: str
    file_count: int
    file_paths: list[str]
    same_directory: bool


@dataclass(frozen=True, slots=True)
class BackfillCandidate:
    """Stats for quick-win backfill."""

    description: str
    affected_tracks: int
    distinct_source_ids: int  # unique album IDs / release IDs = API call count


@dataclass(frozen=True, slots=True)
class IdSectionData:
    """Data for one ID source section (MusicBrainz or Discogs)."""

    source_name: str  # "MusicBrainz" or "Discogs"
    coverage: list[IdCoverageRow]
    partial_albums: list[PartialAlbumCoverage]
    duplicate_ids: list[DuplicateIdEntry]
    backfill: BackfillCandidate | None


@dataclass(frozen=True, slots=True)
class TagGroupQuality:
    """Format validation results for a config tag group (required/recommended/other)."""

    group_name: str  # "Required Tags", "Recommended Tags", "Other Tracked Tags"
    fields: list[FieldFormatSection]


@dataclass(frozen=True, slots=True)
class TagQualityFullData:
    """Complete data for the tag quality and integrity report."""

    total_files: int
    date_quality: list[DateQualityRow]  # DATE, ORIGINALDATE precision
    groups: list[TagGroupQuality]  # grouped by config section (only validated tags)
    musicbrainz: IdSectionData
    discogs: IdSectionData


@dataclass(frozen=True, slots=True)
class TagQualitySummaryData:
    """Summary data for tag quality in the summary command."""

    fields_with_invalid: list[FieldValidationRow]
    mb_overall_coverage_pct: float
    mb_malformed_count: int
    discogs_overall_coverage_pct: float
    discogs_malformed_count: int


# =========================================================================
# Report 8 — Duplicates
# =========================================================================


@dataclass(frozen=True, slots=True)
class ExactDuplicateGroup:
    """A group of files sharing the same SHA-256 checksum."""

    checksum: str
    copy_count: int
    paths: list[str]


@dataclass(frozen=True, slots=True)
class MbidDuplicateGroup:
    """Files sharing the same MUSICBRAINZ_TRACKID."""

    mbid: str
    file_count: int
    files: list[MbidDuplicateFile]
    same_directory: bool


@dataclass(frozen=True, slots=True)
class MbidDuplicateFile:
    """A file within an MBID duplicate group."""

    path: str
    album: str
    date: str


@dataclass(frozen=True, slots=True)
class ProbableDuplicateGroup:
    """Files sharing the same (ALBUMARTIST, ALBUM, DISCNUMBER, TRACKNUMBER)
    but with different checksums."""

    albumartist: str
    album: str
    discnumber: str
    tracknumber: str
    file_count: int
    paths: list[str]
    checksums: list[str]


@dataclass(frozen=True, slots=True)
class DuplicatesFullData:
    """Complete data for the duplicates report."""

    exact: list[ExactDuplicateGroup]
    mbid: list[MbidDuplicateGroup]
    probable: list[ProbableDuplicateGroup]


@dataclass(frozen=True, slots=True)
class DuplicatesSummaryData:
    """Summary data for duplicates in the summary command."""

    exact_count: int
    mbid_count: int
    probable_count: int


# =========================================================================
# Report 9 — Scan issues (Album-centric)
# =========================================================================


@dataclass(frozen=True, slots=True)
class AlbumIssuesSummary:
    """Issue counts for a single album, aggregated by severity."""

    artist: str
    album: str
    catalog_number: str | None
    album_dir: str  # Directory path for linking
    error_count: int
    warning_count: int
    info_count: int
    total_count: int


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
    album_level_issues: list[str]  # issues not tied to a specific track
    tracks: list[TrackDetail]


@dataclass(frozen=True, slots=True)
class IssuesFullData:
    """Complete data for the scan issues report (album-level overview)."""

    total_albums: int
    total_files_with_issues: int
    total_issues: int
    albums: list[AlbumIssuesSummary]  # All albums with issues, sorted by severity


@dataclass(frozen=True, slots=True)
class IssuesSummaryData:
    """Summary data for scan issues in the summary command."""

    total_albums_with_issues: int
    total_issues: int
    by_severity: dict[str, int]  # severity → count
    top_albums: list[AlbumIssuesSummary]  # top 5 albums by issue count


# =========================================================================
# Report 10 — Artist name consistency
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
# Report 11 — Genre analysis
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
# Report 12 — Lyrics coverage
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
# Report 13 — ReplayGain coverage
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
    technical: TechnicalSummaryData | None = None
    tags: TagCoverageSummaryData | None = None
    tag_quality: TagQualitySummaryData | None = None
    album_consistency: AlbumConsistencySummaryData | None = None
    duplicates: DuplicatesSummaryData | None = None
    issues: IssuesSummaryData | None = None
    artists: ArtistsSummaryData | None = None
    genres: GenresSummaryData | None = None
    lyrics: LyricsSummaryData | None = None
    replaygain: ReplayGainSummaryData | None = None
