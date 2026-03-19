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
    present_nonempty_count: int
    present_nonempty_pct: float
    present_empty_count: int
    present_empty_pct: float
    absent_count: int
    absent_pct: float


@dataclass(frozen=True, slots=True)
class AliasUsageRow:
    """Counts for a single alias→canonical mapping."""

    canonical_key: str
    alias_key: str
    files_using_alias: int
    files_using_alias_pct: float


@dataclass(frozen=True, slots=True)
class TagInventoryRow:
    """One row in the full tag key inventory."""

    tag_key_upper: str
    file_count: int
    coverage_pct: float
    classification: str  # required / recommended / musicbrainz / other / alias / standard_optional / strip / unknown


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


@dataclass(frozen=True, slots=True)
class DatePrecisionRow:
    """Distribution of date format precision."""

    precision: str  # "full_date" / "year_month" / "year_only" / "invalid"
    count: int
    percentage: float


# =========================================================================
# Report 1 — Collection overview
# =========================================================================


@dataclass(frozen=True, slots=True)
class OverviewTotals:
    """Top-level collection totals."""

    total_tracks: int
    total_artists: int  # unique ALBUMARTIST values
    total_albums: int  # unique ALBUMARTIST+ALBUM combinations
    total_duration_seconds: float
    total_size_bytes: int


@dataclass(frozen=True, slots=True)
class ReleaseTypeBreakdown:
    """Breakdown for a single release type (Albums / EPs / etc.)."""

    release_type: str
    track_count: int
    album_count: int
    total_size_bytes: int
    total_duration_seconds: float
    avg_album_duration_seconds: float
    avg_track_duration_seconds: float


@dataclass(frozen=True, slots=True)
class DistributionSummary:
    """Median / average stats for the overview report."""

    median_track_duration_seconds: float
    median_file_size_bytes: int
    avg_bitrate_kbps: float
    median_bitrate_kbps: float


@dataclass(frozen=True, slots=True)
class OverviewFullData:
    """Complete data for the collection overview report."""

    totals: OverviewTotals
    by_release_type: list[ReleaseTypeBreakdown]
    distribution: DistributionSummary


@dataclass(frozen=True, slots=True)
class OverviewSummaryData:
    """Summary data for the overview in the summary command."""

    totals: OverviewTotals


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
    musicbrainz_coverage: list[TagCoverageRow]
    discogs_coverage: list[TagCoverageRow]
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
# Report 5 — Tag format quality
# =========================================================================


@dataclass(frozen=True, slots=True)
class FieldFormatSection:
    """Validation results for a single field."""

    field_name: str
    summary: FieldValidationRow
    invalid_values: list[InvalidValueRow]
    invalid_values_total: int  # total count if capped at 20
    # Optional extra distributions (e.g. date precision)
    extra: dict[str, list[DatePrecisionRow]] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TagFormatsFullData:
    """Complete data for the tag format quality report."""

    total_files: int
    sections: list[FieldFormatSection]


@dataclass(frozen=True, slots=True)
class TagFormatsSummaryData:
    """Summary data for tag formats in the summary command."""

    fields_with_invalid: list[FieldValidationRow]


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


# =========================================================================
# Report 7 — External ID coverage and integrity
# =========================================================================


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
class IdsFullData:
    """Complete data for the external IDs report."""

    total_files: int
    musicbrainz: IdSectionData
    discogs: IdSectionData


@dataclass(frozen=True, slots=True)
class IdsSummaryData:
    """Summary data for external IDs in the summary command."""

    total_files: int
    mb_overall_coverage_pct: float  # % of files with all 6 MBIDs valid
    mb_malformed_count: int
    mb_partial_album_count: int
    mb_backfill_track_count: int
    discogs_overall_coverage_pct: float  # % of files with all 4 Discogs IDs valid
    discogs_malformed_count: int
    discogs_partial_album_count: int
    discogs_backfill_track_count: int


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
# Report 9 — Scan issues
# =========================================================================


@dataclass(frozen=True, slots=True)
class IssuePivotRow:
    """Count for a (severity, issue_type) combination."""

    severity: str
    issue_type: str
    count: int


@dataclass(frozen=True, slots=True)
class AlbumIssueRow:
    """Issues grouped by album directory."""

    album_dir: str
    issue_count: int


@dataclass(frozen=True, slots=True)
class ArtistIssueRow:
    """Issues grouped by ALBUMARTIST."""

    artist: str
    issue_count: int


@dataclass(frozen=True, slots=True)
class IssueDetailRow:
    """A single issue with full detail."""

    file_path: str | None
    issue_type: str
    severity: str
    description: str
    details: str | None  # JSON string


@dataclass(frozen=True, slots=True)
class IssuesFullData:
    """Complete data for the scan issues report."""

    total_open: int
    pivot: list[IssuePivotRow]
    by_album: list[AlbumIssueRow]
    by_artist: list[ArtistIssueRow]
    by_type: dict[str, list[IssueDetailRow]]  # issue_type → detail rows


@dataclass(frozen=True, slots=True)
class IssuesSummaryData:
    """Summary data for scan issues in the summary command."""

    total_open: int
    by_severity: dict[str, int]  # severity → count
    top_issue_types: list[IssuePivotRow]  # top 5 by count


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
    tag_formats: TagFormatsSummaryData | None = None
    album_consistency: AlbumConsistencySummaryData | None = None
    ids: IdsSummaryData | None = None
    duplicates: DuplicatesSummaryData | None = None
    issues: IssuesSummaryData | None = None
    artists: ArtistsSummaryData | None = None
    genres: GenresSummaryData | None = None
    lyrics: LyricsSummaryData | None = None
    replaygain: ReplayGainSummaryData | None = None
