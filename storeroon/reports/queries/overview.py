"""
storeroon.reports.queries.overview — Collection overview.

Folder-based hierarchical breakdown enriched with per-album issue counts.

Public API:
    full_data(conn) -> OverviewFullData
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict

from storeroon.reports.models import (
    AlbumBreakdown,
    ArtistBreakdown,
    IssuesTotals,
    OverviewFullData,
    OverviewSummaryData,
    OverviewTotals,
    ReleaseTypeBreakdown,
)
from storeroon.reports.utils import album_dir_from_path

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

_FILES_SQL = """
SELECT
    f.id,
    f.size_bytes,
    fp.duration_seconds,
    SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) AS album_dir,
    COALESCE(t_aa.tag_value, 'Unknown')   AS albumartist,
    COALESCE(t_rt.tag_value, 'unknown')   AS releasetype,
    COALESCE(t_al.tag_value, 'Unknown')   AS album,
    t_cn.tag_value                         AS catalognumber,
    t_od.tag_value                         AS originaldate
FROM files f
LEFT JOIN flac_properties fp ON fp.file_id = f.id
LEFT JOIN (SELECT file_id, tag_value FROM raw_tags WHERE tag_key_upper = 'ALBUMARTIST' AND tag_index = 0) t_aa ON t_aa.file_id = f.id
LEFT JOIN (SELECT file_id, tag_value FROM raw_tags WHERE tag_key_upper = 'RELEASETYPE' AND tag_index = 0) t_rt ON t_rt.file_id = f.id
LEFT JOIN (SELECT file_id, tag_value FROM raw_tags WHERE tag_key_upper = 'ALBUM' AND tag_index = 0) t_al ON t_al.file_id = f.id
LEFT JOIN (SELECT file_id, tag_value FROM raw_tags WHERE tag_key_upper = 'CATALOGNUMBER' AND tag_index = 0) t_cn ON t_cn.file_id = f.id
LEFT JOIN (SELECT file_id, tag_value FROM raw_tags WHERE tag_key_upper = 'ORIGINALDATE' AND tag_index = 0) t_od ON t_od.file_id = f.id
WHERE f.status = 'ok'
"""

_ISSUES_SQL = """
SELECT
    si.file_id,
    si.severity,
    f.path,
    f.filename
FROM scan_issues si
JOIN files f ON f.id = si.file_id
WHERE si.resolved = 0
  AND f.status = 'ok'
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_display_name(
    original_date: str | None, album: str, catalog_number: str | None
) -> str:
    """Build display: '{YYYY} - {album} [{catalognumber}]'."""
    parts: list[str] = []
    if original_date and original_date.strip():
        # Extract just the year (first 4 chars) for display
        year = original_date.strip()[:4]
        parts.append(year)
    parts.append(album)
    name = " - ".join(parts)
    if catalog_number and catalog_number.strip():
        name += f" [{catalog_number.strip()}]"
    return name




# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def full_data(
    conn: sqlite3.Connection,
    *,
    aliases: dict[str, str] | None = None,
    canonical_keys: frozenset[str] | None = None,
) -> OverviewFullData:
    """Return collection overview with scan issue counts per album."""

    # ── Phase 1: Build folder hierarchy from files ──
    file_rows = conn.execute(_FILES_SQL).fetchall()

    folder_tracks: dict[str, int] = defaultdict(int)
    folder_size: dict[str, int] = defaultdict(int)
    folder_duration: dict[str, float] = defaultdict(float)
    folder_meta: dict[str, dict[str, str | None]] = {}
    folder_file_ids: dict[str, set[int]] = defaultdict(set)

    total_tracks = 0
    total_size = 0
    total_duration = 0.0

    for row in file_rows:
        adir: str = row["album_dir"] or ""
        size: int = row["size_bytes"] or 0
        duration: float = row["duration_seconds"] or 0.0

        folder_tracks[adir] += 1
        folder_size[adir] += size
        folder_duration[adir] += duration
        folder_file_ids[adir].add(row["id"])

        if adir not in folder_meta:
            folder_meta[adir] = {
                "albumartist": row["albumartist"],
                "releasetype": (row["releasetype"] or "unknown").lower(),
                "album": row["album"],
                "catalognumber": row["catalognumber"],
                "originaldate": row["originaldate"],
            }

        total_tracks += 1
        total_size += size
        total_duration += duration

    # ── Phase 2: Count issues per album folder and per file ──
    issue_rows = conn.execute(_ISSUES_SQL).fetchall()

    folder_issues: dict[str, dict[str, int]] = defaultdict(lambda: {
        "critical": 0, "error": 0, "warning": 0, "info": 0,
    })
    # Per-file severity tracking for health score calculation
    file_has_error: dict[int, bool] = defaultdict(bool)
    file_warning_count: dict[int, int] = defaultdict(int)
    files_with_issues: set[int] = set()
    total_issues = 0
    albums_with_issues: set[str] = set()

    for ir in issue_rows:
        adir = album_dir_from_path(ir["path"])
        sev = ir["severity"]
        fid = ir["file_id"]
        if sev in ("critical", "error"):
            folder_issues[adir]["critical" if sev == "critical" else "error"] += 1
            file_has_error[fid] = True
        elif sev == "warning":
            folder_issues[adir]["warning"] += 1
            file_warning_count[fid] += 1
        else:
            folder_issues[adir]["info"] += 1
        files_with_issues.add(fid)
        total_issues += 1
        albums_with_issues.add(adir)

    def _compute_health(adir: str, issues: dict[str, int]) -> int:
        """Compute album health score from per-track issue data."""
        if issues["critical"] > 0:
            return 0
        fids = folder_file_ids.get(adir, set())
        if not fids:
            return 100
        track_scores: list[int] = []
        for fid in fids:
            if file_has_error.get(fid, False):
                track_scores.append(0)
            else:
                warnings = file_warning_count.get(fid, 0)
                track_scores.append(max(0, 100 - warnings * 5))
        return round(sum(track_scores) / len(track_scores)) if track_scores else 100

    # ── Phase 2b: Run album consistency checks per folder ──
    from storeroon.reports.queries.album_consistency import (
        _CONSISTENCY_FIELDS,
        _check_field_consistency,
        _check_track_numbering,
    )

    for adir in folder_meta:
        track_count = folder_tracks[adir]
        # Field consistency → warning
        for field_name in _CONSISTENCY_FIELDS:
            violation = _check_field_consistency(conn, adir, field_name, track_count)
            if violation is not None:
                folder_issues[adir]["warning"] += 1
                total_issues += 1
                albums_with_issues.add(adir)

        # Track numbering violations
        numbering_violations = _check_track_numbering(conn, adir, track_count)
        for nv in numbering_violations:
            if nv.check_type in ("missing_track", "missing_disc"):
                folder_issues[adir]["critical"] += 1
            else:
                folder_issues[adir]["warning"] += 1
            total_issues += 1
            albums_with_issues.add(adir)

    # ── Phase 2c: Per-file alias consistency checks ──
    _aliases = aliases or {}
    _canonical = canonical_keys or frozenset()

    if _aliases and _canonical:
        # Build relevant alias pairs
        relevant_pairs: list[tuple[str, str]] = []
        all_keys: set[str] = set()
        for alias_key, canon_key in _aliases.items():
            if canon_key in _canonical:
                relevant_pairs.append((alias_key, canon_key))
                all_keys.add(alias_key)
                all_keys.add(canon_key)

        if relevant_pairs:
            # Fetch all relevant tag values across all files in one query
            key_placeholders = ",".join(f"'{k}'" for k in all_keys)
            alias_sql = f"""
            SELECT rt.file_id, rt.tag_key_upper, rt.tag_value
            FROM raw_tags rt
            JOIN files f ON f.id = rt.file_id
            WHERE f.status = 'ok'
              AND rt.tag_index = 0
              AND rt.tag_key_upper IN ({key_placeholders})
            """
            alias_rows = conn.execute(alias_sql).fetchall()

            # Build file_id → {tag_key: tag_value}
            file_tags: dict[int, dict[str, str]] = defaultdict(dict)
            for ar in alias_rows:
                file_tags[ar["file_id"]][ar["tag_key_upper"]] = ar["tag_value"]

            # Check each file, attribute mismatches to the file's album folder
            # Build file_id → album_dir lookup
            file_to_adir: dict[int, str] = {}
            for adir, fids in folder_file_ids.items():
                for fid in fids:
                    file_to_adir[fid] = adir

            for fid, tags in file_tags.items():
                adir = file_to_adir.get(fid)
                if not adir:
                    continue
                for alias_key, canon_key in relevant_pairs:
                    canon_val = tags.get(canon_key)
                    if not canon_val or not canon_val.strip():
                        continue  # canonical not present — skip
                    alias_val = tags.get(alias_key)
                    if not alias_val or not alias_val.strip():
                        # Canonical present but alias missing → warning
                        folder_issues[adir]["warning"] += 1
                        file_warning_count[fid] = file_warning_count.get(fid, 0) + 1
                        total_issues += 1
                        albums_with_issues.add(adir)
                    elif canon_val.strip() != alias_val.strip():
                        # Both present but values differ → warning
                        folder_issues[adir]["warning"] += 1
                        file_warning_count[fid] = file_warning_count.get(fid, 0) + 1
                        total_issues += 1
                        albums_with_issues.add(adir)

    # ── Phase 2d: Audio technical quality checks per album ──
    from storeroon.reports.queries.technical import _is_suspicious_vendor

    _AUDIO_PROPS_SQL = """
    SELECT fp.bits_per_sample, fp.sample_rate_hz, fp.channels,
           fp.vendor_string, f.id AS file_id
    FROM flac_properties fp
    JOIN files f ON f.id = fp.file_id
    WHERE f.status = 'ok'
    """
    audio_rows = conn.execute(_AUDIO_PROPS_SQL).fetchall()

    # Build file_id → album_dir reverse map
    fid_to_adir: dict[int, str] = {}
    for adir, fids in folder_file_ids.items():
        for fid in fids:
            fid_to_adir[fid] = adir

    # Group audio properties by album
    album_bit_depths: dict[str, set[int]] = defaultdict(set)
    album_sample_rates: dict[str, set[int]] = defaultdict(set)
    album_channels: dict[str, set[int]] = defaultdict(set)
    album_vendors: dict[str, set[str]] = defaultdict(set)

    for ar in audio_rows:
        adir = fid_to_adir.get(ar["file_id"])
        if not adir:
            continue
        if ar["bits_per_sample"]:
            album_bit_depths[adir].add(ar["bits_per_sample"])
        if ar["sample_rate_hz"]:
            album_sample_rates[adir].add(ar["sample_rate_hz"])
        if ar["channels"]:
            album_channels[adir].add(ar["channels"])
        if ar["vendor_string"]:
            album_vendors[adir].add(ar["vendor_string"])

    for adir in folder_meta:
        if len(album_bit_depths.get(adir, set())) > 1:
            folder_issues[adir]["error"] += 1
            total_issues += 1
            albums_with_issues.add(adir)
        if len(album_sample_rates.get(adir, set())) > 1:
            folder_issues[adir]["error"] += 1
            total_issues += 1
            albums_with_issues.add(adir)
        if len(album_channels.get(adir, set())) > 1:
            folder_issues[adir]["error"] += 1
            total_issues += 1
            albums_with_issues.add(adir)
        for vendor in album_vendors.get(adir, set()):
            if _is_suspicious_vendor(vendor):
                folder_issues[adir]["info"] += 1
                total_issues += 1
                albums_with_issues.add(adir)
                break  # one info per album, not per vendor

    # ── Phase 3: Build hierarchy ──
    # Album level
    rtype_groups: dict[tuple[str, str], list[AlbumBreakdown]] = defaultdict(list)

    for adir, meta in folder_meta.items():
        artist = meta["albumartist"] or "Unknown"
        rtype = meta["releasetype"] or "unknown"
        display = _make_display_name(
            meta["originaldate"], meta["album"] or "Unknown", meta["catalognumber"]
        )
        issues = folder_issues.get(adir, {"critical": 0, "error": 0, "warning": 0, "info": 0})

        ab = AlbumBreakdown(
            album_dir=adir,
            display_name=display,
            track_count=folder_tracks[adir],
            total_size_bytes=folder_size[adir],
            total_duration_seconds=folder_duration[adir],
            health_score=_compute_health(adir, issues),
            critical_count=issues["critical"],
            error_count=issues["error"],
            warning_count=issues["warning"],
            info_count=issues["info"],
        )
        rtype_groups[(artist, rtype)].append(ab)

    # Sort albums by display name
    for albums in rtype_groups.values():
        albums.sort(key=lambda a: a.display_name.lower())

    # Release type level
    artist_groups: dict[str, list[ReleaseTypeBreakdown]] = defaultdict(list)
    for (artist, rtype), albums in rtype_groups.items():
        rtb = ReleaseTypeBreakdown(
            release_type=rtype,
            album_count=len(albums),
            track_count=sum(a.track_count for a in albums),
            total_size_bytes=sum(a.total_size_bytes for a in albums),
            total_duration_seconds=sum(a.total_duration_seconds for a in albums),
            critical_count=sum(a.critical_count for a in albums),
            error_count=sum(a.error_count for a in albums),
            warning_count=sum(a.warning_count for a in albums),
            info_count=sum(a.info_count for a in albums),
            albums=albums,
        )
        artist_groups[artist].append(rtb)

    # Sort release types
    for rtypes in artist_groups.values():
        rtypes.sort(key=lambda rt: rt.release_type.lower())

    # Artist level
    result: list[ArtistBreakdown] = []
    for artist in sorted(artist_groups.keys(), key=str.lower):
        rtypes = artist_groups[artist]
        result.append(
            ArtistBreakdown(
                artist=artist,
                album_count=sum(rt.album_count for rt in rtypes),
                track_count=sum(rt.track_count for rt in rtypes),
                total_size_bytes=sum(rt.total_size_bytes for rt in rtypes),
                total_duration_seconds=sum(rt.total_duration_seconds for rt in rtypes),
                critical_count=sum(rt.critical_count for rt in rtypes),
                error_count=sum(rt.error_count for rt in rtypes),
                warning_count=sum(rt.warning_count for rt in rtypes),
                info_count=sum(rt.info_count for rt in rtypes),
                release_types=rtypes,
            )
        )

    all_artists = {meta["albumartist"] or "Unknown" for meta in folder_meta.values()}

    totals = OverviewTotals(
        total_album_artists=len(all_artists),
        total_albums=len(folder_meta),
        total_tracks=total_tracks,
        total_duration_seconds=total_duration,
        total_size_bytes=total_size,
    )

    issues_totals = IssuesTotals(
        albums_with_issues=len(albums_with_issues),
        files_with_issues=len(files_with_issues),
        total_issues=total_issues,
    )

    return OverviewFullData(
        totals=totals,
        issues_totals=issues_totals,
        by_artist=result,
    )


def summary_data(conn: sqlite3.Connection) -> OverviewSummaryData:
    """Return headline metrics only for the ``summary`` command."""
    # Reuse full_data to get totals (the hierarchy is discarded)
    data = full_data(conn)
    return OverviewSummaryData(totals=data.totals)
