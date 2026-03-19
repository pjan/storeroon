# Music Collection Management Toolchain — Project Context & Current State

## What this project is

A Python toolchain and SQLite database for cleaning, normalising, and maintaining a personal FLAC music collection of ~40,000 files. The goal is a consistent, well-tagged collection with embedded lyrics, cover art, and a database that mirrors and stages all metadata changes before they touch the files.

---

## Collection structure

Files live under a single root folder. The folder hierarchy is the naming schema:

```
{album artist}/Albums/{original release year} - {Release title}/{album artist} - {release date} - {album title} [{catalog number}]/
{album artist}/Albums/{original release year} - {Release title}/{album artist} - {release date} - {album title} ({release version}) [{catalog number}]/
{album artist}/Compilations/...
{album artist}/Live/...
{album artist}/EPs/...
{album artist}/Singles/...
```

Track filenames follow one of two patterns:
- Single-disc: `{track number} {track title}.flac`
- Multi-disc: `{disc number}-{track number} {track title}.flac`

**Important:** The file and folder names were calculated from the metadata tags, so the tags are the authoritative source of truth. Path structure can be treated as derived, not independent.

Folders may also contain `.lrc` lyric sidecar files (same stem as the FLAC), cover images (`cover.jpg` etc.), and occasionally `.cue`, `.log`, and other files.

---

## Overall process (all phases)

The cleanup process is staged: the database is always cleaned and verified first, and only then are changes written back to files.

### Phase 0 — Pre-flight
- Walk the collection tree; build a file manifest with SHA-256 checksums (the "before" snapshot)
- Identify non-FLAC audio files (MP3, AAC, etc.) for review
- Identify files that shouldn't be in collection folders (not FLAC/LRC/image/CUE/LOG) — flag for quarantine, never delete
- Run `flac --test` integrity check on all FLAC files

### Phase 1 — Import to database
- Read all FLAC files; import physical properties (STREAMINFO block) and all Vorbis comment tags verbatim into the database
- Raw tag data is immutable once written — it is the permanent record of the pre-cleanup state
- Raise `scan_issues` rows for any anomalies found during import (unreadable files, encoding problems, missing required tags, etc.)

### Phase 2 — Quality analysis & reporting
- Comprehensive read-only analysis of collection quality across 13 report areas
- No writes to the database; no writes to any file; no external API calls
- Output to terminal (Rich), CSV/JSON files, and HTML
- The findings from this sprint inform all decisions made in Sprints 3 and 4

### Phase 3 — Enrichment (DB only)
- Match releases to MusicBrainz: check for existing MBIDs in tags, then AcoustID fingerprinting, then fuzzy search; flag ambiguous matches for human review
- Pull full release metadata from MusicBrainz API (dates, label, catalog number, ISRC, etc.)
- Cross-reference with Discogs for release data and integrity checking
- Fetch cover art via Cover Art Archive (keyed by MusicBrainz album ID)
- Fetch synced LRC lyrics via LRCLIB (by ISRC first, then artist+title+duration)
- Calculate ReplayGain values using `loudgain`
- All fetched data stored in the database; nothing written to files yet

### Phase 4 — Write tags to files
- Dry-run first: print a diff of what would change without writing anything
- Write canonical tag set from DB to FLAC files (using mutagen); strip all non-canonical tags
- Verify writes by re-reading tags and comparing against the DB

### Phase 5 — Rename files and folders
- Generate new paths from the canonical tag values in the DB
- Validate for conflicts, illegal characters, and path length (Windows 260-char limit)
- Execute renames; update paths in the DB

### Phase 6 — Post-write verification
- Re-read all tags; diff against the pre-write DB state
- Run `flac --test` again; compare checksums against Phase 0 manifest

### Phase 7 — Backfill folders
- Write cover images to album folders as `cover.jpg`; embed into FLAC files
- Write `.lrc` sidecar files; embed lyrics as `LYRICS` tag in FLAC files
- Move quarantine-flagged files out of the collection

### Phase 8 — Maintenance mode
- Inbox pattern: new downloads go to a staging folder, never directly into the collection
- Auto-identify new releases via existing MBIDs or AcoustID fingerprinting
- Human review step (minimal: confirm MusicBrainz match, ~10–30 seconds per release)
- Auto-enrich, tag, rename, and move into the collection on confirmation

---

## Current state

**Phase 1 database schema has been designed and is already in the repository.** It consists of exactly four tables plus one view:

### `files`
One row per FLAC file. The stable identity anchor for everything else.

```sql
id, path (UNIQUE), filename, size_bytes, checksum_sha256,
mtime_on_disk, status (ok|unreadable|missing),
imported_at, last_scanned_at
```

### `flac_properties`
Technical audio stream properties from the FLAC STREAMINFO block. One row per file.

```sql
id, file_id (FK → files), duration_seconds (REAL),
sample_rate_hz, bits_per_sample, channels, total_samples,
audio_md5, vendor_string, approx_bitrate_kbps
```

`duration_seconds` is stored as `REAL` (IEEE 754 double, ~15 significant digits of precision). It is used as a disambiguation signal in AcoustID fingerprint lookups and LRCLIB queries in Phase 3 — do not cast to integer or truncate when reading from mutagen.

`audio_md5` is the MD5 of the decoded PCM stream embedded by the encoder. NULL means the encoder did not embed one — a data quality flag. Verifiable with `flac --test`.

### `raw_tags`
Verbatim Vorbis comment data. **Immutable after initial import.** One row per tag key-value instance; multi-value tags (e.g. multiple `ARTIST` entries) get multiple rows with incrementing `tag_index`.

```sql
id, file_id (FK → files), imported_at,
tag_key (verbatim casing), tag_key_upper (always use for queries),
tag_value, tag_index,
encoding_suspect (bool), raw_bytes_hex (only set when encoding_suspect)
```

`encoding_suspect` flags tags where non-UTF-8 bytes were detected — a known problem with pre-~2010 rips that used Latin-1 or Windows-1252 encoding.

### `scan_issues`
Quality flags and anomalies, one row per discrete problem. The review and resolution queue for the whole pipeline.

```sql
id, file_id (FK → files, nullable for collection-wide issues),
issue_type (text, controlled vocabulary),
severity (info|warning|error|critical),
description, details (TEXT, JSON string),
detected_at, resolved (bool), resolved_at, resolution_method
```

Controlled `issue_type` vocabulary (extend as needed):
- `file_unreadable`, `tag_read_error`, `tag_encoding_suspect`, `no_audio_md5`
- `missing_required_tag`, `empty_tag_value`
- `date_format_invalid`, `tracknumber_format_invalid`, `discnumber_format_invalid`, `unknown_tag_key`
- `duplicate_checksum`

### `v_common_tags` (view)
Pivots `raw_tags` EAV rows into named columns for the ~35 most common Vorbis tags, including legacy alias variants (`YEAR`, `TRACKTOTAL`, `DISCTOTAL`, `ALBUM ARTIST`, etc.). Also joins `flac_properties` columns and exposes two derived flags: `total_tag_count` and `has_open_issues`. Use this for ad-hoc analysis and quality queries. For multi-value tags, query `raw_tags` directly.

---

## Technology stack

- **Language:** Python 3.11+
- **Database:** SQLite (the database is a single file; no server required)
- **Key libraries:**
  - `sqlite3` — built-in Python module; no additional driver needed
  - `mutagen` — FLAC tag reading and writing
  - `rich` — terminal output: tables, progress bars, colour (Sprint 2+)
  - `jinja2` — HTML report templating (Sprint 2+)
  - `pyacoustid` + `fpcalc` (Chromaprint) — AcoustID fingerprinting in Phase 3
  - `musicbrainzngs` — MusicBrainz API client in Phase 3
  - `loudgain` — ReplayGain calculation in Phase 3 (CLI tool, called via subprocess)
  - `click` or `typer` — CLI framework for all scripts
- **System dependencies:** `flac` CLI binary (for `flac --test` integrity checks, called via subprocess)
- **Configuration:** single TOML config file read by every script; defines collection root path, database file path, canonical tag schema, API credentials, naming templates, and report tuning parameters (fuzzy match threshold, output directory, etc.)
- **All scripts must have a `--dry-run` flag** that prints what would change without writing anything

### SQLite-specific notes
- The schema uses `TEXT` instead of PostgreSQL's `JSONB` for the `details` column in `scan_issues`. Store as a JSON string; parse in Python with `json.loads()`. SQLite's `json_extract()` is available for in-database queries if needed (requires SQLite 3.38+).
- `duration_seconds` is stored as `REAL` (SQLite's floating-point type). SQLite is dynamically typed, so the `NUMERIC(12,6)` declaration in the schema file is advisory — values are stored as IEEE 754 doubles, which gives ~15 significant digits of precision, sufficient for microsecond-level duration accuracy.
- SQLite does not support `ALTER TABLE ... DROP COLUMN` cleanly on older versions. During active schema development (Sprint 1), simply delete the database file and re-run the migration to apply changes. Once real collection data is imported, treat schema changes more carefully.
- Enable WAL mode (`PRAGMA journal_mode=WAL`) and foreign key enforcement (`PRAGMA foreign_keys=ON`) at connection time. These are off by default in SQLite.

---

## Development plan — sprints

### Sprint 1 — Foundation (COMPLETE)

The goal of Sprint 1 was to build the infrastructure that everything else depends on. No enrichment, no writes to files.

**What was built:**
1. Project scaffolding: repo structure, dependency management (`pyproject.toml`), config file loader
2. Database connection module and migration runner (applies the Phase 1 schema; idempotent)
3. **File walker:** recursively walks the collection root; discovers all `.flac` files; yields file records (path, size, mtime, checksum)
4. **FLAC importer:** for each discovered file:
   - Reads the STREAMINFO block → inserts into `flac_properties`
   - Reads all Vorbis comment tags verbatim → inserts into `raw_tags`
   - Inserts the file record into `files`
   - Detects encoding anomalies; sets `encoding_suspect` where needed
   - Raises `scan_issues` rows for: unreadable files, tag read errors, missing required tags (`TITLE`, `ARTIST`, `ALBUMARTIST`, `ALBUM`, `DATE`, `TRACKNUMBER`), empty tag values, missing `audio_md5`
5. **Duplicate detector:** after import, queries for files sharing the same `checksum_sha256`; raises `duplicate_checksum` scan issues
6. **CLI entrypoint:** `python -m musiclib scan --root /path/to/collection [--dry-run]`

---

### Sprint 2 — Analysis & Reporting (CURRENT SPRINT)

The goal of Sprint 2 is to produce a comprehensive, read-only picture of the collection's current state — quality, consistency, completeness, and anomalies — before any cleanup work begins. Every decision made in Sprints 3 and 4 should be informed by having read these reports. Sprint 2 produces **no writes of any kind**: no writes to the database, no writes to files, no external API calls. All findings are ephemeral output only; issues surfaced here will be written to `scan_issues` by Sprint 4 when a human has reviewed the reports and decided what to fix.

---

#### Architecture (build this first, before any individual report)

Sprint 2 must be built as a clean three-layer architecture. Do not skip this — writing report logic and rendering logic in the same function produces unmaintainable code.

**Layer 1 — Query layer** (`musiclib/reports/queries/`)
One module per report area (e.g. `queries/technical.py`, `queries/tag_coverage.py`). Each module contains pure query functions that accept a database connection and return typed Python data structures (dataclasses or typed dicts). No rendering logic lives here. No CLI logic lives here. Functions are independently testable.

Each report module must expose two public functions:
- `full_data(conn) -> ReportData` — returns the complete dataset for the deep-dive subcommand
- `summary_data(conn) -> SummaryData` — returns a reduced dataset (top N rows, headline metrics only) for the `summary` command

Both return dataclass instances, not raw query results. Define the dataclasses in a `musiclib/reports/models.py` file.

**Layer 2 — Renderer layer** (`musiclib/reports/renderers/`)
Four renderer modules: `terminal.py`, `csv_renderer.py`, `json_renderer.py`, `html_renderer.py`. Each accepts a `ReportData` instance and produces output. The terminal renderer uses `rich`. The HTML renderer uses `jinja2`. Renderers know nothing about the database.

The HTML renderer writes a **single self-contained file per subcommand** (no external dependencies — inline `<style>` block only, **no JavaScript**). Clean, readable tables with a `<h1>` title, generation timestamp, and applied filters at the top. Each logical table within the report has a `<h2>` heading. No interactivity, no sidebar, no dark mode toggle. CSS uses `@media (prefers-color-scheme: dark)` for passive dark mode only. The goal is a clean printable document, not an interactive dashboard.

CSV output produces one file per logical table within a report. For reports that produce multiple distinct tables (e.g. Report 6 produces a field-consistency table and a track-numbering table), use explicitly named files: `report_album_consistency_fields.csv`, `report_album_consistency_numbering.csv`. The complete list of output filenames is specified per-report below.

**Layer 3 — CLI layer** (`musiclib/reports/cli.py`)
Wires the query and renderer layers together. Handles `--output`, `--output-dir`, `--artist`, `--album`, `--min-severity` flags. Handles empty database gracefully (print a clear message, exit 0). Handles `--artist` / `--album` filter producing no results gracefully (print "no tracks matched", exit 0).

**Shared infrastructure to build:**
- `musiclib/reports/models.py` — all dataclasses used across the reporting layer
- `musiclib/reports/utils.py` — shared helpers: number formatting, percentage formatting, bar chart rendering for terminal (block characters), severity colour mapping for Rich
- Progress bar wrapper: any query that may take >2 seconds on a 40,000-track collection must show a Rich progress bar. Use `rich.progress.Progress` with a consistent style across all reports.
- Output directory management: create `--output-dir` if it does not exist; warn but do not fail if files already exist (overwrite silently).

**CLI structure:**

```
python -m musiclib report summary
python -m musiclib report overview      [--output terminal|csv|json|html] [--output-dir PATH]
python -m musiclib report technical     [--output terminal|csv|json|html] [--output-dir PATH]
python -m musiclib report tags          [--output terminal|csv|json|html] [--output-dir PATH]
python -m musiclib report tag-formats   [--output terminal|csv|json|html] [--output-dir PATH] [--artist ARTIST]
python -m musiclib report album-consistency [--output terminal|csv|json|html] [--output-dir PATH] [--artist ARTIST]
python -m musiclib report ids           [--output terminal|csv|json|html] [--output-dir PATH] [--artist ARTIST]
python -m musiclib report duplicates    [--output terminal|csv|json|html] [--output-dir PATH]
python -m musiclib report issues        [--output terminal|csv|json|html] [--output-dir PATH] [--min-severity info|warning|error|critical]
python -m musiclib report artists       [--output terminal|csv|json|html] [--output-dir PATH]
python -m musiclib report genres        [--output terminal|csv|json|html] [--output-dir PATH]
python -m musiclib report lyrics        [--output terminal|csv|json|html] [--output-dir PATH] [--artist ARTIST]
python -m musiclib report replaygain    [--output terminal|csv|json|html] [--output-dir PATH] [--artist ARTIST]
```

Flag notes:
- `--output` default is `terminal` for all deep-dive subcommands; create `--output-dir` if it does not exist; overwrite existing files silently; filename pattern: `{report_name}_{table_name}_{YYYYMMDD_HHMMSS}.{ext}`
- `summary` has **no `--output` flag** — it is terminal-only by design. Its purpose is a fast health check, not a data export. Run individual subcommands with `--output csv` or `--output json` for exports.
- `--artist` performs a **case-insensitive substring match** on `ALBUMARTIST`. `--album` may only be used in combination with `--artist` — reject `--album` without `--artist` with a clear error message. If filters match no records, print `"No tracks matched the given filters."` and exit with code 0.
- The fuzzy similarity threshold (used in `artists` and `genres`) is read from the TOML config under `[reports] fuzzy_threshold = 0.85`; it is not a CLI flag
- The `summary` command runs only **fast aggregate queries** — no fuzzy matching, no O(n²) operations, no per-album nested loops. It must complete in under 30 seconds on a 40,000-track collection. Any report section that requires expensive computation is excluded from summary mode; this is noted per-report below.

---

#### Report 1 — Collection overview (`report overview`)

**Query logic:**
Aggregate queries against `files`, `flac_properties`, and `raw_tags`. Group by the release type directory component (second path segment: Albums / EPs / Singles / Compilations / Live — extract using SQLite's `substr` / `instr` path parsing).

**Full report tables:**
1. Top-level totals: total tracks, total unique `ALBUMARTIST` values, total unique `ALBUMARTIST + ALBUM` combinations (proxy for album count), combined duration (formatted as days/hours/minutes), total size on disk (formatted as GB)
2. Breakdown by release type (the five folder categories): track count, album count (unique `ALBUMARTIST + ALBUM` pairs), total size, total duration, average album duration, average track duration
3. Distribution summary: median track duration, median file size, average and median approximate bitrate

**Summary data:** Table 1 only (top-level totals).

**Output files (CSV/JSON):** `report_overview_totals.csv`, `report_overview_by_release_type.csv`

---

#### Report 2 — Audio technical quality (`report technical`)

**Query logic:**
All from `flac_properties` joined to `files`. Bucketing is done in Python after fetching raw values, not in SQL, for flexibility.

**Full report tables:**
1. Sample rate distribution: counts and percentages for 44100 / 48000 / 88200 / 96000 / 176400 / 192000 / other. Rendered as a horizontal bar chart in terminal output.
2. Bit depth distribution: 16 / 24 / 32 / other. Bar chart.
3. Channel distribution: 1 (mono) / 2 (stereo) / other.
4. Approximate bitrate distribution (kbps): buckets `<400` / `400–600` / `600–800` / `800–1000` / `1000–1200` / `1200–1500` / `>1500`. Bar chart. Note: this is the most important table for spotting transcodes masquerading as FLAC — a genuine 16-bit/44.1kHz FLAC rip of a CD typically falls in the 800–1200 kbps range; values below 400 kbps for a stereo file are suspicious.
5. File size distribution (MB): `<10` / `10–20` / `20–30` / `30–50` / `>50`
6. Track duration distribution: `<1min` / `1–3min` / `3–5min` / `5–8min` / `8–15min` / `>15min`. Bar chart.
7. Duration outliers: individual listing of all tracks under 30 seconds or over 30 minutes — these almost always indicate a problem (incomplete rip, bonus silence track, wrong file)
8. Encoder provenance: all distinct `vendor_string` values with counts. Flag (with a warning indicator) any vendor string containing (case-insensitive) any of: `iTunes`, `LAME`, `Fraunhofer`, `Windows Media`, `AAC`, `MP3`, `Nero`, `QuickTime`, `RealAudio`. These indicate the file may be a transcode rather than a lossless rip.
9. Missing `audio_md5`: count and percentage of files where `audio_md5 IS NULL`. List affected files grouped by `ALBUMARTIST + ALBUM` (using the `ALBUMARTIST` and `ALBUM` tag values from `raw_tags`).

**Summary data:** Tables 1, 2, 4 (distributions only, no raw values), count of duration outliers, count of suspicious vendor strings.

**Output files (CSV/JSON):** `report_technical_distributions.csv` (all bucketed distributions), `report_technical_outliers.csv`, `report_technical_vendors.csv`, `report_technical_missing_md5.csv`

---

#### Report 3 — Tag coverage and key inventory (`report tags`)

Reports 3 and 4 from the original design are consolidated here. They draw from the same base query (`raw_tags GROUP BY tag_key_upper`) and splitting them into separate subcommands would require running that query twice.

**Prerequisite:** The canonical tag schema must be defined in the TOML config before this report can be implemented. Add the following structure to the config file as part of Sprint 2 scaffolding:

```toml
[tags.required]
fields = ["TITLE", "ARTIST", "ALBUMARTIST", "ALBUM", "DATE", "TRACKNUMBER"]

[tags.recommended]
fields = ["TOTALTRACKS", "DISCNUMBER", "TOTALDISCS", "ORIGINALDATE",
          "LABEL", "CATALOGNUMBER", "ISRC", "RELEASETYPE"]

[tags.musicbrainz]
fields = ["MUSICBRAINZ_TRACKID", "MUSICBRAINZ_RELEASETRACKID",
          "MUSICBRAINZ_ALBUMID", "MUSICBRAINZ_ARTISTID",
          "MUSICBRAINZ_ALBUMARTISTID", "MUSICBRAINZ_RELEASEGROUPID"]

[tags.other]
# Tracked but not canonical — informational coverage only
fields = ["LYRICS", "REPLAYGAIN_TRACK_GAIN", "REPLAYGAIN_TRACK_PEAK",
          "REPLAYGAIN_ALBUM_GAIN", "REPLAYGAIN_ALBUM_PEAK",
          "MEDIA", "BARCODE", "COMMENT"]

[tags.aliases]
# Maps non-canonical key (uppercased) to its canonical replacement
YEAR            = "DATE"
ORIGINALYEAR    = "ORIGINALDATE"
"ALBUM ARTIST"  = "ALBUMARTIST"
TRACKTOTAL      = "TOTALTRACKS"
DISCTOTAL       = "TOTALDISCS"
DISCS           = "TOTALDISCS"

[tags.standard_optional]
# Legitimate tags that are out of canonical scope but should not be stripped.
# Informational only; presence/absence not reported as issues.
fields = ["COMPOSER", "LYRICIST", "CONDUCTOR", "PERFORMER", "ENSEMBLE",
          "OPUS", "PART", "MOVEMENT", "WORK", "SUBTITLE", "GROUPING",
          "MOOD", "BPM", "KEY", "LANGUAGE", "SCRIPT",
          "ACOUSTID_ID", "ACOUSTID_FINGERPRINT",
          "REPLAYGAIN_REFERENCE_LOUDNESS",
          "ACCURATERIPCRC", "ACCURATERIPCOUNT", "ACCURATERIPRESULT", "ACCURATERIPDISCID",
          "ENCODEDBY", "ENCODING", "ENCODERSETTINGS", "SOURCE", "SOURCEMEDIA"]

[tags.strip]
# Populated after reviewing Section C of this report. Left empty initially.
fields = []
```

The report reads all lists from config at runtime. Tag lists are never hardcoded in the report logic.

**Section A: Canonical tag coverage**

Query `raw_tags` with `tag_key_upper` filters for each tag in `required`, `recommended`, `musicbrainz`, and `other` groups. For each tag, compute three counts against the total non-missing file count: (a) present and non-empty (after `TRIM()`), (b) present but empty/whitespace-only, (c) absent entirely.

Output: one table per group. Columns: tag key / present+non-empty (count, %) / present+empty (count, %) / absent (count, %). Sorted by absent % descending within each group. Severity colouring in terminal: required tags with any missing coverage = red; recommended tags with missing % > 20% = yellow; others = no colouring.

**Section B: Alias usage**

For each entry in `tags.aliases`, count files where the alias key is present but the canonical key is absent. These files need key renaming in Phase 4. Columns: canonical key / alias found / files using alias (count, %).

**Section C: Full tag key inventory**

`SELECT tag_key_upper, COUNT(DISTINCT file_id) FROM raw_tags GROUP BY tag_key_upper ORDER BY count DESC`

Classify each key against the config: `required` / `recommended` / `musicbrainz` / `other` / `alias` / `standard_optional` / `unknown`. `unknown` = not found in any config list. These are stripping candidates.

Output: full inventory table (all keys, count, coverage %, classification), then a filtered table of `unknown` keys only sorted by count descending. Keys with coverage below 0.1% are flagged with a note regardless of classification.

The `unknown` keys table is the primary input for populating `tags.strip` in the config — the user reviews this output and decides what to add.

**Summary data:** Section A — required tags with any missing coverage; recommended tags with missing % > 20%. Section C — count of `unknown` keys, top 5 by file count.

**Output files (CSV/JSON):** `report_tags_coverage_required.csv`, `report_tags_coverage_recommended.csv`, `report_tags_coverage_mb.csv`, `report_tags_coverage_other.csv`, `report_tags_aliases.csv`, `report_tags_inventory.csv`, `report_tags_unknown.csv`

---

#### Report 5 — Tag format quality (`report tag-formats`)

**Query logic:**
Fetch raw values from `raw_tags` for each validated field. Apply format checks in Python. Group results into: valid / invalid / absent.

**Validation rules per field:**

`DATE` and `ORIGINALDATE`:
- Valid: matches regex `^\d{4}(-\d{2}(-\d{2})?)?$` (YYYY, YYYY-MM, or YYYY-MM-DD)
- Also report the distribution of format precision: full date / year-month / year-only
- Flag: year value outside range 1900–2030; month outside 01–12; day outside 01–31

`TRACKNUMBER`:
- Valid: a positive integer string, optionally zero-padded; or the legacy `N/T` format (flag these — the total belongs in `TOTALTRACKS`)
- Flag: non-numeric, zero, negative, exceeds 99 (unusual but valid; list individually)

`DISCNUMBER` and `TOTALDISCS`:
- Valid: positive integer string
- Flag: non-numeric, zero, negative, disc number exceeding `TOTALDISCS` value on the same file

`ISRC`:
- Valid: matches regex `^[A-Z]{2}[A-Z0-9]{3}\d{2}\d{5}$` (12 characters, no hyphens)
- Also accept hyphenated form `XX-XXX-YY-NNNNN` and note how many use this form (it should be normalised to no-hyphen in Phase 4)
- Flag: wrong length, non-alphanumeric characters, designation code `00000` (placeholder)

`MUSICBRAINZ_TRACKID`, `MUSICBRAINZ_RELEASETRACKID`, `MUSICBRAINZ_ALBUMID`, `MUSICBRAINZ_ARTISTID`, `MUSICBRAINZ_ALBUMARTISTID`, `MUSICBRAINZ_RELEASEGROUPID`:
- Valid: UUID v4 format, matches regex `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$` (case-insensitive)
- Flag: anything that doesn't match — these are corrupt and will fail all API lookups in Phase 3

**Full report tables:**
One section per field. Each section shows: valid count (%), invalid count (%), absent count (%), and a table of the distinct invalid values with their counts (capped at 20 rows; note total count if more exist).

**Summary data:** Fields with any invalid values, count of invalid instances per field.

**Output files (CSV/JSON):** One file per field: `report_tag_formats_date.csv`, `report_tag_formats_tracknumber.csv`, `report_tag_formats_isrc.csv`, `report_tag_formats_mbids.csv`

---

#### Report 6 — Intra-album consistency (`report album-consistency`)

**Query logic:**
**Album boundaries are defined by the parent directory path of the FLAC file** — i.e., `SUBSTR(path, 1, LENGTH(path) - LENGTH(filename) - 1)`. This is the unambiguous album boundary. Do not group by `ALBUMARTIST + ALBUM` tag values for this purpose — two different releases (e.g. original and remaster) may share those values but are distinct directories and must be checked independently.

For each album directory:
1. Fetch all tracks (all `files` rows) in that directory
2. For each checked field, collect all distinct non-null values across the album's tracks using `raw_tags`
3. **Flag the album if `COUNT(DISTINCT TRIM(LOWER(tag_value))) > 1` for that field** — i.e., any disagreement at all, not a majority/minority judgement. Do not attempt to determine which value is "correct"; simply surface the disagreement for human review. Tracks where the field is NULL are excluded from the distinctness count but a separate check flags albums where some tracks have the field and others don't (NULL mixed with non-NULL is itself a consistency problem).

**Fields checked for cross-track consistency within an album directory:**
`ALBUMARTIST`, `ALBUM`, `DATE`, `ORIGINALDATE`, `TOTALTRACKS`, `TOTALDISCS`, `LABEL`, `CATALOGNUMBER`, `RELEASETYPE`, `MUSICBRAINZ_ALBUMID`, `MUSICBRAINZ_ALBUMARTISTID`, `MUSICBRAINZ_RELEASEGROUPID`

**Track numbering checks (per album directory):**
- `TOTALTRACKS` declared value vs actual FLAC file count in the directory (for single-disc albums or per disc for multi-disc)
- Track number gaps: construct the set of track numbers present; flag if any integer in the range [1, max] is absent
- Track number duplicates: two or more files claim the same track number on the same disc
- Disc number gaps: if `TOTALDISCS` > 1, check that all disc numbers in range [1, TOTALDISCS] have at least one track
- Track number values exceeding the declared `TOTALTRACKS`

**Full report tables:**
1. Field consistency violations: one row per (album directory, field) pair where inconsistency was found. Columns: album path / field / all distinct values found / count of tracks per value. Sorted by album path.
2. Track numbering violations: one row per (album directory, check type) pair. Columns: album path / check type / description of the problem (e.g. "track 7 missing", "two tracks claim disc 2 track 3"). Sorted by album path.
3. Summary by violation type: count of albums affected by each check type.

**Summary data:** Total albums checked, total albums with any violation, top 5 most common violation types with counts.

**Output files (CSV/JSON):** `report_album_consistency_fields.csv`, `report_album_consistency_numbering.csv`, `report_album_consistency_summary.csv`

---

#### Report 7 — External ID coverage and integrity (`report ids`)

**Query logic:**
Query `raw_tags` directly (not `v_common_tags`) for all ID tag instances. For MusicBrainz IDs, fetch all six tag keys. For Discogs IDs, fetch rows WHERE `tag_key_upper IN ('DISCOGS_RELEASE_ID', 'DISCOGS_ARTIST_ID', 'DISCOGS_MASTER_ID', 'DISCOGS_LABEL_ID')` — do not use a substring match on 'DISCOGS'.

The report is structured in two independent sections — MusicBrainz and Discogs — each with identical sub-section structure. This makes parity explicit and makes it easy to add further ID sources (e.g. Bandcamp, iTunes) in future.

---

**MusicBrainz sub-sections:**

**MB-1. Coverage table**

For each of the six MBID tag keys (`MUSICBRAINZ_TRACKID`, `MUSICBRAINZ_RELEASETRACKID`, `MUSICBRAINZ_ALBUMID`, `MUSICBRAINZ_ARTISTID`, `MUSICBRAINZ_ALBUMARTISTID`, `MUSICBRAINZ_RELEASEGROUPID`), one row showing:
- Valid count and % — present and matches UUID v4 regex `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$` (case-insensitive)
- Malformed count and % — present but fails UUID validation (corrupt; will fail all API lookups in Phase 3)
- Absent count and %

Sorted by valid % ascending so the worst-covered IDs are at the top.

**MB-2. Partial album coverage**

Albums (grouped by parent directory path) where `MUSICBRAINZ_ALBUMID` is present on at least one track but absent on at least one other track in the same directory. These are the highest-priority targets for Phase 3 matching — the release is already partially identified and completing it requires only filling the gaps rather than a cold match. Columns: album path / tracks with ALBUMID / tracks without ALBUMID / total tracks.

**MB-3. Duplicate `MUSICBRAINZ_TRACKID`**

The same `MUSICBRAINZ_TRACKID` UUID appearing on two or more different files. Columns: MBID / file count / list of file paths / whether all files are in the same album directory or different directories. Same-directory = likely tagging error. Different directories = may be a legitimate duplicate (you own both the original and a remaster); flag for human review rather than auto-resolution.

**MB-4. Quick-win backfill candidates**

Count (and list in CSV/JSON) of tracks where `MUSICBRAINZ_ALBUMID` is present and valid but `MUSICBRAINZ_RELEASEGROUPID` is absent. The release group ID can be backfilled from the MusicBrainz API in Phase 3 with one lookup per unique album ID — no fingerprinting required. Output: count of affected tracks, count of distinct album IDs (this is the actual API call count required).

---

**Discogs sub-sections:**

**DG-1. Coverage table**

For each of the four Discogs tag keys (`DISCOGS_RELEASE_ID`, `DISCOGS_ARTIST_ID`, `DISCOGS_MASTER_ID`, `DISCOGS_LABEL_ID`), one row showing:
- Valid count and % — present and matches the valid format: a non-empty string of digits representing a positive integer (regex `^\d+$`), value > 0, and value < 100000000 (values at or above this threshold are treated as implausible placeholders)
- Malformed count and % — present but fails format validation
- Absent count and %

Sorted by valid % ascending.

**DG-2. Partial album coverage**

Albums (grouped by parent directory path) where `DISCOGS_RELEASE_ID` is present on at least one track but absent on at least one other in the same directory. Same interpretation as MB-2: partially identified releases are higher-priority enrichment targets. Columns: album path / tracks with DISCOGS_RELEASE_ID / tracks without / total tracks.

**DG-3. Duplicate `DISCOGS_RELEASE_ID`**

The same `DISCOGS_RELEASE_ID` value appearing in two or more different album directories. (Unlike MusicBrainz where the same track recording could legitimately appear on multiple releases, the same Discogs release ID appearing in two separate directories almost always means two rips/downloads of the same physical release and is worth flagging.) Columns: DISCOGS_RELEASE_ID / directory count / list of directory paths. Flag separately: same `DISCOGS_RELEASE_ID` appearing multiple times within the same album directory — this is a tagging error.

**DG-4. Quick-win backfill candidates**

Count (and list in CSV/JSON) of tracks where `DISCOGS_RELEASE_ID` is present and valid but `DISCOGS_MASTER_ID` is absent. The master ID is the Discogs equivalent of MusicBrainz's release group — it links a specific pressing to the canonical master release and is useful for original release year lookup. Output: count of affected tracks, count of distinct release IDs (the actual API call count required in Phase 3).

---

**Summary data:**
- MusicBrainz: overall coverage % (% of files with all six MBIDs valid), count of malformed MBIDs, count of partial-coverage albums, quick-win backfill track count
- Discogs: overall coverage % (% of files with all four Discogs IDs valid), count of malformed IDs, count of partial-coverage albums, quick-win backfill track count

**Output files (CSV/JSON):**
MusicBrainz: `report_ids_mb_coverage.csv`, `report_ids_mb_partial.csv`, `report_ids_mb_duplicates.csv`, `report_ids_mb_backfill.csv`
Discogs: `report_ids_discogs_coverage.csv`, `report_ids_discogs_partial.csv`, `report_ids_discogs_duplicates.csv`, `report_ids_discogs_backfill.csv`

---

#### Report 8 — Duplicates (`report duplicates`)

**Query logic:**
Three passes, each independent.

Pass 1 — Exact: `SELECT checksum_sha256, COUNT(*), GROUP_CONCAT(path) FROM files WHERE checksum_sha256 IS NOT NULL GROUP BY checksum_sha256 HAVING COUNT(*) > 1`

Pass 2 — Same recording (MBID): `SELECT tag_value, COUNT(DISTINCT file_id), GROUP_CONCAT(DISTINCT file_id) FROM raw_tags WHERE tag_key_upper = 'MUSICBRAINZ_TRACKID' GROUP BY tag_value HAVING COUNT(DISTINCT file_id) > 1`. Then join back to `files` and `raw_tags` to get album names and paths.

Pass 3 — Probable duplicates: find files sharing the same (`ALBUMARTIST` tag value, `ALBUM` tag value, `DISCNUMBER` tag value, `TRACKNUMBER` tag value) where checksums differ. Use `raw_tags` with four self-joins or a grouped pivot query. Exclude pairs already caught by Pass 1.

Note: Discogs release-level duplicates (same `DISCOGS_RELEASE_ID` in multiple directories) are reported in Report 7 DG-3, not here, since they operate at album level rather than track level.

**Full report tables:**
1. Exact duplicates: checksum / copy count / all paths. Severity: error.
2. Same-recording duplicates (by `MUSICBRAINZ_TRACKID`): MBID / file count / for each file: path, album name, date. Note whether files are in same or different album directories.
3. Probable duplicates (by position): the (`ALBUMARTIST`, `ALBUM`, `DISCNUMBER`, `TRACKNUMBER`) combination / file count / all paths / all checksums. Severity: warning.

**Summary data:** Count of each duplicate type. Any exact duplicates are displayed prominently.

**Output files (CSV/JSON):** `report_duplicates_exact.csv`, `report_duplicates_mbid.csv`, `report_duplicates_probable.csv`

---

#### Report 9 — Scan issues (`report issues`)

**Query logic:**
Reads from the `scan_issues` table populated during Phase 1 import. All queries filtered by `resolved = FALSE` by default; `--min-severity` flag filters to that severity and above.

**Full report tables:**
1. Issues by severity and type: pivot table showing count for each (severity, issue_type) combination. Sorted by severity (critical first) then count descending.
2. Most-affected albums: group issues by album directory path (derived from `files.path`), show count of issues per album, sorted descending. Top 50 shown; full list in CSV/JSON export.
3. Most-affected artists: group by `ALBUMARTIST` tag value. Top 20 shown.
4. Per-issue-type drill-down: for each distinct `issue_type`, a table of the affected files with their paths and the `details` field parsed from JSON. Each issue type is a collapsible section in HTML output.

**Summary data:** Total open issues, count by severity, top 5 issue types by count.

**Output files (CSV/JSON):** `report_issues_summary.csv`, `report_issues_by_album.csv`, `report_issues_detail.csv`

---

#### Report 10 — Artist name consistency (`report artists`)

**Query logic:**
Fetch all distinct `ALBUMARTIST` values with file counts from `raw_tags`. Fetch all distinct `ARTIST` values with file counts separately.

Fuzzy matching runs in two passes:

**Pass 1 — Case/whitespace variants (fast, always runs including in `summary`):** Group all distinct values by `LOWER(TRIM(tag_value))`. Any group with more than one distinct original value is a case inconsistency. List: normalised form / variant spellings / total track count. Severity: `error` — these are the same artist with certainty.

**Pass 2 — Token-sort fuzzy matching (slow, excluded from `summary`):** For each pair of distinct values not already caught by Pass 1:
1. Pre-filter: skip pairs where `max(len(a), len(b)) / min(len(a), len(b)) > 2.0` (eliminates clearly different-length names).
2. Normalise each name: lowercase, strip leading "the ", tokenise on whitespace, sort tokens alphabetically, rejoin with space. This is the token-sort transformation — it makes "The Beatles" and "Beatles, The" both normalise to "beatles" before comparison, and "David Bowie" and "Bowie, David" both normalise to "bowie david".
3. Compute `difflib.SequenceMatcher` ratio on the normalised forms.
4. Flag pairs above threshold (default 0.85 from TOML `[reports] fuzzy_threshold`).

Cap output at the top 100 pairs by similarity score. Apply same two-pass logic to `ARTIST` values separately.

**Note:** Token-sort catches transposed names (The Beatles / Beatles, The) and re-ordered names (David Bowie / Bowie, David). It will not catch abbreviations (R.E.M. / REM), transliterations, or intentionally different names with similar letters — those remain for human judgement.

**Full report tables:**
1. All distinct `ALBUMARTIST` values: value / track count / album count. Sorted by track count descending.
2. `ALBUMARTIST` fuzzy similarity pairs: name A / name B / similarity score / track count A / track count B. Sorted by similarity descending. Capped at 100 pairs.
3. All distinct `ARTIST` values: same structure as table 1. Note: this list will be long (includes all featured artists) — in terminal output, show only top 100 by count with a note that CSV contains the full list.
4. `ARTIST` fuzzy similarity pairs: same structure as table 2.

**Note:** This report is exploratory and produces no fix decisions. The output is a human review list.

**Summary data:** Count of distinct `ALBUMARTIST` values, count of `ALBUMARTIST` pairs above similarity threshold, count of distinct `ARTIST` values.

**Output files (CSV/JSON):** `report_artists_albumartist.csv`, `report_artists_albumartist_fuzzy.csv`, `report_artists_artist.csv`, `report_artists_artist_fuzzy.csv`

---

#### Report 11 — Genre analysis (`report genres`)

**Query logic:**
Fetch all `GENRE` tag values from `raw_tags` where `tag_key_upper = 'GENRE'`. Because GENRE can be multi-value (multiple rows for the same file), count both distinct file occurrences and distinct value occurrences. Apply same fuzzy matching logic as Report 10 (same thresholds).

**Full report tables:**
1. All distinct `GENRE` values: value / file count / % of collection. Rendered as a horizontal bar chart (top 30) in terminal output. Full list in CSV.
2. Genre fuzzy similarity pairs: same structure and caps as Report 10.
3. Files with no `GENRE` tag: count and %, grouped by `ALBUMARTIST` (top 20 artists by missing count).
4. Files with multiple `GENRE` tag instances: count. List the distinct value combinations found (e.g. some files have both `"Rock"` and `"Alternative"`).

**Summary data:** Total distinct genres, count above threshold for fuzzy pairs, % of files with no genre.

**Output files (CSV/JSON):** `report_genres_values.csv`, `report_genres_fuzzy.csv`, `report_genres_missing.csv`

---

#### Report 12 — Lyrics coverage (`report lyrics`)

**Query logic:**
Query `raw_tags` for `tag_key_upper = 'LYRICS'`. Separately check for `tag_key_upper = 'UNSYNCEDLYRICS'` (an alternate key some taggers use). Treat either as "has embedded lyrics".

**Full report tables:**
1. Overall coverage: files with non-empty `LYRICS` or `UNSYNCEDLYRICS` / files with the tag but empty value / files with no lyrics tag. Count and %.
2. Coverage by artist: for each `ALBUMARTIST`, count of tracks with lyrics / total tracks / %. Sorted by % ascending (worst coverage first). In terminal output, show bottom 20 artists; full list in CSV.
3. Coverage by album: same, grouped by album directory path. Sorted by % ascending. In terminal output, show bottom 30 albums; full list in CSV.
4. Key variant note: count of files using `UNSYNCEDLYRICS` vs `LYRICS` — this informs whether alias normalisation is needed in Phase 4.

**Summary data:** Overall coverage %, count of artists with zero lyrics coverage.

**Output files (CSV/JSON):** `report_lyrics_overall.csv`, `report_lyrics_by_artist.csv`, `report_lyrics_by_album.csv`

---

#### Report 13 — ReplayGain coverage (`report replaygain`)

**Query logic:**
Query `raw_tags` for the four ReplayGain tag keys. Parse gain values (float from strings like `-6.23 dB` — strip the ` dB` suffix). Flag values outside the range -20.0 dB to +10.0 dB as outliers.

**Full report tables:**
1. Coverage for all four tags: `REPLAYGAIN_TRACK_GAIN` / `REPLAYGAIN_TRACK_PEAK` / `REPLAYGAIN_ALBUM_GAIN` / `REPLAYGAIN_ALBUM_PEAK` — count with valid value / count with malformed value (present but not parseable as a float + optional " dB") / count absent. Count and %.
2. Partially-tagged albums: albums where some tracks have album-level ReplayGain tags (`REPLAYGAIN_ALBUM_GAIN`, `REPLAYGAIN_ALBUM_PEAK`) and others don't. Partial album ReplayGain is worse than none — it produces inconsistent playback volume within a single album. List: album path / tracks with album RG / tracks without / total tracks.
3. Track gain value distribution: histogram of `REPLAYGAIN_TRACK_GAIN` values in 2 dB buckets from -20 to +10. Bar chart in terminal output.
4. Outliers: tracks where the parsed gain value is outside [-20.0, +10.0] dB. List with path and value. These almost certainly indicate a calculation error.

**Summary data:** Track-level coverage %, album-level coverage %, count of partially-tagged albums, count of outlier values.

**Output files (CSV/JSON):** `report_replaygain_coverage.csv`, `report_replaygain_partial_albums.csv`, `report_replaygain_outliers.csv`

---

#### What Sprint 2 does NOT do
- No writes to the database (read-only throughout — `scan_issues` is read but not written)
- No writes to any file except report output files in `--output-dir`
- No calls to MusicBrainz, Discogs, AcoustID, LRCLIB, or any external service
- No tag normalisation or fix decisions
- No Discogs ID coverage reporting — Discogs is not yet an enrichment source; add to a Sprint 2 revision once Sprint 3 adds Discogs IDs to the collection
- `--album` without `--artist` is rejected with a clear error message
- `summary` does not accept `--output` — terminal only

#### New dependencies added in Sprint 2
- `rich` — terminal tables, progress bars, colour output
- `jinja2` — HTML report templating
- `difflib` — built-in; used for fuzzy name matching (no additional install)

---

### Sprint 3 — MusicBrainz & Discogs integration
MBID extraction from existing tags, AcoustID fingerprinting pipeline (batch, resumable, rate-limit-aware), MusicBrainz release lookup and data import, Discogs release cross-reference and ID enrichment, human review queue with match confidence scoring. **Discogs ID coverage reporting is deferred to a Sprint 2 revision after this sprint adds Discogs IDs to the collection.**

### Sprint 4 — Normalisation engine
Tag normaliser (DB only), alias → canonical name mapping, artist name resolver using MBIDs, date standardiser, genre normaliser, duplicate tag collapser. Issues surfaced by Sprint 2 reports get written to `scan_issues` here after human review. No file writes.

### Sprint 5 — File writers
Tag writer (DB → FLAC, with dry-run and write verification), file renamer (path generation, conflict and length validation, execution), DB path updater post-rename, rollback tooling.

### Sprint 6 — Enrichment
Cover art fetcher and writer, LRC fetcher and writer, ReplayGain calculator.

### Sprint 7 — Verification & maintenance
Post-write verification pass, checksum validator, inbox watcher / new album processor, full pipeline orchestrator.

---

## Important design decisions and constraints

1. **Tags are authoritative.** Folder/file names were generated from tags. The path structure is a consequence of the tags, not an independent source of truth.

2. **Raw tags are immutable.** Once written to `raw_tags` at import, those rows are never updated or deleted. They are the permanent record of what was in the files before any cleanup. All normalised/cleaned tag values will live in a separate table introduced in Phase 4.

3. **DB-first, files-last.** Every change is staged and validated in the database before anything is written to a FLAC file. The database is the source of truth for the target state; files are the output.

4. **Never delete.** Quarantine-flagged files are moved outside the collection, never deleted. This applies to any automated process.

5. **Resumability.** Any long-running batch operation (especially AcoustID fingerprinting at 40,000 tracks) must be resumable from where it left off. Use the database to checkpoint progress.

6. **Rate limits.** MusicBrainz: 1 req/sec without authentication (more with). AcoustID and LRCLIB also have rate limits. All API-calling scripts must implement exponential backoff.

7. **Multi-value tags.** Vorbis comments allow multiple values for the same key. The `raw_tags` table handles this with `tag_index`. The canonical schema decision (multiple tag instances vs. semicolon-delimited single tag) is deferred to Phase 4 normalisation, but the importer must preserve all values.

8. **The `v_common_tags` view uses MAX() aggregation** for multi-value tags — it returns one arbitrarily chosen value. This is intentional and documented. Always use `raw_tags` directly when multi-value correctness matters.

9. **Windows path compatibility.** Even if the current host is Linux/macOS, the collection may be accessed from Windows (e.g. via Samba). Characters illegal on Windows (`\ / : * ? " < > |`) must be stripped or replaced during Phase 5 renaming. Path length must stay under 240 characters.

10. **`duration_seconds` precision matters.** Store as `REAL` in SQLite (IEEE 754 double, ~15 significant digits). Both AcoustID and LRCLIB use duration as a disambiguation signal and are sensitive to rounding — do not cast to integer or truncate when reading from mutagen.

11. **Album boundaries in reporting use directory path, not tag values.** When grouping tracks into albums for consistency or numbering checks, use the parent directory path of the FLAC file, not the `ALBUMARTIST + ALBUM` tag combination. Tag-based grouping conflates distinct releases (e.g. original and remaster) that share album name and artist but are different directories.


    "COMPOSER",
    "LYRICIST",
    "CONDUCTOR",
    "PERFORMER",
    "ENSEMBLE",
    "OPUS",
    "PART",
    "MOVEMENT",
    "WORK",
    "SUBTITLE",
    "GROUPING",
    "MOOD",
    "BPM",
    "KEY",
    "LANGUAGE",
    "SCRIPT",
    "REPLAYGAIN_REFERENCE_LOUDNESS",
    "ACCURATERIPCRC",
    "ACCURATERIPCOUNT",
    "ACCURATERIPRESULT",
    "ACCURATERIPDISCID",
    "ENCODEDBY",
    "ENCODING",
    "ENCODERSETTINGS",
    "SOURCE",
    "SOURCEMEDIA",
