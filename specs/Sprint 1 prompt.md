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

### Phase 2 — Quality analysis & normalisation (DB only)
- Define the canonical tag schema (which tags to keep, their names, their formats)
- Run quality analysis queries to understand the scope of problems before writing any fix rules
- Normalise tag values in the database: date formats, track number padding, artist name consistency, multi-value field handling, stripping non-canonical tags
- All changes at this stage are DB-only; files are not touched yet

### Phase 3 — Enrichment (DB only)
- Match releases to MusicBrainz: check for existing MBIDs in tags, then AcoustID fingerprinting, then fuzzy search; flag ambiguous matches for human review
- Pull full release metadata from MusicBrainz API (dates, label, catalog number, ISRC, etc.)
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

`duration_seconds` is stored at microsecond precision because it is used as a disambiguation signal in AcoustID fingerprint lookups and LRCLIB queries in Phase 3.

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

- **Language:** Python 3.12+
- **Database:** SQLite (the database is a single file; no server required)
- **Key libraries:**
  - `sqlite3` — built-in Python module; no additional driver needed
  - `mutagen` — FLAC tag reading and writing
  - `pyacoustid` + `fpcalc` (Chromaprint) — AcoustID fingerprinting in Phase 3
  - `musicbrainzngs` — MusicBrainz API client in Phase 3
  - `loudgain` — ReplayGain calculation in Phase 3 (CLI tool, called via subprocess)
  - `rich` — CLI framework for all scripts
- **Configuration:** single TOML config file read by every script; defines collection root path, database file path, canonical tag schema, API credentials, naming templates
- **All scripts must have a `--dry-run` flag** that prints what would change without writing anything

### SQLite-specific notes
- The schema uses `TEXT` instead of PostgreSQL's `JSONB` for the `details` column in `scan_issues`. Store as a JSON string; parse in Python with `json.loads()`. SQLite's `json_extract()` is available for in-database queries if needed (requires SQLite 3.38+).
- `duration_seconds` is stored as `REAL` (SQLite's floating-point type). SQLite is dynamically typed, so the `NUMERIC(12,6)` declaration in the schema file is advisory — values are stored as IEEE 754 doubles, which gives ~15 significant digits of precision, sufficient for microsecond-level duration accuracy.
- SQLite does not support `ALTER TABLE ... DROP COLUMN` cleanly on older versions. During active schema development (Sprint 1), simply delete the database file and re-run the migration to apply changes. Once real collection data is imported, treat schema changes more carefully.
- Enable WAL mode (`PRAGMA journal_mode=WAL`) and foreign key enforcement (`PRAGMA foreign_keys=ON`) at connection time. These are off by default in SQLite.

---

## Development plan — sprints

### Sprint 1 — Foundation (CURRENT SPRINT)
The goal of Sprint 1 is to build the infrastructure that everything else depends on. No enrichment, no writes to files.

**What to build:**
1. Project scaffolding: repo structure, dependency management (`pyproject.toml` or `requirements.txt`), config file loader
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

**What Sprint 1 does NOT do:**
- No tag normalisation
- No file writes of any kind
- No MusicBrainz or any external API calls
- No renaming

### Sprint 2 — Analysis & reporting
Quality reporter: for each field in the canonical tag schema, report coverage %, distinct values, obvious anomalies. All output only, no writes.

### Sprint 3 — MusicBrainz integration
MBID extraction, AcoustID fingerprinting pipeline (batch, resumable, rate-limit-aware), MusicBrainz release lookup, human review queue with match confidence scoring.

### Sprint 4 — Normalisation engine
Tag normaliser (DB only), artist name resolver, date standardiser, genre normaliser, duplicate tag collapser. No file writes.

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

7. **Multi-value tags.** Vorbis comments allow multiple values for the same key. The `raw_tags` table handles this with `tag_index`. The canonical schema decision (multiple tag instances vs. semicolon-delimited single tag) is deferred to Phase 2 normalisation, but the importer must preserve all values.

8. **The `v_common_tags` view uses MAX() aggregation** for multi-value tags — it returns one arbitrarily chosen value. This is intentional and documented. Always use `raw_tags` directly when multi-value correctness matters.

9. **Windows path compatibility.** Even if the current host is Linux/macOS, the collection may be accessed from Windows (e.g. via Samba). Characters illegal on Windows (`\ / : * ? " < > |`) must be stripped or replaced during Phase 5 renaming. Path length must stay under 240 characters.

10. **`duration_seconds` precision matters.** Store as `REAL` in SQLite (IEEE 754 double, ~15 significant digits). Both AcoustID and LRCLIB use duration as a disambiguation signal and are sensitive to rounding — do not cast to integer or truncate when reading from mutagen.
