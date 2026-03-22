-- storeroon — Phase 1 database schema
-- Applied by the migration runner; intended to be idempotent (IF NOT EXISTS).

-- -----------------------------------------------------------------------
-- files — one row per FLAC file, the stable identity anchor
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS files (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    path            TEXT    NOT NULL UNIQUE,
    filename        TEXT    NOT NULL,
    size_bytes      INTEGER NOT NULL,
    checksum_sha256 TEXT,
    mtime_on_disk   TEXT    NOT NULL,
    status          TEXT    NOT NULL DEFAULT 'ok'
                        CHECK (status IN ('ok', 'unreadable', 'missing')),
    imported_at     TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now')),
    last_scanned_at TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_files_checksum ON files (checksum_sha256)
    WHERE checksum_sha256 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_files_status ON files (status)
    WHERE status != 'ok';

-- -----------------------------------------------------------------------
-- flac_properties — STREAMINFO block data, one row per file
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS flac_properties (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id           INTEGER NOT NULL UNIQUE
                          REFERENCES files (id) ON DELETE CASCADE,
    duration_seconds  REAL,
    sample_rate_hz    INTEGER,
    bits_per_sample   INTEGER,
    channels          INTEGER,
    total_samples     INTEGER,
    audio_md5         TEXT,
    vendor_string     TEXT,
    approx_bitrate_kbps INTEGER
);

-- -----------------------------------------------------------------------
-- raw_tags — verbatim Vorbis comment data (IMMUTABLE after import)
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw_tags (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id          INTEGER NOT NULL
                         REFERENCES files (id) ON DELETE CASCADE,
    imported_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now')),
    tag_key          TEXT    NOT NULL,
    tag_key_upper    TEXT    NOT NULL,
    tag_value        TEXT    NOT NULL DEFAULT '',
    tag_index        INTEGER NOT NULL DEFAULT 0,
    encoding_suspect INTEGER NOT NULL DEFAULT 0,
    raw_bytes_hex    TEXT
);

CREATE INDEX IF NOT EXISTS idx_raw_tags_file_id ON raw_tags (file_id);

CREATE INDEX IF NOT EXISTS idx_raw_tags_key_upper ON raw_tags (tag_key_upper);

CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_tags_file_key_idx
    ON raw_tags (file_id, tag_key_upper, tag_index);

-- -----------------------------------------------------------------------
-- scan_issues — quality flags and anomalies
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS scan_issues (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id           INTEGER
                          REFERENCES files (id) ON DELETE CASCADE,
    issue_type        TEXT    NOT NULL,
    severity          TEXT    NOT NULL DEFAULT 'warning'
                          CHECK (severity IN ('info', 'warning', 'error', 'critical')),
    description       TEXT    NOT NULL DEFAULT '',
    details           TEXT,
    detected_at       TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now')),
    resolved          INTEGER NOT NULL DEFAULT 0,
    resolved_at       TEXT,
    resolution_method TEXT
);

CREATE INDEX IF NOT EXISTS idx_scan_issues_file_id ON scan_issues (file_id)
    WHERE file_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_scan_issues_type ON scan_issues (issue_type);

CREATE INDEX IF NOT EXISTS idx_scan_issues_unresolved ON scan_issues (resolved)
    WHERE resolved = 0;

-- -----------------------------------------------------------------------
-- lyrics_analysis — scan-time lyrics classification
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS lyrics_analysis (
    file_id   INTEGER PRIMARY KEY REFERENCES files (id) ON DELETE CASCADE,
    embedded  TEXT NOT NULL DEFAULT 'absent'
                  CHECK (embedded IN ('synced', 'unsynced', 'absent')),
    sidecar   TEXT NOT NULL DEFAULT 'absent'
                  CHECK (sidecar IN ('synced', 'unsynced', 'absent'))
);

-- -----------------------------------------------------------------------
-- v_common_tags — pivot view for ad-hoc analysis
--
-- Uses MAX() aggregation on multi-value tags: returns ONE arbitrarily
-- chosen value.  For multi-value correctness, query raw_tags directly.
-- -----------------------------------------------------------------------

CREATE VIEW IF NOT EXISTS v_common_tags AS
SELECT
    f.id                    AS file_id,
    f.path                  AS path,
    f.filename              AS filename,
    f.size_bytes            AS size_bytes,
    f.checksum_sha256       AS checksum_sha256,
    f.status                AS status,

    -- flac_properties
    fp.duration_seconds     AS duration_seconds,
    fp.sample_rate_hz       AS sample_rate_hz,
    fp.bits_per_sample      AS bits_per_sample,
    fp.channels             AS channels,
    fp.total_samples        AS total_samples,
    fp.audio_md5            AS audio_md5,
    fp.vendor_string        AS vendor_string,
    fp.approx_bitrate_kbps  AS approx_bitrate_kbps,

    -- Core identification tags
    MAX(CASE WHEN rt.tag_key_upper = 'TITLE'       THEN rt.tag_value END) AS title,
    MAX(CASE WHEN rt.tag_key_upper = 'ARTIST'      THEN rt.tag_value END) AS artist,
    MAX(CASE WHEN rt.tag_key_upper = 'ALBUMARTIST'  THEN rt.tag_value END) AS albumartist,
    MAX(CASE WHEN rt.tag_key_upper = 'ALBUM ARTIST' THEN rt.tag_value END) AS albumartist_legacy,
    MAX(CASE WHEN rt.tag_key_upper = 'ALBUM'       THEN rt.tag_value END) AS album,
    MAX(CASE WHEN rt.tag_key_upper = 'DATE'        THEN rt.tag_value END) AS date,
    MAX(CASE WHEN rt.tag_key_upper = 'YEAR'        THEN rt.tag_value END) AS year_legacy,
    MAX(CASE WHEN rt.tag_key_upper = 'ORIGINALDATE' THEN rt.tag_value END) AS originaldate,
    MAX(CASE WHEN rt.tag_key_upper = 'ORIGINALYEAR' THEN rt.tag_value END) AS originalyear,

    -- Track / disc numbering
    MAX(CASE WHEN rt.tag_key_upper = 'TRACKNUMBER'  THEN rt.tag_value END) AS tracknumber,
    MAX(CASE WHEN rt.tag_key_upper = 'TRACKTOTAL'   THEN rt.tag_value END) AS tracktotal_legacy,
    MAX(CASE WHEN rt.tag_key_upper = 'TOTALTRACKS'  THEN rt.tag_value END) AS totaltracks,
    MAX(CASE WHEN rt.tag_key_upper = 'DISCNUMBER'   THEN rt.tag_value END) AS discnumber,
    MAX(CASE WHEN rt.tag_key_upper = 'DISCTOTAL'    THEN rt.tag_value END) AS disctotal_legacy,
    MAX(CASE WHEN rt.tag_key_upper = 'TOTALDISCS'   THEN rt.tag_value END) AS totaldiscs,
    MAX(CASE WHEN rt.tag_key_upper = 'DISCSUBTITLE' THEN rt.tag_value END) AS discsubtitle,

    -- Genre / style
    MAX(CASE WHEN rt.tag_key_upper = 'GENRE'       THEN rt.tag_value END) AS genre,
    MAX(CASE WHEN rt.tag_key_upper = 'STYLE'       THEN rt.tag_value END) AS style,

    -- MusicBrainz IDs
    MAX(CASE WHEN rt.tag_key_upper = 'MUSICBRAINZ_TRACKID'         THEN rt.tag_value END) AS musicbrainz_trackid,
    MAX(CASE WHEN rt.tag_key_upper = 'MUSICBRAINZ_ALBUMID'         THEN rt.tag_value END) AS musicbrainz_albumid,
    MAX(CASE WHEN rt.tag_key_upper = 'MUSICBRAINZ_ARTISTID'        THEN rt.tag_value END) AS musicbrainz_artistid,
    MAX(CASE WHEN rt.tag_key_upper = 'MUSICBRAINZ_ALBUMARTISTID'   THEN rt.tag_value END) AS musicbrainz_albumartistid,
    MAX(CASE WHEN rt.tag_key_upper = 'MUSICBRAINZ_RELEASEGROUPID'  THEN rt.tag_value END) AS musicbrainz_releasegroupid,
    MAX(CASE WHEN rt.tag_key_upper = 'MUSICBRAINZ_RELEASETRACKID'  THEN rt.tag_value END) AS musicbrainz_releasetrackid,
    MAX(CASE WHEN rt.tag_key_upper = 'MUSICBRAINZ_WORKID'          THEN rt.tag_value END) AS musicbrainz_workid,
    MAX(CASE WHEN rt.tag_key_upper = 'ACOUSTID_ID'                 THEN rt.tag_value END) AS acoustid_id,

    -- Release metadata
    MAX(CASE WHEN rt.tag_key_upper = 'LABEL'         THEN rt.tag_value END) AS label,
    MAX(CASE WHEN rt.tag_key_upper = 'CATALOGNUMBER'  THEN rt.tag_value END) AS catalognumber,
    MAX(CASE WHEN rt.tag_key_upper = 'BARCODE'       THEN rt.tag_value END) AS barcode,
    MAX(CASE WHEN rt.tag_key_upper = 'ISRC'          THEN rt.tag_value END) AS isrc,
    MAX(CASE WHEN rt.tag_key_upper = 'MEDIA'         THEN rt.tag_value END) AS media,
    MAX(CASE WHEN rt.tag_key_upper = 'RELEASETYPE'   THEN rt.tag_value END) AS releasetype,
    MAX(CASE WHEN rt.tag_key_upper = 'RELEASESTATUS'  THEN rt.tag_value END) AS releasestatus,
    MAX(CASE WHEN rt.tag_key_upper = 'RELEASECOUNTRY' THEN rt.tag_value END) AS releasecountry,

    -- People / credits
    MAX(CASE WHEN rt.tag_key_upper = 'COMPOSER'      THEN rt.tag_value END) AS composer,
    MAX(CASE WHEN rt.tag_key_upper = 'CONDUCTOR'     THEN rt.tag_value END) AS conductor,
    MAX(CASE WHEN rt.tag_key_upper = 'PERFORMER'     THEN rt.tag_value END) AS performer,
    MAX(CASE WHEN rt.tag_key_upper = 'LYRICIST'      THEN rt.tag_value END) AS lyricist,
    MAX(CASE WHEN rt.tag_key_upper = 'REMIXER'       THEN rt.tag_value END) AS remixer,
    MAX(CASE WHEN rt.tag_key_upper = 'PRODUCER'      THEN rt.tag_value END) AS producer,
    MAX(CASE WHEN rt.tag_key_upper = 'ENGINEER'      THEN rt.tag_value END) AS engineer,
    MAX(CASE WHEN rt.tag_key_upper = 'MIXER'         THEN rt.tag_value END) AS mixer,

    -- Lyrics / comments
    MAX(CASE WHEN rt.tag_key_upper = 'LYRICS'        THEN rt.tag_value END) AS lyrics,
    MAX(CASE WHEN rt.tag_key_upper = 'COMMENT'       THEN rt.tag_value END) AS comment,
    MAX(CASE WHEN rt.tag_key_upper = 'DESCRIPTION'   THEN rt.tag_value END) AS description,
    MAX(CASE WHEN rt.tag_key_upper = 'COPYRIGHT'     THEN rt.tag_value END) AS copyright,

    -- ReplayGain
    MAX(CASE WHEN rt.tag_key_upper = 'REPLAYGAIN_TRACK_GAIN' THEN rt.tag_value END) AS replaygain_track_gain,
    MAX(CASE WHEN rt.tag_key_upper = 'REPLAYGAIN_TRACK_PEAK' THEN rt.tag_value END) AS replaygain_track_peak,
    MAX(CASE WHEN rt.tag_key_upper = 'REPLAYGAIN_ALBUM_GAIN' THEN rt.tag_value END) AS replaygain_album_gain,
    MAX(CASE WHEN rt.tag_key_upper = 'REPLAYGAIN_ALBUM_PEAK' THEN rt.tag_value END) AS replaygain_album_peak,

    -- Derived helpers
    COUNT(DISTINCT rt.id)  AS total_tag_count,
    COALESCE(
        (SELECT 1 FROM scan_issues si
         WHERE si.file_id = f.id AND si.resolved = 0
         LIMIT 1),
        0
    ) AS has_open_issues

FROM files f
LEFT JOIN flac_properties fp ON fp.file_id = f.id
LEFT JOIN raw_tags rt        ON rt.file_id = f.id
GROUP BY f.id;
