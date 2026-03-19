"""
storeroon.reports — analysis and reporting layer.

This package provides read-only analysis of the collection database,
producing reports across 13 areas: overview, technical quality, tag
coverage, tag formats, album consistency, external IDs, duplicates,
scan issues, artist consistency, genres, lyrics, and ReplayGain.

Architecture:
    Layer 1 — Query layer (``reports.queries.*``):
        Pure query functions returning typed dataclasses.
    Layer 2 — Renderer layer (``reports.renderers.*``):
        Terminal (Rich) for live output, JSON for file export.
        HTML section builders for the ``storeroon serve`` web server.
    Layer 3 — CLI layer (``reports.cli``):
        Wires queries and renderers together with CLI flags.
"""
