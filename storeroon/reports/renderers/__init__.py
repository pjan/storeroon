"""
storeroon.reports.renderers — output rendering layer.

Renderers accept ReportData instances and produce output. Renderers know
nothing about the database.

Modules:
    terminal — Rich terminal output (interactive use)
    json_renderer — JSON file output (stable filenames, envelope metadata)
    html_sections — Section builders for HTML rendering (used by the server)
"""
