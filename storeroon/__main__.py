"""
Allow running storeroon as a module: ``python -m storeroon``.
"""

from storeroon.cli import cli

if __name__ == "__main__":
    cli()
