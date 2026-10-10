"""Jinja globals for sqlfluff.

sqlfluff's built-in ``var`` stand-in cannot be used in arithmetic, and several models
compute with their variables. This one returns the real defaults from dbt_project.yml.
"""

from pathlib import Path

import yaml

_VARS = yaml.safe_load((Path(__file__).resolve().parents[2] / "dbt" / "dbt_project.yml").read_text())["vars"]


def var(name, default=None):
    return _VARS.get(name, default)
