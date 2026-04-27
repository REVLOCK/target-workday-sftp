#!/usr/bin/env python
"""setuptools entry; egg_info at repo root."""
from pathlib import Path

from setuptools import setup

ROOT = Path(__file__).resolve().parent

setup(options={"egg_info": {"egg_base": str(ROOT)}})
