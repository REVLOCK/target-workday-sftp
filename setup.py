#!/usr/bin/env python
"""setup.cfg drives build; egg_info forced to repo root (not under src/)."""
from pathlib import Path

from setuptools import setup

ROOT = Path(__file__).resolve().parent

setup(options={"egg_info": {"egg_base": str(ROOT)}})
