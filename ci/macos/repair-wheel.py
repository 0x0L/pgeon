#!/usr/bin/env python
"""Ignore libarrow libraries in the wheel"""
import logging
import sys
from fnmatch import fnmatch
from pathlib import Path

import delocate.tools
from delocate import delocate_wheel
from delocate.delocating import filter_system_libs
from delocate.tools import _parse_otool_install_names as _parse_names

logging.basicConfig(level=logging.INFO)

EXCLUDE_PATTERN = "*libarrow*"


# Patching otool search to exclude libraries we don't want parsed
def parse_otool_install_names(stdout: str) -> dict[str, list[tuple[str, str, str]]]:
    names = _parse_names(stdout)

    for k, v in names.copy().items():
        names[k] = [t for t in v if not (fnmatch(t[0], EXCLUDE_PATTERN))]

    return names


delocate.tools._parse_otool_install_names = parse_otool_install_names


if __name__ == "__main__":
    wheel, dest_dir, delocate_archs = sys.argv[1:]

    wheel = Path(wheel)
    dest_dir = Path(dest_dir)
    require_archs = [s.strip() for s in delocate_archs.split(",")]

    delocate_wheel(
        in_wheel=wheel.as_posix(),
        out_wheel=(dest_dir / wheel.name).as_posix(),
        require_archs=require_archs,
    )
