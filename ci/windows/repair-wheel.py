#!/usr/bin/env python
"""Ignore libarrow libraries in the wheel"""
import logging
import re
import sys

from delvewheel._dll_list import ignore_regexes
from delvewheel._wheel_repair import WheelRepair

logging.basicConfig(level=logging.INFO)

ignore_regexes.add(re.compile(r"\S*arrow\S*"))

if __name__ == "__main__":
    wheel, dest_dir = sys.argv[1:]
    wr = WheelRepair(
        wheel,
        dest_dir,
        add_dlls=None,
        no_dlls=None,
        ignore_in_wheel=True,
        verbose=0,
        test=[""],
    )
    wr.repair(
        dest_dir, no_mangles=set(), no_mangle_all=True, lib_sdir=".libs", strip=True
    )
