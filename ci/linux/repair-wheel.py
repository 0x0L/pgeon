# /usr/bin/env python
"""Ignore libarrow libraries in the wheel"""

import logging
import sys
from fnmatch import fnmatch

from auditwheel.patcher import Patchelf
from auditwheel.policy import (
    POLICY_PRIORITY_HIGHEST,
    POLICY_PRIORITY_LOWEST,
    get_policy_by_name,
    get_policy_name,
)
from auditwheel.repair import repair_wheel
from auditwheel.wheel_abi import analyze_wheel_abi

logging.basicConfig(level=logging.INFO)


EXCLUDE_PATTERN = "libarrow*"

if __name__ == "__main__":
    wheel, dest_dir = sys.argv[1:]

    policy_name = get_policy_name(POLICY_PRIORITY_HIGHEST)
    if policy_name is None:
        raise ValueError("Invalid policy")

    policy = get_policy_by_name(policy_name)
    if policy is None:
        raise ValueError("Invalid policy")

    winfo = analyze_wheel_abi(wheel)
    libs = winfo.external_refs[get_policy_name(POLICY_PRIORITY_LOWEST)]["libs"]

    excludes = []
    for lib in libs:
        if fnmatch(lib, EXCLUDE_PATTERN):
            excludes.append(lib)

    abis = [policy["name"]] + policy["aliases"]

    repair_wheel(
        wheel,
        abis,
        out_dir=dest_dir,
        lib_sdir=".libs",
        update_tags=True,
        patcher=Patchelf(),
        exclude=excludes,
    )
