#!/usr/bin/env python3
"""Checks that the tests catch a planted bug (a mutant). Standard library only.

Copies the working tree to a temporary directory, runs each test target there
unmutated (it must pass, or the check proves nothing), applies the mutant patch
with `git apply`, and runs each target again: it must now fail. The copy is
deleted afterwards; the checkout itself is never modified.

  python scripts/mutation_check.py tests/mutants/capture_releases_after_spending.patch \\
      tests/test_holds.py::test_a_capture_at_the_funds_boundary_succeeds tests/test_invariants.py

Needs what the test suite needs: TALLY_TEST_DATABASE_URL, or Docker for Testcontainers.
Exit status: 0 if every target caught the mutant, 1 if one let it survive, 2 if the
check couldn't run (tests failing without the mutant, a patch that doesn't apply).
"""

import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IGNORED = (".git", ".venv", "__pycache__", ".pytest_cache", ".hypothesis", ".ruff_cache", "htmlcov", ".coverage*")


def pytest(copy: Path, target: str) -> tuple[int, float]:
    # PYTHONPATH puts the copy's package ahead of the checkout's editable install.
    env = {**os.environ, "PYTHONPATH": str(copy / "src")}
    command = [sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", "--no-cov", target]
    command.append("--hypothesis-profile=mutation")  # stop at the first failing sequence, no shrinking
    started = time.monotonic()
    result = subprocess.run(command, cwd=copy, env=env, capture_output=True, text=True)
    if result.returncode not in (0, 1):  # 2-5: interrupted, internal or usage error, nothing collected
        sys.exit(f"pytest {target} could not run (exit {result.returncode}):\n{result.stdout}{result.stderr}")
    return result.returncode, time.monotonic() - started


def main() -> int:
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    patch, targets = Path(sys.argv[1]).resolve(), sys.argv[2:]
    copy = Path(tempfile.mkdtemp(prefix="tally-mutant-")) / "tally"
    try:
        shutil.copytree(ROOT, copy, ignore=shutil.ignore_patterns(*IGNORED))
        imported = subprocess.run(
            [sys.executable, "-c", "import tally; print(tally.__file__)"],
            cwd=copy,
            env={**os.environ, "PYTHONPATH": str(copy / "src")},
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        if not Path(imported).is_relative_to(copy):
            print(f"the tests would import tally from {imported}, not the copy", file=sys.stderr)
            return 2

        for target in targets:
            code, seconds = pytest(copy, target)
            if code != 0:
                print(f"{target} fails without the mutant: fix that first", file=sys.stderr)
                return 2
            print(f"without mutant  passed  {seconds:5.1f} s  {target}")

        applied = subprocess.run(["git", "apply", str(patch)], cwd=copy, capture_output=True, text=True)
        if applied.returncode != 0:
            print(f"{patch.name} does not apply:\n{applied.stderr}", file=sys.stderr)
            return 2

        survived = []
        for target in targets:
            code, seconds = pytest(copy, target)
            print(f"with mutant     {'FAILED' if code else 'passed'}  {seconds:5.1f} s  {target}")
            if code == 0:
                survived.append(target)
    finally:
        shutil.rmtree(copy.parent, ignore_errors=True)

    if survived:
        print(f"\nmutant SURVIVED in {', '.join(survived)}")
        return 1
    print(f"\nmutant caught by every target ({patch.name}); temporary copy removed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
