#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""Resolve Maven modules impacted by Sonar-analyzable PR diff changes.

SonarCloud for apache_iotdb analyzes main Java sources (`src/main/java/**` and
`src/main/i18n/en/**`) in the default Maven reactor excluding `distribution`
(and excluding profile-only modules such as `integration-test`, `code-coverage`,
`iotdb-client/client-cpp`, and `iotdb-core/ainode`).

Given a base Git ref and head ref, this script inspects the three-dot PR diff
(`<base>...<head>`), maps changed analyzable Java files to their enclosing
Maven modules, and emits `run_sonar` and `modules` outputs for GitHub Actions.
"""

from __future__ import annotations

import argparse
from pathlib import Path, PurePosixPath
import subprocess
import sys
import xml.etree.ElementTree as ET

EXCLUDED_MODULES = {"distribution"}
ANALYZABLE_SOURCE_PREFIXES = ("src/main/java/", "src/main/i18n/en/")
MAVEN_NS = "{http://maven.apache.org/POM/4.0.0}"


def discover_reactor_modules(repo_root: Path) -> set[str]:
    """Walk default `<modules>` from the root pom.xml, excluding non-Sonar modules."""
    modules: set[str] = set()

    def walk(rel_dir: PurePosixPath) -> None:
        rel_str = "." if rel_dir == PurePosixPath(".") else rel_dir.as_posix()
        if rel_str in EXCLUDED_MODULES:
            return
        pom_path = repo_root / rel_str / "pom.xml" if rel_str != "." else repo_root / "pom.xml"
        if not pom_path.is_file():
            return
        modules.add(rel_str)

        tree = ET.parse(pom_path)
        root = tree.getroot()
        ns = MAVEN_NS if root.tag.startswith(MAVEN_NS) else ""
        modules_elem = root.find(f"{ns}modules")
        if modules_elem is None:
            return
        for child in modules_elem.findall(f"{ns}module"):
            if not child.text:
                continue
            child_name = child.text.strip()
            if not child_name:
                continue
            child_rel = (
                PurePosixPath(child_name)
                if rel_str == "."
                else rel_dir / PurePosixPath(child_name)
            )
            walk(child_rel)

    walk(PurePosixPath("."))
    modules.discard(".")
    return modules


def find_enclosing_module(rel_path: str, eligible_modules: set[str]) -> str | None:
    """Return the nearest enclosing eligible Maven module for a repo-relative path."""
    parent = PurePosixPath(rel_path).parent
    while str(parent) not in ("", "."):
        candidate = parent.as_posix()
        if candidate in eligible_modules:
            return candidate
        parent = parent.parent
    return None


def is_analyzable_source(rel_path: str, module_dir: str) -> bool:
    """Check whether `rel_path` is a main Java source indexed by SonarCloud."""
    if not rel_path.endswith(".java"):
        return False
    prefix = f"{module_dir}/"
    if not rel_path.startswith(prefix):
        return False
    module_rel = rel_path[len(prefix) :]
    return module_rel.startswith(ANALYZABLE_SOURCE_PREFIXES)


def select_modules_for_files(
    changed_files: list[str],
    repo_root: Path,
    eligible_modules: set[str],
    *,
    require_existing: bool = True,
) -> tuple[list[str], list[str]]:
    """Map changed repo-relative paths to sorted Sonar-analyzable files and modules."""
    analyzable_files: list[str] = []
    selected_modules: set[str] = set()

    for raw_path in changed_files:
        rel_path = raw_path.replace("\\", "/")
        while rel_path.startswith("./"):
            rel_path = rel_path[2:]
        if not rel_path:
            continue
        if require_existing and not (repo_root / rel_path).is_file():
            continue
        module_dir = find_enclosing_module(rel_path, eligible_modules)
        if module_dir is None:
            continue
        if is_analyzable_source(rel_path, module_dir):
            analyzable_files.append(rel_path)
            selected_modules.add(module_dir)

    return sorted(analyzable_files), sorted(selected_modules)


def git_changed_files(repo_root: Path, base_ref: str, head_ref: str) -> tuple[str, list[str]]:
    """Verify merge-base and return (merge_base_sha, changed_files) for `<base>...<head>`."""
    merge_base = subprocess.check_output(
        ["git", "merge-base", base_ref, head_ref],
        cwd=repo_root,
        text=True,
    ).strip()

    diff_output = subprocess.check_output(
        [
            "git",
            "-c",
            "core.quotepath=false",
            "diff",
            "-z",
            "--name-only",
            "--diff-filter=ACMR",
            f"{base_ref}...{head_ref}",
        ],
        cwd=repo_root,
    )
    files = [entry.decode("utf-8", errors="surrogateescape") for entry in diff_output.split(b"\0") if entry]
    return merge_base, files


def write_github_outputs(output_path: Path, run_sonar: bool, modules: list[str]) -> None:
    with output_path.open("a", encoding="utf-8") as fh:
        fh.write(f"run_sonar={'true' if run_sonar else 'false'}\n")
        fh.write(f"modules={','.join(modules)}\n")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Select Maven modules for PR-diff SonarCloud analysis."
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="Path to the repository root.",
    )
    parser.add_argument(
        "--base-ref",
        required=True,
        help="Git base ref to diff against (e.g. refs/remotes/origin/master).",
    )
    parser.add_argument(
        "--head-ref",
        default="HEAD",
        help="Git head ref to diff (default: HEAD).",
    )
    parser.add_argument(
        "--github-output",
        type=Path,
        default=None,
        help="Optional path to $GITHUB_OUTPUT file.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    repo_root = args.repo_root.resolve()

    eligible_modules = discover_reactor_modules(repo_root)
    merge_base, changed_files = git_changed_files(repo_root, args.base_ref, args.head_ref)
    analyzable_files, selected_modules = select_modules_for_files(
        changed_files,
        repo_root,
        eligible_modules,
        require_existing=(args.head_ref == "HEAD"),
    )

    print(f"Base ref: {args.base_ref}")
    print(f"Merge base: {merge_base}")
    print(f"Changed files in PR diff (ACMR): {len(changed_files)}")
    print(f"Sonar-analyzable changed Java files: {len(analyzable_files)}")
    for path in analyzable_files[:50]:
        print(f"  - {path}")
    if len(analyzable_files) > 50:
        print(f"  ... and {len(analyzable_files) - 50} more")

    run_sonar = bool(selected_modules)
    if run_sonar:
        print(f"Selected Maven modules for SonarCloud (-pl): {','.join(selected_modules)}")
    else:
        print("No Sonar-analyzable main Java sources changed in this PR; skipping SonarCloud scan.")

    if args.github_output is not None:
        write_github_outputs(args.github_output, run_sonar, selected_modules)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
