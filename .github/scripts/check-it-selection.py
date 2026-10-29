#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""Fail when CI would silently skip an integration test.

Failsafe only runs the classes whose simple names match its default includes
(IT*, *IT, *ITCase), and every test profile sets `groups`, so a test method
only runs if it, its class or a superclass of its class has a @Category. The
sharded CI jobs pass the classes listed by build-it-shard-list.sh to failsafe,
so these lists must together cover every candidate class exactly once.

The Java sources are scanned, not compiled, so this check is fast enough to
run before the build.
"""

import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = REPO_ROOT / "integration-test" / "src" / "test" / "java"
SHARD_SCRIPT = REPO_ROOT / ".github" / "scripts" / "build-it-shard-list.sh"
# The shard count of the sharded CI jobs
SHARD_COUNT = 3

COMMENT_OR_LITERAL = re.compile(
    r'/\*.*?\*/|//[^\n]*|"(?:\\.|[^"\\\n])*"|\'(?:\\.|[^\'\\\n])*\'', re.S
)
PACKAGE = re.compile(r"\bpackage\s+([\w.]+)\s*;")
IMPORT = re.compile(r"\bimport\s+(?!static\b)([\w.]+?)(\.\*)?\s*;")
ANNOTATION = r"@[\w.]+(?:\s*\((?:[^()]|\([^()]*\))*\))?"
METHOD = re.compile(
    r"((?:" + ANNOTATION + r"\s*)+)"
    r"(?:(?:public|protected|private|static|final|synchronized|abstract)\s+)*"
    r"[\w.$<>\[\], ?]+\s+(\w+)\s*\("
)
TEST = re.compile(r"@(?:org\.junit\.)?Test\b")
IGNORE = re.compile(r"@(?:org\.junit\.)?Ignore\b")
CATEGORY = re.compile(r"@(?:org\.junit\.experimental\.categories\.)?Category\b")


def is_candidate(simple_name):
    """Whether failsafe's default includes match the class."""
    return (
        simple_name.startswith("IT")
        or simple_name.endswith("IT")
        or simple_name.endswith("ITCase")
    )


def strip_comments_and_literals(source):
    return COMMENT_OR_LITERAL.sub(
        lambda match: " " if match.group(0).startswith("/") else '""', source
    )


def skip_type_parameters(text, index):
    """Returns the index after the type parameters starting at index, if any."""
    while index < len(text) and text[index].isspace():
        index += 1
    if index >= len(text) or text[index] != "<":
        return index
    depth = 0
    while index < len(text):
        if text[index] == "<":
            depth += 1
        elif text[index] == ">":
            depth -= 1
            if depth == 0:
                return index + 1
        index += 1
    return index


class JavaClass:
    def __init__(self, path, simple_name, source):
        text = strip_comments_and_literals(source)
        package = PACKAGE.search(text)
        self.path = path
        self.simple_name = simple_name
        self.name = (package.group(1) + "." if package else "") + simple_name
        self.package = package.group(1) if package else ""
        self.imports = {}
        self.wildcard_imports = []
        for match in IMPORT.finditer(text):
            if match.group(2):
                self.wildcard_imports.append(match.group(1))
            else:
                self.imports[match.group(1).rsplit(".", 1)[-1]] = match.group(1)
        self.is_class = False
        self.is_abstract = False
        self.has_category = False
        self.is_ignored = False
        self.superclass = None
        # test method name -> (has a category, is ignored)
        self.tests = {}
        declaration = re.search(
            r"\b(class|interface|enum|record)\s+" + re.escape(simple_name) + r"\b",
            text,
        )
        if not declaration or declaration.group(1) != "class":
            return
        self.is_class = True
        # Annotations and modifiers follow the last import or the package statement.
        header = text[text.rfind(";", 0, declaration.start()) + 1 : declaration.start()]
        self.is_abstract = re.search(r"\babstract\b", header) is not None
        self.has_category = CATEGORY.search(header) is not None
        self.is_ignored = IGNORE.search(header) is not None
        rest = skip_type_parameters(text, declaration.end())
        extends = re.match(r"\s*extends\s+([\w.]+)", text[rest:])
        self.superclass = extends.group(1) if extends else None
        body = text[text.find("{", rest) + 1 :]
        for match in METHOD.finditer(body):
            annotations = match.group(1)
            if TEST.search(annotations):
                self.tests.setdefault(
                    match.group(2),
                    (
                        CATEGORY.search(annotations) is not None,
                        IGNORE.search(annotations) is not None,
                    ),
                )

    def resolve(self, name, classes):
        """Returns the scanned class that a type name in this file refers to, if any."""
        if "." in name:
            return classes.get(name)
        if name in self.imports:
            return classes.get(self.imports[name])
        for package in [self.package] + self.wildcard_imports:
            candidate = classes.get((package + "." if package else "") + name)
            if candidate:
                return candidate
        return None


def scan_sources():
    classes = {}
    for directory, _, files in os.walk(SOURCE_ROOT):
        for file in files:
            if file.endswith(".java"):
                path = Path(directory) / file
                java_class = JavaClass(
                    path, file[: -len(".java")], path.read_text(encoding="utf-8")
                )
                classes[java_class.name] = java_class
    return classes


def check_categories(classes):
    """Returns the number of concrete test classes and the errors of those that failsafe
    would not run."""
    test_class_count = 0
    errors = []
    for java_class in sorted(classes.values(), key=lambda c: c.name):
        if not java_class.is_class or java_class.is_abstract or java_class.is_ignored:
            continue
        # JUnit runs the test methods of the superclasses too, and @Category is @Inherited.
        has_category = False
        tests = {}
        current, visited = java_class, set()
        while current and current.name not in visited:
            visited.add(current.name)
            has_category = has_category or current.has_category
            for name, test in current.tests.items():
                tests.setdefault(name, test)
            current = (
                current.resolve(current.superclass, classes)
                if current.superclass
                else None
            )
        if not tests:
            continue
        test_class_count += 1
        path = java_class.path.relative_to(REPO_ROOT)
        if not is_candidate(java_class.simple_name):
            errors.append(
                (
                    path,
                    f"{java_class.name} has tests, but failsafe only runs classes named"
                    " IT*, *IT or *ITCase",
                )
            )
        elif not has_category:
            skipped = sorted(
                name
                for name, (category, ignored) in tests.items()
                if not category and not ignored
            )
            if skipped:
                errors.append(
                    (
                        path,
                        f"no test profile runs {java_class.name}#{', #'.join(skipped)}:"
                        " add a @Category to the class or to these methods",
                    )
                )
    return test_class_count, errors


def check_shards(classes):
    """Returns the errors of candidate classes that are not in exactly one shard."""
    candidates = {
        java_class.name
        for java_class in classes.values()
        if is_candidate(java_class.simple_name)
    }
    shards = {}
    errors = []
    with tempfile.TemporaryDirectory() as directory:
        for shard in range(SHARD_COUNT):
            output = Path(directory) / f"shard-{shard}.txt"
            subprocess.run(
                ["bash", str(SHARD_SCRIPT), str(shard), str(SHARD_COUNT), str(output)],
                check=True,
                stdout=subprocess.DEVNULL,
            )
            for name in output.read_text(encoding="utf-8").split():
                if name in shards:
                    errors.append(
                        (
                            SHARD_SCRIPT.relative_to(REPO_ROOT),
                            f"{name} is in shards {shards[name]} and {shard}",
                        )
                    )
                shards[name] = shard
    for name in sorted(candidates - shards.keys()):
        errors.append(
            (classes[name].path.relative_to(REPO_ROOT), f"{name} is in no shard")
        )
    for name in sorted(shards.keys() - candidates):
        errors.append(
            (SHARD_SCRIPT.relative_to(REPO_ROOT), f"{name} is not a candidate class")
        )
    return errors


def main():
    classes = scan_sources()
    test_class_count, errors = check_categories(classes)
    errors += check_shards(classes)
    for path, message in errors:
        if os.environ.get("GITHUB_ACTIONS") == "true":
            print(f"::error file={path}::{message}")
        else:
            print(f"{path}: {message}")
    if errors:
        return 1
    print(
        f"All {test_class_count} test classes have a failsafe class name and a @Category,"
        f" and the {SHARD_COUNT} shard lists cover every candidate class once."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
