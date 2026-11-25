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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Map OWASP Dependency-Check SARIF findings to POMs in the checkout."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
from urllib.parse import quote
import xml.etree.ElementTree as ET


def pom_paths_by_project():
    tracked_poms = subprocess.check_output(
        ["git", "ls-files", "-z", "--", "pom.xml", "**/pom.xml"], text=True
    )
    paths = {}
    for path in filter(None, tracked_poms.split("\0")):
        project = ET.parse(path).getroot()
        name = project.findtext("{*}name") or project.findtext("{*}artifactId")
        paths.setdefault(name, path)
    return paths


def prepare_sarif(dependencies, sarif):
    pom_paths = pom_paths_by_project()
    for run in sarif["runs"]:
        for rule in run["tool"]["driver"]["rules"]:
            tags = rule.setdefault("properties", {}).setdefault("tags", [])
            if "security" not in tags:
                tags.append("security")

        for result in run["results"]:
            location = result["locations"][0]["physicalLocation"]
            artifact = location["artifactLocation"]
            # Dependency-Check emits the same dependency order in JSON and SARIF.
            dependency = dependencies[artifact["index"]]
            if artifact["uri"] != f"file://{dependency['filePath']}":
                raise ValueError("Dependency-Check JSON and SARIF dependency locations differ")

            manifests = []
            for reference in dependency.get("projectReferences", []):
                path = pom_paths.get(reference) or pom_paths.get(reference.rsplit(":", 1)[0])
                if path:
                    manifests.append(path)
            location["artifactLocation"] = {"uri": quote(min(manifests, default="pom.xml"))}

            package_ids = (
                package["id"]
                for package in dependency.get("packages", [])
                if package["id"].startswith("pkg:maven/")
            )
            component = min(package_ids, default=dependency["fileName"])
            message = result["message"]["text"]
            result["message"]["text"] = f"Affected dependency {component}\n\n{message}"

            identity = json.dumps([result["ruleId"], component]).encode("utf-8")
            fingerprint = hashlib.sha256(identity).hexdigest()
            result["partialFingerprints"] = {"primaryLocationLineHash": f"{fingerprint}:1"}

        # The original artifacts refer to JARs in the runner's Maven cache.
        run.pop("artifacts", None)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dependency_report", type=Path)
    parser.add_argument("sarif_report", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()

    dependencies = json.loads(args.dependency_report.read_text(encoding="utf-8"))["dependencies"]
    sarif = json.loads(args.sarif_report.read_text(encoding="utf-8"))
    prepare_sarif(dependencies, sarif)
    args.output.write_text(json.dumps(sarif), encoding="utf-8")


if __name__ == "__main__":
    main()
