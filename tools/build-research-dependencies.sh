#!/usr/bin/env bash
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

set -euo pipefail

# Bootstrap the expired upstream snapshot from the matching, immutable Apache source.
# The checkout and build outputs live outside this repository and are removed on exit.
tsfile_revision=052ff6b44f68d3f2e04bfd7190067e4f5efb038c
tsfile_archive_sha256=0ab7bd249844d8293ab5d9f64010920ef32b9f78e48dad39bb1c479b598b4014
tsfile_build_dir=$(mktemp -d "${TMPDIR:-/tmp}/iotdb-tsfile.XXXXXX")
trap 'rm -rf "$tsfile_build_dir"' EXIT
curl --fail --location --retry 3 \
  "https://codeload.github.com/apache/tsfile/tar.gz/${tsfile_revision}" \
  --output "$tsfile_build_dir/source.tar.gz"
python3 - "$tsfile_build_dir/source.tar.gz" "$tsfile_archive_sha256" <<'PYTHON'
import hashlib
import pathlib
import sys
actual = hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest()
if actual != sys.argv[2]:
    raise SystemExit("TsFile source checksum mismatch: " + actual)
PYTHON
tar -xzf "$tsfile_build_dir/source.tar.gz" -C "$tsfile_build_dir"
tsfile_source_dir="$tsfile_build_dir/tsfile-$tsfile_revision"
python3 - "$tsfile_source_dir" <<'PYTHON'
import pathlib
import sys
for pom in pathlib.Path(sys.argv[1]).rglob("pom.xml"):
    text = pom.read_text(encoding="utf-8")
    pom.write_text(text.replace("2.2.1-SNAPSHOT", "2.2.1-260206-SNAPSHOT"), encoding="utf-8")
PYTHON
cd "$tsfile_source_dir"
mvn -Pwith-java -pl java/tsfile -am -DskipTests "$@" install
