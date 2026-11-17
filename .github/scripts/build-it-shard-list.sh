#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with this
# work for additional information regarding copyright ownership.  The ASF
# licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the
# License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Write one shard of the integration test classes to a file for
# -Dfailsafe.includesFile, one fully qualified class name per line.
#
# The shards distribute every IT class that failsafe would pick up by default
# (IT*, *IT, *ITCase). The profile's category filter (groups) then selects the
# classes to run, including classes that only inherit their @Category, and the
# POM excludes still apply. Fully qualified names keep same-named classes in
# different packages apart. The file avoids command-line length limits; write it
# outside the repository so that Apache RAT (license check) doesn't flag it.
#
# Usage: build-it-shard-list.sh <shard> <total> <output file>
# where 0 <= shard < total.
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <shard> <total> <output file>" >&2
  exit 1
fi
SHARD=$1
TOTAL=$2
OUTPUT=$3
SOURCE_DIR="$(cd "$(dirname "$0")/../.." && pwd)/integration-test/src/test/java"

(cd "$SOURCE_DIR" && find . -type f \( -name 'IT*.java' -o -name '*IT.java' -o -name '*ITCase.java' \)) \
  | sed 's|^\./||; s|\.java$||; s|/|.|g' \
  | LC_ALL=C sort \
  | awk -v s="$SHARD" -v t="$TOTAL" 'NR%t==s' \
  > "$OUTPUT"
# Failsafe falls back to its default includes, i.e. runs every class, for an
# empty includes file.
if [ ! -s "$OUTPUT" ]; then
  echo "Shard $SHARD/$TOTAL contains no classes" >&2
  exit 1
fi
echo "Shard $SHARD/$TOTAL contains $(wc -l < "$OUTPUT") candidate classes"
head -5 "$OUTPUT"
