<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->
# Third-party dependency cache (`client-cpp/third-party`)

CMake downloads (or reuses) build-time tarballs and archives here. The
directory ships with the source tree so you can **stage dependencies on a
networked machine, copy the whole IoTDB checkout to an offline host, and
build `client-cpp` with `-DIOTDB_OFFLINE=ON`**.

Tarballs themselves are **not** committed to Git (see per-platform
`.gitignore` files). Only this README and the empty platform folders are
tracked. Each `linux/`, `mac/`, and `windows/` sub-folder ships a minimal
`.gitignore` (`*` with `!.gitignore`) so Git keeps the directory in the
tree while ignoring downloaded archives.

## Layout

```
third-party/
├── linux/     # tarballs for Linux offline builds
├── mac/       # tarballs for macOS offline builds
└── windows/   # tarballs / zips for Windows offline builds
```

Override the root with `-DIOTDB_DEPS_DIR=<path>` (Maven: `-Diotdb.deps.dir=...`).
The platform sub-folder (`linux/`, `mac/`, `windows/`) is selected automatically.

## Staging dependencies (online machine)

Run a normal **online** configure once; CMake caches everything under the
matching `<os>/` folder:

```bash
cmake -S iotdb-client/client-cpp -B build
# or: mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests package
```

Alternatively copy files manually from the URLs listed in
[`README.md`](../README.md) (*Offline build* section).

## Offline machine (copy whole IoTDB tree)

1. Copy the entire IoTDB repository (including `iotdb-client/client-cpp/third-party/<os>/`).
2. Configure with offline mode:

   ```bash
   cmake -S iotdb-client/client-cpp -B build -DIOTDB_OFFLINE=ON
   cmake --build build --config Release --target install
   ```

## Per-platform files (offline minimum)

| Platform   | Typical files |
|------------|---------------|
| `linux/`   | `thrift-0.23.0.tar.gz`, `boost_1_60_0.tar.gz`, `m4-1.4.19.tar.gz`, `flex-2.6.4.tar.gz`, `bison-3.8.tar.gz` (+ `openssl-3.5.0.tar.gz` only when `WITH_SSL=ON` and no system OpenSSL is present) |
| `mac/`     | `thrift-0.23.0.tar.gz`, `boost_1_60_0.tar.gz` (Xcode CLT usually provides m4/flex/bison) |
| `windows/` | `thrift-0.23.0.tar.gz`, `boost_1_60_0.tar.gz`, `win_flex_bison-2.5.25.zip` (or any `win_flex_bison*.zip`; skip if flex/bison already on `PATH`) |

Download URLs: see the *Offline build* table in [`README.md`](../README.md).
