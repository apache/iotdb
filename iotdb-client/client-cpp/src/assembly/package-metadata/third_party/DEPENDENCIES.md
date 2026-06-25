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
# Third-party Dependencies

## Redistributed in this package

These components are statically linked into the `iotdb_session` library, or
bundled as shared libraries, and are therefore part of the binary distribution.
Their licenses are Category A (Apache-2.0 / Boost). Attribution is provided in
the [`NOTICE`](NOTICE) file in this directory; non-Apache license texts are under
[`licenses/`](licenses). Apache-2.0 components are covered by the top-level
`LICENSE` file.

| Component | Version | How | License |
| --- | --- | --- | --- |
| Apache Thrift | 0.23.0 | statically linked | Apache License 2.0 |
| Boost | 1.60.0 on Linux/Windows, 1.84.0 on macOS by default | statically linked (header-only) | Boost Software License 1.0 |
| OpenSSL | 3.x: system OpenSSL 3.x when present, else 3.5.0 built from source (`WITH_SSL=ON`, default) | bundled shared libs in `lib/` | Apache License 2.0 |

## Build-time only (not redistributed)

These tools are used only to build Thrift / generate code; none of their code
is included in the distributed library.

| Component | Version | License |
| --- | --- | --- |
| GNU m4 | 1.4.19 on Linux build bootstrap | GPL-3.0-or-later |
| GNU flex | 2.6.4 on Linux build bootstrap | BSD-style flex license |
| GNU bison | 3.8 on Linux build bootstrap | GPL-3.0-or-later |
