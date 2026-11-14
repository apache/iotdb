<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->


# Rebuilding the inherited TsFile dependency

The upstream IoTDB base uses `org.apache.tsfile:tsfile:2.2.1-260206-SNAPSHOT`.
The Apache snapshot metadata and timestamped artifact returned HTTP 404 during
publication verification on 2026-11-14. A warm Maven cache alone therefore does
not establish that another developer can build this branch.

From the IoTDB repository root, run:

```sh
# Requires JDK 21, Maven, Python 3, curl and tar.
bash tools/build-research-dependencies.sh
mvn -pl iotdb-core/datanode,iotdb-core/confignode -am -DskipTests package
```

The bootstrap fetches Apache TsFile commit
`052ff6b44f68d3f2e04bfd7190067e4f5efb038c`, verifies archive SHA-256
`0ab7bd249844d8293ab5d9f64010920ef32b9f78e48dad39bb1c479b598b4014`,
and assigns its Maven module versions the inherited `2.2.1-260206-SNAPSHOT`
coordinate. It builds and installs the parent, Java parent, common and TsFile
artifacts into the selected Maven local repository. Extra arguments, including
`-Dmaven.repo.local=/path/to/cache`, are forwarded to Maven. Its temporary source
checkout is removed on exit. No dependency source or binary is vendored here.
The script requires access to GitHub and the ordinary public Maven repositories.

## Verification against the original cached snapshot

The four module POMs match the original snapshot POMs byte for byte after the
version substitution. A JDK 21 rebuild produced:

| Artifact | Comparison |
| --- | --- |
| common | Entire JAR SHA-256 identical: `3ebed3adaacbe21cbc1986c81ee512731be6946ba935dc4abc69c57f018dc16a` |
| tsfile | All 1022 class files and all non-Manifest entries identical |

TsFile's Manifest differs in build-directory and OSGi package-version metadata;
the original and rebuilt JARs are not claimed to be byte-identical. IoTDB's tested
ordinary JVM classpath uses the unchanged class files. OSGi deployment is outside
this verification scope. This bootstrap preserves the dependency version and
implementation instead of substituting a newer TsFile release.
