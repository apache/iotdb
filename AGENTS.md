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

# Project Overview

Apache IoTDB is a distributed time-series database written in Java. Version 2.0.x supports both tree-model (time-series paths) and table-model (relational) data access. It uses a ConfigNode/DataNode architecture with pluggable consensus protocols.


# IoTDB Java Code Style Guide

This document defines the coding rules that **must** be followed when generating or modifying Java code in this project. These rules are enforced by CheckStyle (`checkstyle.xml`) and Spotless (`pom.xml`).

## Imports

1. **Star imports are forbidden**: Never use `import xxx.*` or `import static xxx.*`. Always list each required class or static member explicitly.
2. **Import ordering** (enforced by Spotless):
   ```
   org.apache.iotdb.*
                          ← blank line
   Other third-party packages (e.g. org.apache.arrow, com.google, etc.)
                          ← blank line
   javax.*
   java.*
                          ← blank line
   static imports
   ```
3. **Remove unused imports**.

## Formatting

- Use **Google Java Format** (GOOGLE style).
- Indent with **2 spaces**, no tabs.
- Line width limit is **100 characters** (except `package`, `import`, and URL lines).
- Use **UNIX (LF)** line endings.

## Naming Conventions

| Type | Rule | Example |
|------|------|---------| 
| Package | All lowercase, dot-separated | `org.apache.iotdb.flight` |
| Class | UpperCamelCase | `TsBlockToArrowConverter` |
| Method | lowerCamelCase (`^[a-z][a-z0-9][a-zA-Z0-9_]*$`) | `fillVectorSchemaRoot` |
| Member variable | lowerCamelCase (`^[a-z][a-z0-9][a-zA-Z0-9]*$`) | `allocator` |
| Parameter | lowerCamelCase | `tsBlock` |
| Constant | UPPER_SNAKE_CASE, at most 1 consecutive uppercase letter in abbreviations | `MAX_RETRY_COUNT` |

## Code Structure

- Only one top-level class per `.java` file.
- `switch` statements must have a `default` branch.
- Empty `try`/`catch`/`if`/`else`/`switch` blocks are not allowed (unless they contain a comment).
- Exception variables in empty `catch` blocks must be named `expected`.
- `if`/`else`/`for`/`while`/`do` must use braces `{}`.

## License Header

Every file must start with the Apache License 2.0 header, java file:

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
```

md or xml file:

```xml
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
```

## Verification Commands

```bash
# Format code
mvn spotless:apply -P with-integration-tests

# Check style
mvn checkstyle:check -P with-integration-tests
```

# Integration Test Rules

- When writing integration tests, always use `SessionConfig.DEFAULT_USER` and `SessionConfig.DEFAULT_PASSWORD` for the user and password instead of hardcoding values.

# Build Commands

```bash
# Full build (produces distribution in distribution/target/)
mvn clean package -pl distribution -am -DskipTests

# Build all modules (skip tests)
mvn clean package -DskipTests

# Build CLI only
mvn clean package -pl iotdb-client/cli -am -DskipTests

# Code formatting check / auto-fix (Google Java Format via Spotless)
mvn spotless:check -P with-integration-tests
mvn spotless:apply -P with-integration-tests

# Format integration test code
mvn spotless:apply -P with-integration-tests
```

# Running Tests

```bash
# Unit tests for a specific module
mvn test -pl iotdb-core/datanode

# Single unit test class
mvn test -pl iotdb-core/datanode -Dtest=ClassName

# Single unit test method
mvn test -pl iotdb-core/datanode -Dtest=ClassName#methodName

# Integration tests — Simple mode (1 ConfigNode + 1 DataNode, tree model)
mvn clean verify -DskipUTs -pl integration-test -am -P with-integration-tests

# Single IT test class — Simple mode (1 ConfigNode + 1 DataNode, tree model)
mvn clean verify -DskipUTs -Dit.test=ClassName -DfailIfNoTests=false -Dfailsafe.failIfNoSpecifiedTests=false -pl integration-test -P with-integration-tests -am -PSimpleIT

# Integration tests — Cluster mode (1 ConfigNode + 3 DataNodes, tree model)
mvn clean verify -DskipUTs -pl integration-test -am -PClusterIT -P with-integration-tests

# Single IT test class — Cluster mode (1 ConfigNode + 3 DataNodes, tree model)
mvn clean verify -DskipUTs -Dit.test=ClassName -DfailIfNoTests=false -Dfailsafe.failIfNoSpecifiedTests=false -pl integration-test -P with-integration-tests -am -PClusterIT

# Integration tests — Table model simple
mvn clean verify -DskipUTs -pl integration-test -am -PTableSimpleIT -P with-integration-tests

# Single IT test class — Table model simple
mvn clean verify -DskipUTs -Dit.test=ClassName -DfailIfNoTests=false -Dfailsafe.failIfNoSpecifiedTests=false -pl integration-test -P with-integration-tests -am -PTableSimpleIT

# Integration tests — Table model cluster
mvn clean verify -DskipUTs -pl integration-test -am -PTableClusterIT -P with-integration-tests

# Single IT test class — Table model cluster
mvn clean verify -DskipUTs -Dit.test=ClassName -DfailIfNoTests=false -Dfailsafe.failIfNoSpecifiedTests=false -pl integration-test -P with-integration-tests -am -PTableClusterIT


# Before running ITs in IDE for the first time (generates template-node):
mvn clean package -DskipTests -pl integration-test -am -P with-integration-tests
```