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

Every Java file must start with the Apache License 2.0 header:

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

## Verification Commands

```bash
# Format code
mvn spotless::apply -P with-integration-tests

# Check style
mvn checkstyle::check -P with-integration-tests
```
