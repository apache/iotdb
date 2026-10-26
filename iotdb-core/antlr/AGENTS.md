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

# Tree-Model SQL Grammar Guidelines

These instructions apply to `iotdb-core/antlr/` and supplement the
[repository instructions](../../AGENTS.md).

## Keyword and identifier compatibility

- When adding a keyword token to
  [SqlLexer.g4](src/main/antlr4/org/apache/iotdb/db/qp/sql/SqlLexer.g4), **add the same token to
  the `keyWords` rule in
  [IdentifierParser.g4](src/main/antlr4/org/apache/iotdb/db/qp/sql/IdentifierParser.g4) in the
  same change**. A new dedicated lexer token no longer matches the generic `ID` alternative;
  omitting it from `keyWords` can break its use as an unquoted measurement or path node name.
- Preserve the existing identifier and path rules in
  [IoTDBSqlParser.g4](src/main/antlr4/org/apache/iotdb/db/qp/sql/IoTDBSqlParser.g4), which imports
  `IdentifierParser`. Do not make users quote previously valid unquoted names to work around a
  missing keyword entry, or change existing reserved/special-token behavior incidentally.
- This synchronization applies to keyword tokens. Do not add punctuation, operators,
  whitespace, lexer fragments, or literal rules to `keyWords` merely because they are defined
  in the lexer.

## Verification

- Verify both the newly introduced SQL syntax and the same word used as an unquoted measurement
  and path component in create/insert/query statements. Include case variants; testing only
  quoted identifiers does not cover this compatibility requirement. The existing
  [IoTDBSyntaxConventionIdentifierIT](../../integration-test/src/test/java/org/apache/iotdb/db/it/IoTDBSyntaxConventionIdentifierIT.java)
  provides identifier/path regression coverage. Follow
  [integration-test/AGENTS.md](../../integration-test/AGENTS.md) before editing or running ITs.
- Edit the `.g4` sources and regenerate through Maven; do not edit generated Java, `.tokens`,
  or `.interp` files. Rebuild the affected grammar and DataNode consumers and run the affected
  parser/identifier tests. Successful grammar generation alone does not prove that existing
  names still parse correctly.
