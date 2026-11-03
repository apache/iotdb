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

# Table-Model SQL Grammar Guidelines

These instructions apply to `iotdb-core/relational-grammar/` and supplement the
[repository instructions](../../AGENTS.md).

## Reserved and non-reserved keywords

- For each new keyword token in the lexer portion of
  [RelationalSql.g4](src/main/antlr4/org/apache/iotdb/db/relational/grammar/sql/RelationalSql.g4),
  determine whether it is intentionally reserved. **If it is not a reserved word, add the token
  to the `nonReserved` rule in the same change.** Defining only the lexer token can make a
  previously valid unquoted database, table, column, or alias name stop parsing, because the
  `identifier` rule accepts these keyword tokens through `nonReserved`.
- Keep `nonReserved` as a list of token alternatives only; do not add nested parser rules.
  The DataNode SQL parser's `exitNonReserved` implementation depends on this structure.
- Do not reserve a word accidentally by omitting it from `nonReserved`, or make an existing
  reserved word non-reserved merely to resolve a grammar conflict. Intentional reservation
  must be part of the SQL change's contract and covered by compatibility tests. This rule is
  about keyword tokens, not operators, punctuation, whitespace, fragments, or literals.

## Verification

- Cover the new SQL syntax and non-reserved keyword use as unquoted identifiers in the relevant
  database/table/column/alias positions, including case variants. Quoted-only tests do not
  protect unquoted-name compatibility. For intentionally reserved words, verify rejection in
  unquoted identifier positions and acceptance with supported quoting.
- The existing
  [StreamKeywordTest](../datanode/src/test/java/org/apache/iotdb/db/queryengine/plan/relational/sql/parser/StreamKeywordTest.java)
  demonstrates reserved and non-reserved identifier checks. For integration coverage, follow
  [integration-test/AGENTS.md](../../integration-test/AGENTS.md) and use the table-model profile.
- Edit the `.g4` sources and regenerate through Maven; do not edit generated Java, `.tokens`,
  or `.interp` files. Rebuild the affected grammar and DataNode consumers and run the affected
  parser/identifier tests. Successful grammar generation alone does not establish identifier
  compatibility or correct AST construction.
