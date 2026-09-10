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

# Table SELECT Alias Resolution

The table-model analyzer resolves SELECT aliases with the following rules.

## SELECT List

Later `SingleColumn` items in a SELECT list can reference aliases explicitly defined by earlier
`SingleColumn` items. Resolution is left-to-right: forward references and self references are not
visible. `AllColumns` and `COLUMNS(...)` items do not register reusable aliases.

Only unqualified identifiers are eligible for lateral column alias resolution. Qualified names and
dereference expressions, such as `t.x`, `table1.x`, and `x.y`, continue through normal column
resolution. Lateral alias rewriting does not enter subqueries.

For an unqualified identifier in a later SELECT item, the resolution order is:

1. Local source column.
2. Previously visible SELECT aliases.
3. Existing analyzer resolution.

SELECT alias matching uses a canonical alias key derived from `Identifier.getCanonicalValue()` and
folded to uppercase. As a result, ordinary aliases and delimited aliases are matched
case-insensitively for SELECT alias reuse: `x`, `X`, `"x"`, and `"X"` all refer to the same alias
name. This alias rule does not change the source-column precedence above.

If multiple previous aliases have the same canonical name, the reference is ambiguous unless a
local source column with that name takes precedence.

Lateral alias rewriting applies inside the later SELECT item's expression, including function
arguments and inline window specifications such as `OVER (PARTITION BY ... ORDER BY ...)`. Named
`WINDOW` definitions are analyzed outside the SELECT list and do not see SELECT aliases.

The target type in `CAST(value AS type)` is not part of expression name resolution, so type names
are never treated as lateral column alias references.

If a lateral alias reference expands to an expression containing a window function, the analyzer
rejects it explicitly.

## GROUP BY, ORDER BY, WHERE, And HAVING

`GROUP BY` can reuse SELECT aliases and uses the SELECT output expression after lateral alias
rewriting. `ORDER BY` keeps the existing table-model behavior where output aliases take precedence
over source columns, including aliases whose expressions used lateral alias rewriting.

`WHERE` and `HAVING` do not see SELECT aliases.

## Output Field Names

Lateral alias rewriting changes the semantic expression used for type analysis, source-column
tracking, and planning. When a SELECT item has no explicit alias, the output field name is still
derived from the original SELECT item text, not from the rewritten expression.
