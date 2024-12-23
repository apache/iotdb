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

package org.apache.iotdb.db.queryengine.plan.relational.sql.util;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.ParsingException;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;

import java.time.ZoneId;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.iotdb.db.relational.grammar.sql.RelationalSqlKeywords.sqlKeywords;

public final class ReservedIdentifiers {
  private static final SqlParser PARSER = new SqlParser();

  public static Set<String> reservedIdentifiers() {
    return sqlKeywords().stream()
        .filter(ReservedIdentifiers::reserved)
        .sorted()
        .collect(toImmutableSet());
  }

  public static boolean reserved(String name) {
    try {
      return !(PARSER.createExpression(name, ZoneId.systemDefault()) instanceof Identifier);
    } catch (ParsingException ignored) {
      return true;
    }
  }
}
