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

package org.apache.iotdb.db.queryengine.plan.parser;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CancelMigrations;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CancelMigrationsStatement;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;

public class CancelMigrationsStatementTest {

  private static final String[] VALID_STATEMENTS = {
    "cancel all migrations", "CaNcEl AlL MiGrAtIoNs"
  };

  private static final String[] INVALID_STATEMENTS = {
    "cancel all migrations 1",
    "cancel migrations",
    "cancel all migration",
    "cancel all migrations on 1"
  };

  private final SqlParser tableSqlParser = new SqlParser();
  private final IClientSession clientSession = new InternalClientSession("testClient");

  @Test
  public void testCancelAllMigrationsTreeDialectSyntax() {
    for (String sql : VALID_STATEMENTS) {
      org.apache.iotdb.db.queryengine.plan.statement.Statement statement =
          StatementGenerator.createStatement(sql, ZoneId.systemDefault());
      Assert.assertTrue(statement instanceof CancelMigrationsStatement);
    }

    for (String sql : INVALID_STATEMENTS) {
      Assert.assertThrows(
          "Tree dialect should reject: " + sql,
          RuntimeException.class,
          () -> StatementGenerator.createStatement(sql, ZoneId.systemDefault()));
    }
  }

  @Test
  public void testCancelAllMigrationsTableDialectSyntax() {
    for (String sql : VALID_STATEMENTS) {
      Statement statement =
          tableSqlParser.createStatement(sql, ZoneId.systemDefault(), clientSession);
      Assert.assertTrue(statement instanceof CancelMigrations);
    }

    for (String sql : INVALID_STATEMENTS) {
      Assert.assertThrows(
          "Table dialect should reject: " + sql,
          RuntimeException.class,
          () -> tableSqlParser.createStatement(sql, ZoneId.systemDefault(), clientSession));
    }
  }
}
