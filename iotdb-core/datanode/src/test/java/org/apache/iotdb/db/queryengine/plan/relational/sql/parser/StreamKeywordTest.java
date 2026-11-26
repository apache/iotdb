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

package org.apache.iotdb.db.queryengine.plan.relational.sql.parser;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTable;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;

public class StreamKeywordTest {

  private SqlParser sqlParser;
  private IClientSession clientSession;

  @Before
  public void setUp() {
    sqlParser = new SqlParser();
    clientSession = new InternalClientSession("testClient");
  }

  @Test
  public void testStreamAsTableNameShouldFail() {
    // STREAM cannot be used as table name without quoting
    try {
      sqlParser.createStatement(
          "CREATE TABLE stream (col1 INT32 TAG, col2 INT32 FIELD)",
          ZoneId.systemDefault(),
          clientSession);
      Assert.fail("Creating table with reserved keyword 'stream' should fail");
    } catch (final Exception e) {
      // Expected - stream is a reserved keyword
      Assert.assertTrue(
          "Expected parsing error for 'stream' keyword",
          e.getMessage().contains("Encountered") || e.getMessage().contains("mismatched"));
    }
  }

  @Test
  public void testStreamAsColumnNameShouldFail() {
    // STREAM cannot be used as column name without quoting
    try {
      sqlParser.createStatement(
          "CREATE TABLE test_table (stream INT32 TAG, col2 INT32 FIELD)",
          ZoneId.systemDefault(),
          clientSession);
      Assert.fail("Creating column with reserved keyword 'stream' should fail");
    } catch (final Exception e) {
      // Expected - stream is a reserved keyword
      Assert.assertTrue(
          "Expected parsing error for 'stream' keyword",
          e.getMessage().contains("Encountered") || e.getMessage().contains("mismatched"));
    }
  }

  @Test
  public void testStreamAsDatabaseNameShouldFail() {
    // STREAM cannot be used as database name without quoting
    try {
      sqlParser.createStatement("CREATE DATABASE stream", ZoneId.systemDefault(), clientSession);
      Assert.fail("Creating database with reserved keyword 'stream' should fail");
    } catch (final Exception e) {
      // Expected - stream is a reserved keyword
      Assert.assertTrue(
          "Expected parsing error for 'stream' keyword",
          e.getMessage().contains("Encountered") || e.getMessage().contains("mismatched"));
    }
  }

  @Test
  public void testQuotedStreamShouldWork() {
    // Quoted STREAM should work as identifier
    Statement statement =
        sqlParser.createStatement(
            "CREATE DATABASE \"stream\"", ZoneId.systemDefault(), clientSession);
    CreateDB createDb = (CreateDB) statement;
    Assert.assertEquals("stream", createDb.getDbName());

    statement =
        sqlParser.createStatement(
            "CREATE TABLE \"stream\" (\"stream\" INT32 TAG, col2 INT32 FIELD)",
            ZoneId.systemDefault(),
            clientSession);
    CreateTable createTable = (CreateTable) statement;
    Assert.assertEquals("stream", createTable.getName().toString());
  }

  @Test
  public void testStreamsAsNonReservedKeyword() {
    // STREAMS is non-reserved and can be used as identifier
    Statement statement =
        sqlParser.createStatement("CREATE DATABASE streams", ZoneId.systemDefault(), clientSession);
    CreateDB createDb = (CreateDB) statement;
    Assert.assertEquals("streams", createDb.getDbName());

    statement =
        sqlParser.createStatement(
            "CREATE TABLE streams (streams INT32 TAG, col2 INT32 FIELD)",
            ZoneId.systemDefault(),
            clientSession);
    CreateTable createTable = (CreateTable) statement;
    Assert.assertEquals("streams", createTable.getName().toString());
  }
}
