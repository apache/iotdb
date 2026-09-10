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
import org.apache.iotdb.commons.queryengine.plan.relational.sql.parser.ParsingException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.util.DataNodeSqlFormatter;

import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CountDBStatementTest {

  private SqlParser sqlParser;
  private IClientSession clientSession;

  @Before
  public void setUp() {
    sqlParser = new SqlParser();
    clientSession = new InternalClientSession("testClient");
  }

  @Test
  public void testCountDatabaseStatementRejected() {
    assertThrows(
        ParsingException.class,
        () -> sqlParser.createStatement("count database", ZoneId.systemDefault(), clientSession));
  }

  @Test
  public void testCountDatabasesStatement() {
    final Statement statement =
        sqlParser.createStatement("count databases", ZoneId.systemDefault(), clientSession);

    assertTrue(statement instanceof CountDB);
    assertEquals("COUNT DATABASES", statement.toString());
    assertEquals("COUNT DATABASES", DataNodeSqlFormatter.formatDataNodeSql(statement));
  }
}
