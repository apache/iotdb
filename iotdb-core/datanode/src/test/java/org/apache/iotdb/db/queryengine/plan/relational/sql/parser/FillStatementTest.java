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

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Fill;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.commons.queryengine.plan.statement.component.FillPolicy;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;

import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FillStatementTest {

  private SqlParser sqlParser;
  private IClientSession clientSession;

  @Before
  public void setUp() {
    sqlParser = new SqlParser();
    clientSession = new InternalClientSession("testClient");
  }

  @Test
  public void testNextFillStatement() {
    Statement statement =
        sqlParser.createStatement(
            "select time, device_id, s1 from table1 FILL METHOD NEXT TIME_BOUND 2ms TIME_COLUMN 1 FILL_GROUP 2,3",
            ZoneId.systemDefault(),
            clientSession);

    assertTrue(statement instanceof Query);
    QuerySpecification querySpecification = (QuerySpecification) ((Query) statement).getQueryBody();
    assertTrue(querySpecification.getFill().isPresent());
    Fill fill = querySpecification.getFill().get();
    assertEquals(FillPolicy.NEXT, fill.getFillMethod());
    assertTrue(fill.getTimeBound().isPresent());
    assertEquals(1, fill.getTimeColumnIndex().get().getParsedValue());
    assertEquals(2, fill.getFillGroupingElements().get().size());
    assertEquals(2, fill.getFillGroupingElements().get().get(0).getParsedValue());
    assertEquals(3, fill.getFillGroupingElements().get().get(1).getParsedValue());
  }
}
