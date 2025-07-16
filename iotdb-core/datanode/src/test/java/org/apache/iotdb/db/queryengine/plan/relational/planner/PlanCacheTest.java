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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LiteralMarkerReplacer;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.util.SqlFormatter;

import org.junit.Test;
import org.mockito.Mockito;

import java.time.ZoneId;

public class PlanCacheTest {

  @Test
  public void statementGeneralizationTest() {

    String databaseName = "tetsdb";
    IClientSession clientSession = Mockito.mock(IClientSession.class);
    Mockito.when(clientSession.getDatabaseName()).thenReturn(databaseName);

    SqlParser sqlParser = new SqlParser();
    String sql =
        "select id + 1 from table1 where id > 10 and time=13289078 and deviceId = 'test' order by rank+1";
    long startTime = System.nanoTime();
    Statement originalStatement =
        sqlParser.createStatement(sql, ZoneId.systemDefault(), clientSession);
    System.out.println("Time to parse: " + (System.nanoTime() - startTime));

    LiteralMarkerReplacer literalMarkerReplacer = new LiteralMarkerReplacer();
    startTime = System.nanoTime();
    literalMarkerReplacer.process(originalStatement);

    String newSql = SqlFormatter.formatSql(originalStatement);
    System.out.println("Time to replace: " + (System.nanoTime() - startTime));
    System.out.println(newSql);
    System.out.println(literalMarkerReplacer.getLiteralList());
  }
}
