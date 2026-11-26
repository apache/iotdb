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

package org.apache.iotdb.db.queryengine.plan.relational.sql;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCreateTopic;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.util.DataNodeSqlFormatter;

import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ShowCreateTopicTest {

  private SqlParser sqlParser;
  private IClientSession clientSession;

  @Before
  public void setUp() {
    sqlParser = new SqlParser();
    clientSession = new InternalClientSession("testClient");
    clientSession.setDatabaseName("testdb");
  }

  @Test
  public void testShowCreateTopicRoundTripWithQuotedIdentifier() {
    final String sql = "SHOW CREATE TOPIC \"topic-1\"";
    final Statement statement =
        sqlParser.createStatement(sql, ZoneId.systemDefault(), clientSession);

    assertTrue(statement instanceof ShowCreateTopic);
    assertEquals("topic-1", ((ShowCreateTopic) statement).getTopicName());
    assertEquals(sql, DataNodeSqlFormatter.formatDataNodeSql(statement));

    final Analysis analysis =
        AnalyzerTest.analyzeSQL(
            sql,
            TestUtils.TEST_MATADATA,
            new MPPQueryContext(
                sql, new QueryId("show_create_topic_test"), TestUtils.SESSION_INFO, null, null));
    assertNotNull(analysis);
  }
}
