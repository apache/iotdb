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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowSubscriptions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.util.DataNodeSqlFormatter;

import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ShowSubscriptionsTest {

  private SqlParser sqlParser;
  private IClientSession clientSession;

  @Before
  public void setUp() {
    sqlParser = new SqlParser();
    clientSession = new InternalClientSession("testClient");
  }

  @Test
  public void testDetailsRoundTrip() {
    assertDetailsRoundTrip("SHOW SUBSCRIPTIONS DETAILS", null);
    assertDetailsRoundTrip("SHOW SUBSCRIPTIONS DETAILS ON \"topic-1\"", "topic-1");
  }

  private void assertDetailsRoundTrip(final String sql, final String topicName) {
    final Statement statement =
        sqlParser.createStatement(sql, ZoneId.systemDefault(), clientSession);

    assertTrue(statement instanceof ShowSubscriptions);
    final ShowSubscriptions showSubscriptions = (ShowSubscriptions) statement;
    assertTrue(showSubscriptions.isDetails());
    assertEquals(topicName, showSubscriptions.getTopicName());
    assertEquals(sql, DataNodeSqlFormatter.formatDataNodeSql(statement));
  }
}
