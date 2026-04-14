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

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.ZoneId;

public class PlanCacheTest {

  @After
  public void tearDown() {
    PlanCacheManager.getInstance().clear();
  }

  @Test
  public void statementGeneralizationTest() {
    String databaseName = "testdb";
    IClientSession clientSession = Mockito.mock(IClientSession.class);
    Mockito.when(clientSession.getDatabaseName()).thenReturn(databaseName);

    SqlParser sqlParser = new SqlParser();
    String sql =
        "select id + 1 from table1 where id > 10 and time=13289078 and deviceId = 'test' order by rank+1";
    Statement originalStatement =
        sqlParser.createStatement(sql, ZoneId.systemDefault(), clientSession);

    LiteralMarkerReplacer literalMarkerReplacer = new LiteralMarkerReplacer();
    literalMarkerReplacer.process(originalStatement);

    String generalizedSql = SqlFormatter.formatSql(originalStatement);
    String expectedSql =
        String.join(
            "\n",
            "SELECT (id + LiteralMarker(#0))",
            "FROM",
            "  table1",
            "WHERE ((id > LiteralMarker(#1)) AND (time = LiteralMarker(#2)) AND (deviceId = LiteralMarker(#3)))",
            "ORDER BY (rank + LiteralMarker(#4)) ASC\n");
    Assert.assertEquals(expectedSql, generalizedSql);
    Assert.assertEquals(5, literalMarkerReplacer.getLiteralList().size());
  }

  @Test
  public void planCacheStatePromotionTest() {
    String cacheKey = "promotion-key";
    PlanCacheManager planCacheManager = PlanCacheManager.getInstance();

    for (int i = 0; i < 5; i++) {
      PlanCacheManager.LookupDecision lookupDecision = planCacheManager.getLookupDecision(cacheKey);
      Assert.assertEquals(PlanCacheManager.PlanCacheState.MONITOR, lookupDecision.getState());
      Assert.assertFalse(lookupDecision.shouldLookup());
      planCacheManager.recordExecution(cacheKey, 2_000_000L, 5_000_000L, false);
    }

    Assert.assertEquals(
        PlanCacheManager.PlanCacheState.ACTIVE, planCacheManager.getTemplateState(cacheKey));
    Assert.assertTrue(planCacheManager.shouldCache(cacheKey));
    Assert.assertTrue(planCacheManager.getEstimatedReusablePlanningCost(cacheKey) >= 2_000_000L);
  }

  @Test
  public void planCacheStateBypassTest() {
    String cacheKey = "bypass-key";
    PlanCacheManager planCacheManager = PlanCacheManager.getInstance();

    for (int i = 0; i < 5; i++) {
      planCacheManager.recordExecution(cacheKey, 100_000L, 5_000_000L, false);
    }

    Assert.assertEquals(
        PlanCacheManager.PlanCacheState.BYPASS, planCacheManager.getTemplateState(cacheKey));

    PlanCacheManager.LookupDecision lookupDecision = planCacheManager.getLookupDecision(cacheKey);
    Assert.assertEquals(PlanCacheManager.PlanCacheState.BYPASS, lookupDecision.getState());
    Assert.assertFalse(lookupDecision.shouldLookup());
    Assert.assertEquals("Low_Benefit", lookupDecision.getReason());
  }
}
