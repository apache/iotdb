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

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExplainOutputFormat;
import org.apache.iotdb.db.queryengine.statistics.FragmentInstanceStatisticsJsonDrawer;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class ExplainAnalyzeOperatorTest {

  @Test
  public void testNextBuildsSingleJsonAnalyzeResult() throws Exception {
    ExplainAnalyzeOperator explainAnalyzeOperator =
        Mockito.mock(ExplainAnalyzeOperator.class, Mockito.CALLS_REAL_METHODS);
    Operator child = Mockito.mock(Operator.class);
    when(child.hasNextWithTimer()).thenReturn(false);

    MPPQueryContext context = createTestContext();
    context.setAnalyzeCost(1000000L);
    context.recordDispatchCost(2000000L);
    FragmentInstanceStatisticsJsonDrawer jsonDrawer = new FragmentInstanceStatisticsJsonDrawer();
    jsonDrawer.renderPlanStatistics(context);

    setField(explainAnalyzeOperator, "child", child);
    setField(explainAnalyzeOperator, "verbose", true);
    setField(explainAnalyzeOperator, "instances", Collections.emptyList());
    setField(explainAnalyzeOperator, "outputFormat", ExplainOutputFormat.JSON);
    setField(explainAnalyzeOperator, "fragmentInstanceStatisticsJsonDrawer", jsonDrawer);
    setField(explainAnalyzeOperator, "clientManager", null);
    setField(explainAnalyzeOperator, "mppQueryContext", context);

    TsBlock result = explainAnalyzeOperator.next();

    assertEquals(1, result.getPositionCount());
    String json = result.getColumn(0).getBinary(0).getStringValue(TSFileConfig.STRING_CHARSET);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();
    assertEquals(0, root.get("fragmentInstancesCount").getAsInt());
    assertTrue(root.getAsJsonObject("planStatistics").has("analyzeCostMs"));
    assertTrue(root.getAsJsonObject("planStatistics").has("dispatchCostMs"));
  }

  private static MPPQueryContext createTestContext() {
    SessionInfo sessionInfo = new SessionInfo(0, "root", ZoneId.systemDefault());
    MPPQueryContext context =
        new MPPQueryContext(
            "EXPLAIN ANALYZE (FORMAT JSON) SELECT * FROM table1",
            new QueryId("test_query_id"),
            sessionInfo,
            new TEndPoint("127.0.0.1", 6667),
            new TEndPoint("127.0.0.1", 10730));
    context.setAnalyzeCost(0);
    return context;
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = ExplainAnalyzeOperator.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }
}
