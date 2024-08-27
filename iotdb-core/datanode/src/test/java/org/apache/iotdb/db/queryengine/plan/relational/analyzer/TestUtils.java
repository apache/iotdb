/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestUtils {
  public static final WarningCollector DEFAULT_WARNING = WarningCollector.NOOP;
  public static final QueryId QUERY_ID = new QueryId("test_query");
  public static final SessionInfo SESSION_INFO =
      new SessionInfo(
          1L,
          "iotdb-user",
          ZoneId.systemDefault(),
          IoTDBConstant.ClientVersion.V_1_0,
          "db",
          IClientSession.SqlDialect.TABLE);
  public static final Metadata TEST_MATADATA = new TestMatadata();
  public static final MPPQueryContext QUERY_CONTEXT =
      new MPPQueryContext("only for test", QUERY_ID, SESSION_INFO, null, null);

  public static final List<String> ALL_DEVICE_ENTRIES =
      Arrays.asList(
          "table1.shanghai.B3.YY",
          "table1.beijing.A1.ZZ",
          "table1.shenzhen.B1.XX",
          "table1.shenzhen.B2.ZZ",
          "table1.shanghai.A3.YY",
          "table1.beijing.A2.XX");
  // data region 1
  public static final List<String> ORIGINAL_DEVICE_ENTRIES_1 =
      Arrays.asList(
          "table1.shanghai.B3.YY",
          "table1.shenzhen.B1.XX",
          "table1.shenzhen.B2.ZZ",
          "table1.shanghai.A3.YY");
  // data region 2
  public static final List<String> ORIGINAL_DEVICE_ENTRIES_2 =
      Arrays.asList("table1.shenzhen.B1.XX", "table1.shenzhen.B2.ZZ");

  public static void assertTableScan(
      TableScanNode tableScanNode,
      List<String> deviceEntries,
      Ordering ordering,
      long pushLimit,
      long pushOffset,
      boolean pushLimitToEachDevice) {
    assertEquals(
        deviceEntries,
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(ordering, tableScanNode.getScanOrder());
    assertEquals(pushLimit, tableScanNode.getPushDownLimit());
    assertEquals(pushOffset, tableScanNode.getPushDownOffset());
    if (tableScanNode.getPushDownLimit() > 0) {
      assertEquals(pushLimitToEachDevice, tableScanNode.isPushLimitToEachDevice());
    }
  }

  public static void assertMergeSortNode(MergeSortNode mergeSortNode) {
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof SortNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
  }

  public static List<Symbol> buildSymbols(String... names) {
    return Arrays.stream(names).map(Symbol::of).collect(Collectors.toList());
  }

  public static void assertJoinNodeEquals(
      JoinNode joinNode,
      JoinNode.JoinType joinType,
      List<JoinNode.EquiJoinClause> joinCriteria,
      List<Symbol> leftOutputSymbols,
      List<Symbol> rightOutputSymbols) {
    assertEquals(joinType, joinNode.getJoinType());
    assertEquals(joinCriteria, joinNode.getCriteria());
    assertEquals(leftOutputSymbols, joinNode.getLeftOutputSymbols());
    assertEquals(rightOutputSymbols, joinNode.getRightOutputSymbols());
  }
}
