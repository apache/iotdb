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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.ParsingException;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ReplicaHintPlannerTest extends PlanTester {

  @Test
  public void testReplicaHint() {
    String sql = "SELECT /*+ REPLICA(0) */ * FROM table1";
    createPlan(sql);
    for (int fragment = 1; fragment <= 3; fragment++) {
      verifyReplica(fragment, ImmutableMap.of("table1", 0));
    }
  }

  @Test
  public void testReplicaHintWithNegativeIndex() {
    try {
      String sql = "SELECT /*+ REPLICA(-1) */ * FROM table1";
      createPlan(sql);
    } catch (ParsingException ex) {
      assertTrue(
          ex.getMessage().contains("mismatched input '-'. Expecting: <identifier>, <integer>"));
    }
  }

  @Test
  public void testReplicaHintWithTable() {
    String sql = "SELECT /*+ REPLICA(table1, 1) */ * FROM table1";
    createPlan(sql);
    for (int fragment = 1; fragment <= 3; fragment++) {
      verifyReplica(fragment, ImmutableMap.of("table1", 1));
    }
  }

  @Test
  public void testReplicaHintWithDatabasePrefix() {
    String sql = "SELECT /*+ REPLICA(testdb.table1, 0) */ * FROM table1";
    createPlan(sql);
    for (int fragment = 1; fragment <= 3; fragment++) {
      verifyReplica(fragment, ImmutableMap.of("table1", 0));
    }
  }

  @Test
  public void testReplicaHintWithAlias() {
    String sql = "SELECT /*+ REPLICA(t, 1) */ * FROM table1 as t";
    createPlan(sql);
    for (int fragment = 1; fragment <= 3; fragment++) {
      verifyReplica(fragment, ImmutableMap.of("t", 1));
    }
  }

  @Test
  public void testReplicaHintWithConflict() {
    String sql = "SELECT /*+ REPLICA(1) REPLICA(0) */ * FROM table1";
    createPlan(sql);
    for (int fragment = 1; fragment <= 3; fragment++) {
      verifyReplica(fragment, ImmutableMap.of("table1", 1));
    }
  }

  @Test
  public void testReplicaHintWithAgg() {
    String sql = "SELECT /*+ REPLICA(0) */ tag1, avg(s1) FROM table1 GROUP BY tag1";
    createPlan(sql);
    for (int fragment = 1; fragment <= 3; fragment++) {
      verifyReplica(fragment, ImmutableMap.of("table1", 0));
    }
  }

  @Test
  public void testRegionRouteHintWithNegativeRegion() {
    String sql = "SELECT /*+ REGION_ROUTE((-1, 1)) */ * FROM table1";
    createPlan(sql);
    for (int fragment = 1; fragment <= 3; fragment++) {
      verifyRegionRoute(fragment, ImmutableMap.of(10, 1, 12, 1));
    }
  }

  @Test
  public void testRegionRouteHintWithConflict() {
    String sql = "SELECT /*+ REGION_ROUTE(table1, (10, 1), (10, 2), (11, 3)) */ * FROM table1";
    createPlan(sql);
    for (int fragment = 1; fragment <= 3; fragment++) {
      verifyRegionRoute(fragment, ImmutableMap.of(10, 1, 11, 3));
    }
  }

  @Test
  public void testHintPriority1() {
    String sql =
        "SELECT /*+ REPLICA(2) REPLICA(table1, 2) REGION_ROUTE((-1, 2), (10, 2)) REGION_ROUTE(table1, (-1, 2), (10, 1)) */ * FROM table1";
    createPlan(sql);
    for (int fragment = 1; fragment <= 3; fragment++) {
      verifyRegionRoute(fragment, ImmutableMap.of(10, 1));
    }
  }

  @Test
  public void testHintPriority2() {
    String sql =
        "SELECT /*+ REPLICA(2) REPLICA(table1, 2) REGION_ROUTE((-1, 2), (10, 2)) REGION_ROUTE(table1, (-1, 1)) */ * FROM table1";
    createPlan(sql);
    for (int fragment = 1; fragment <= 3; fragment++) {
      verifyRegionRoute(fragment, ImmutableMap.of(10, 1));
    }
  }

  @Test
  public void testHintPriority3() {
    String sql =
        "SELECT /*+ REPLICA(2) REPLICA(table1, 2) REGION_ROUTE((-1, 2), (10, 1)) */ * FROM table1";
    createPlan(sql);
    for (int fragment = 1; fragment <= 3; fragment++) {
      verifyRegionRoute(fragment, ImmutableMap.of(10, 1));
    }
  }

  @Test
  public void testHintPriority4() {
    String sql = "SELECT /*+ REPLICA(2) REPLICA(table1, 2) REGION_ROUTE((-1, 1)) */ * FROM table1";
    createPlan(sql);
    for (int fragment = 1; fragment <= 3; fragment++) {
      verifyRegionRoute(fragment, ImmutableMap.of(10, 1));
    }
  }

  @Test
  public void testHintPriority5() {
    String sql = "SELECT /*+ REPLICA(2) REPLICA(table1, 1) */ * FROM table1";
    createPlan(sql);
    for (int fragment = 1; fragment <= 3; fragment++) {
      verifyReplica(fragment, ImmutableMap.of("table1", 1));
    }
  }

  @Test
  public void testHintPriority6() {
    String sql = "SELECT /*+ REPLICA(1) */ * FROM table1";
    createPlan(sql);
    for (int fragment = 1; fragment <= 3; fragment++) {
      verifyReplica(fragment, ImmutableMap.of("table1", 1));
    }
  }

  @Test
  public void testParallelHint() {
    String sql = "SELECT /*+ PARALLEL(5) */ * FROM table1";
    createPlan(sql);
    for (int fragment = 1; fragment <= 3; fragment++) {
      FragmentInstance fragmentInstance = getFragmentInstance(fragment);
      assertEquals(5, fragmentInstance.getParallelism());
    }
  }

  private void verifyReplica(int fragment, Map<String, Integer> tableToReplica) {
    FragmentInstance fragmentInstance = getFragmentInstance(fragment);
    PlanNode fragmentPlan = getFragmentPlan(fragment);

    TableScanNode tableScanNode = null;
    if (fragmentPlan instanceof AggregationNode) {
      tableScanNode = (TableScanNode) fragmentPlan.getChildren().get(0);
    } else if (fragmentPlan instanceof TableScanNode) {
      tableScanNode = (TableScanNode) fragmentPlan;
    }
    if (tableScanNode == null) {
      fail("tableScanNode must not be null");
    }

    assertEquals(
        tableScanNode.getRegionReplicaSet().getRegionId().getId(),
        fragmentInstance.getRegionReplicaSet().getRegionId().getId());

    String tableName =
        tableScanNode.getAlias() != null
            ? tableScanNode.getAlias().getValue()
            : tableScanNode.getQualifiedObjectName().getObjectName();

    List<TDataNodeLocation> replicaNodes =
        tableScanNode.getRegionReplicaSet().getDataNodeLocations();
    TDataNodeLocation queryNode = fragmentInstance.getHostDataNode();

    assertEquals(2, replicaNodes.size());
    assertEquals(
        queryNode.getDataNodeId(), replicaNodes.get(tableToReplica.get(tableName)).getDataNodeId());
  }

  private void verifyRegionRoute(int fragment, Map<Integer, Integer> regionToDatanode) {
    FragmentInstance fragmentInstance = getFragmentInstance(fragment);
    TableScanNode fragmentPlan = (TableScanNode) getFragmentPlan(fragment);

    // Verify region IDs match
    int regionId = fragmentPlan.getRegionReplicaSet().getRegionId().getId();
    assertEquals(regionId, fragmentInstance.getRegionReplicaSet().getRegionId().getId());

    List<TDataNodeLocation> replicaNodes =
        fragmentPlan.getRegionReplicaSet().getDataNodeLocations();
    TDataNodeLocation queryNode = fragmentInstance.getHostDataNode();

    assertEquals(2, replicaNodes.size());

    if (regionToDatanode.containsKey(regionId)) {
      int expectedDatanodeId = regionToDatanode.get(regionId);
      assertEquals(expectedDatanodeId, queryNode.getDataNodeId());
      boolean datanodeExists =
          replicaNodes.stream().anyMatch(dn -> dn.getDataNodeId() == expectedDatanodeId);
      assertTrue(datanodeExists);
    }
  }
}
