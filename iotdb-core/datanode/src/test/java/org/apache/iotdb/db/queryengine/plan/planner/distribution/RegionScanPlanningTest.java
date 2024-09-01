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

package org.apache.iotdb.db.queryengine.plan.planner.distribution;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ActiveRegionScanMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.DeviceRegionScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.TimeseriesRegionScanNode;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RegionScanPlanningTest {

  private static final Set<PartialPath> devicePaths = new HashSet<>();
  private static final Set<PartialPath> path = new HashSet<>();

  static {
    try {
      devicePaths.add(new PartialPath("root.sg.d1"));
      devicePaths.add(new PartialPath("root.sg.d22"));
      devicePaths.add(new PartialPath("root.sg.d333"));
      devicePaths.add(new PartialPath("root.sg.d4444"));
      devicePaths.add(new PartialPath("root.sg.d55555"));
      devicePaths.add(new PartialPath("root.sg.d666666"));

      path.add(new MeasurementPath("root.sg.d1.s1"));
      path.add(new MeasurementPath("root.sg.d1.s2"));
      path.add(new MeasurementPath("root.sg.d22.s1"));
      path.add(new MeasurementPath("root.sg.d22.s2"));
      path.add(new MeasurementPath("root.sg.d333.s1"));
      path.add(new MeasurementPath("root.sg.d333.s2"));
      path.add(new MeasurementPath("root.sg.d4444.s1"));
      path.add(new MeasurementPath("root.sg.d4444.s2"));
      path.add(new MeasurementPath("root.sg.d55555.s1"));
      path.add(new MeasurementPath("root.sg.d55555.s2"));
      path.add(new MeasurementPath("root.sg.d666666.s1"));
      path.add(new MeasurementPath("root.sg.d666666.s2"));
    } catch (IllegalPathException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testShowDevicesWithTimeCondition() {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());

    String sql = "show devices where time > 1000";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(4, plan.getFragments().size());

    PlanNode f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    f1Root = f1Root.getChildren().get(0);
    assertTrue(f1Root instanceof ActiveRegionScanMergeNode);
    assertEquals(4, f1Root.getChildren().size());
    assertTrue(f1Root.getChildren().get(0) instanceof DeviceRegionScanNode);
    Set<PartialPath> targetPaths =
        new HashSet<>(((DeviceRegionScanNode) f1Root.getChildren().get(0)).getDevicePaths());
    for (int i = 1; i < 4; i++) {
      PlanNode fRoot = plan.getInstances().get(i).getFragment().getPlanNodeTree();
      assertTrue(fRoot instanceof IdentitySinkNode);

      for (PlanNode child : fRoot.getChildren()) {
        assertTrue(child instanceof DeviceRegionScanNode);
        targetPaths.addAll(((DeviceRegionScanNode) child).getDevicePaths());
      }
    }
    assertEquals(devicePaths, targetPaths);
  }

  @Test
  public void testShowTimeseriesWithTimeCondition() throws IllegalPathException {
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());

    String sql = "show timeseries where time > 1000";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode logicalPlanNode = Util.genLogicalPlan(analysis, context);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, logicalPlanNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(4, plan.getFragments().size());

    PlanNode f1Root = plan.getInstances().get(0).getFragment().getPlanNodeTree();
    assertTrue(f1Root instanceof IdentitySinkNode);
    f1Root = f1Root.getChildren().get(0);
    assertTrue(f1Root instanceof ActiveRegionScanMergeNode);
    assertEquals(4, f1Root.getChildren().size());
    assertTrue(f1Root.getChildren().get(0) instanceof TimeseriesRegionScanNode);
    Set<PartialPath> targetDevicePaths =
        new HashSet<>(((TimeseriesRegionScanNode) f1Root.getChildren().get(0)).getDevicePaths());
    Set<PartialPath> targetMeasurementPaths =
        new HashSet<>(
            ((TimeseriesRegionScanNode) f1Root.getChildren().get(0)).getMeasurementPath());
    for (int i = 1; i < 4; i++) {
      PlanNode fRoot = plan.getInstances().get(i).getFragment().getPlanNodeTree();
      assertTrue(fRoot instanceof IdentitySinkNode);

      for (PlanNode child : fRoot.getChildren()) {
        assertTrue(child instanceof TimeseriesRegionScanNode);
        targetDevicePaths.addAll(((TimeseriesRegionScanNode) child).getDevicePaths());
        targetMeasurementPaths.addAll(((TimeseriesRegionScanNode) child).getMeasurementPath());
      }
    }
    assertEquals(devicePaths, targetDevicePaths);
    assertEquals(path, targetMeasurementPaths);
  }
}
