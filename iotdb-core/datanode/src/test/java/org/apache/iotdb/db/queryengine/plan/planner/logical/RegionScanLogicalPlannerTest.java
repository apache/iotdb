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

package org.apache.iotdb.db.queryengine.plan.planner.logical;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.TimeseriesSchemaInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.DeviceRegionScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.TimeseriesRegionScanNode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.queryengine.plan.planner.logical.LogicalPlannerTestUtil.parseSQLToPlanNode;

public class RegionScanLogicalPlannerTest {

  @Test
  public void testShowDevicesWithTimeCondition() throws IllegalPathException {
    String sql = "show devices where time > 1000";

    QueryId queryId = new QueryId("test");
    // fake initResultNodeContext()
    queryId.genPlanNodeId();

    Map<PartialPath, Boolean> devicePathsToAligned = new HashMap<>();
    devicePathsToAligned.put(new PartialPath(new PlainDeviceID("root.sg.d1")), false);
    devicePathsToAligned.put(new PartialPath(new PlainDeviceID("root.sg.d2")), false);
    devicePathsToAligned.put(new PartialPath(new PlainDeviceID("root.sg.d2.a")), true);

    DeviceRegionScanNode regionScanNode =
        new DeviceRegionScanNode(queryId.genPlanNodeId(), devicePathsToAligned, false, null);

    PlanNode actualPlan = parseSQLToPlanNode(sql);
    Assert.assertEquals(actualPlan, regionScanNode);
  }

  @Test
  public void testShowDevicesWithTimeConditionWithLimitOffset() throws IllegalPathException {
    String sql = "show devices where time > 1000 limit 20 offset 10";

    QueryId queryId = new QueryId("test");
    // fake initResultNodeContext()
    queryId.genPlanNodeId();

    Map<PartialPath, Boolean> devicePathsToAligned = new HashMap<>();
    devicePathsToAligned.put(new PartialPath(new PlainDeviceID("root.sg.d1")), false);
    devicePathsToAligned.put(new PartialPath(new PlainDeviceID("root.sg.d2")), false);
    devicePathsToAligned.put(new PartialPath(new PlainDeviceID("root.sg.d2.a")), true);

    DeviceRegionScanNode regionScanNode =
        new DeviceRegionScanNode(queryId.genPlanNodeId(), devicePathsToAligned, false, null);

    LimitNode limitNode = new LimitNode(queryId.genPlanNodeId(), 20);
    limitNode.addChild(regionScanNode);
    OffsetNode offsetNode = new OffsetNode(queryId.genPlanNodeId(), 10);
    offsetNode.addChild(limitNode);

    PlanNode actualPlan = parseSQLToPlanNode(sql);
    Assert.assertEquals(actualPlan, offsetNode);
  }

  @Test
  public void testCountDevicesWithTimeConditionWithLimitOffset() throws IllegalPathException {
    String sql = "count devices where time > 1000";

    QueryId queryId = new QueryId("test");
    // fake initResultNodeContext()
    queryId.genPlanNodeId();

    Map<PartialPath, Boolean> devicePathsToAligned = new HashMap<>();
    devicePathsToAligned.put(new PartialPath(new PlainDeviceID("root.sg.d1")), false);
    devicePathsToAligned.put(new PartialPath(new PlainDeviceID("root.sg.d2")), false);
    devicePathsToAligned.put(new PartialPath(new PlainDeviceID("root.sg.d2.a")), true);

    DeviceRegionScanNode regionScanNode =
        new DeviceRegionScanNode(queryId.genPlanNodeId(), devicePathsToAligned, true, null);

    PlanNode actualPlan = parseSQLToPlanNode(sql);
    Assert.assertEquals(actualPlan, regionScanNode);
  }

  @Test
  public void testCountTimeseriesWithTimeConditionWithLimitOffset() throws IllegalPathException {
    String sql = "count timeseries where time > 1000";

    QueryId queryId = new QueryId("test");
    // fake initResultNodeContext()
    queryId.genPlanNodeId();

    Map<PartialPath, Map<PartialPath, List<TimeseriesSchemaInfo>>> deviceToTimeseriesSchemaInfoMap =
        new HashMap<>();
    Map<PartialPath, List<TimeseriesSchemaInfo>> timeseriesSchemaInfoMap = new HashMap<>();
    timeseriesSchemaInfoMap.put(
        new MeasurementPath("root.sg.d1.s1", TSDataType.INT32),
        Collections.singletonList(
            new TimeseriesSchemaInfo(
                false, "INT32", "PLAIN", "LZ4", "{\"key1\":\"value1\"}", "", "")));
    timeseriesSchemaInfoMap.put(
        new MeasurementPath("root.sg.d1.s2", TSDataType.DOUBLE),
        Collections.singletonList(
            new TimeseriesSchemaInfo(
                false, "DOUBLE", "PLAIN", "LZ4", "{\"key1\":\"value1\"}", "", "")));
    timeseriesSchemaInfoMap.put(
        new MeasurementPath("root.sg.d1.s3", TSDataType.BOOLEAN),
        Collections.singletonList(
            new TimeseriesSchemaInfo(
                false, "BOOLEAN", "PLAIN", "LZ4", "{\"key1\":\"value2\"}", "", "")));
    deviceToTimeseriesSchemaInfoMap.put(
        new PartialPath(new PlainDeviceID("root.sg.d1")), timeseriesSchemaInfoMap);

    Map<PartialPath, List<TimeseriesSchemaInfo>> timeseriesSchemaInfoMap2 = new HashMap<>();
    timeseriesSchemaInfoMap2.put(
        new MeasurementPath("root.sg.d2.s1", TSDataType.INT32),
        Collections.singletonList(
            new TimeseriesSchemaInfo(
                false, "INT32", "PLAIN", "LZ4", "{\"key1\":\"value1\"}", "", "")));
    timeseriesSchemaInfoMap2.put(
        new MeasurementPath("root.sg.d2.s2", TSDataType.DOUBLE),
        Collections.singletonList(
            new TimeseriesSchemaInfo(
                false, "DOUBLE", "PLAIN", "LZ4", "{\"key1\":\"value1\"}", "", "")));
    timeseriesSchemaInfoMap2.put(
        new MeasurementPath("root.sg.d2.s4", TSDataType.TEXT),
        Collections.singletonList(
            new TimeseriesSchemaInfo(
                false, "TEXT", "PLAIN", "LZ4", "{\"key2\":\"value1\"}", "", "")));
    deviceToTimeseriesSchemaInfoMap.put(
        new PartialPath(new PlainDeviceID("root.sg.d2")), timeseriesSchemaInfoMap2);

    List<String> schemas = new ArrayList<>();
    schemas.add("s1");
    schemas.add("s2");
    List<TimeseriesSchemaInfo> timeseriesSchemaInfoList = new ArrayList<>();
    Map<PartialPath, List<TimeseriesSchemaInfo>> timeseriesSchemaInfoMap3 = new HashMap<>();
    timeseriesSchemaInfoList.add(
        new TimeseriesSchemaInfo(true, "INT32", "PLAIN", "LZ4", "{\"key1\":\"value1\"}", "", ""));
    timeseriesSchemaInfoList.add(
        new TimeseriesSchemaInfo(true, "DOUBLE", "PLAIN", "LZ4", "{\"key1\":\"value1\"}", "", ""));
    timeseriesSchemaInfoMap3.put(
        new AlignedPath("root.sg.d2.a", schemas, Collections.emptyList()),
        timeseriesSchemaInfoList);
    deviceToTimeseriesSchemaInfoMap.put(
        new PartialPath(new PlainDeviceID("root.sg.d2.a")), timeseriesSchemaInfoMap3);

    TimeseriesRegionScanNode regionScanNode =
        new TimeseriesRegionScanNode(queryId.genPlanNodeId(), null, true, null);
    regionScanNode.setDeviceToTimeseriesSchemaInfo(deviceToTimeseriesSchemaInfoMap);

    PlanNode actualPlan = parseSQLToPlanNode(sql);
    Assert.assertEquals(actualPlan, regionScanNode);
  }
}
