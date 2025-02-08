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
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.DeviceContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.TimeseriesContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.DeviceRegionScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.TimeseriesRegionScanNode;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.queryengine.plan.planner.logical.LogicalPlannerTestUtil.parseSQLToPlanNode;

public class RegionScanLogicalPlannerTest {

  private static Map<PartialPath, Map<PartialPath, List<TimeseriesContext>>>
      deviceToTimeseriesSchemaInfoMap;
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static Map<PartialPath, Map<PartialPath, List<TimeseriesContext>>>
      getDeviceToTimeseriesSchemaInfoMap() throws IllegalPathException {

    if (deviceToTimeseriesSchemaInfoMap != null) {
      return deviceToTimeseriesSchemaInfoMap;
    }

    deviceToTimeseriesSchemaInfoMap = new HashMap<>();
    Map<PartialPath, List<TimeseriesContext>> timeseriesSchemaInfoMap = new HashMap<>();
    timeseriesSchemaInfoMap.put(
        new MeasurementPath("root.sg.d1.s1", TSDataType.INT32),
        Collections.singletonList(
            new TimeseriesContext(
                "INT32",
                null,
                config.getDefaultInt32Encoding().toString(),
                "LZ4",
                "{\"key1\":\"value1\"}",
                null,
                null,
                null)));
    timeseriesSchemaInfoMap.put(
        new MeasurementPath("root.sg.d1.s2", TSDataType.DOUBLE),
        Collections.singletonList(
            new TimeseriesContext(
                "DOUBLE",
                "status",
                config.getDefaultDoubleEncoding().toString(),
                "LZ4",
                "{\"key1\":\"value1\"}",
                null,
                null,
                null)));
    timeseriesSchemaInfoMap.put(
        new MeasurementPath("root.sg.d1.s3", TSDataType.BOOLEAN),
        Collections.singletonList(
            new TimeseriesContext(
                "BOOLEAN",
                null,
                config.getDefaultBooleanEncoding().toString(),
                "LZ4",
                "{\"key1\":\"value2\"}",
                null,
                null,
                null)));
    deviceToTimeseriesSchemaInfoMap.put(new PartialPath("root.sg.d1"), timeseriesSchemaInfoMap);

    Map<PartialPath, List<TimeseriesContext>> timeseriesSchemaInfoMap2 = new HashMap<>();
    timeseriesSchemaInfoMap2.put(
        new MeasurementPath("root.sg.d2.s1", TSDataType.INT32),
        Collections.singletonList(
            new TimeseriesContext(
                "INT32",
                null,
                config.getDefaultInt32Encoding().toString(),
                "LZ4",
                "{\"key1\":\"value1\"}",
                null,
                null,
                null)));
    timeseriesSchemaInfoMap2.put(
        new MeasurementPath("root.sg.d2.s2", TSDataType.DOUBLE),
        Collections.singletonList(
            new TimeseriesContext(
                "DOUBLE",
                "status",
                config.getDefaultDoubleEncoding().toString(),
                "LZ4",
                "{\"key1\":\"value1\"}",
                null,
                null,
                null)));
    timeseriesSchemaInfoMap2.put(
        new MeasurementPath("root.sg.d2.s4", TSDataType.TEXT),
        Collections.singletonList(
            new TimeseriesContext(
                "TEXT",
                null,
                config.getDefaultTextEncoding().toString(),
                "LZ4",
                "{\"key2\":\"value1\"}",
                null,
                null,
                null)));
    deviceToTimeseriesSchemaInfoMap.put(new PartialPath("root.sg.d2"), timeseriesSchemaInfoMap2);

    List<String> schemas = new ArrayList<>();
    schemas.add("s1");
    schemas.add("s2");
    List<TimeseriesContext> timeseriesContextList = new ArrayList<>();
    Map<PartialPath, List<TimeseriesContext>> timeseriesSchemaInfoMap3 = new HashMap<>();
    timeseriesContextList.add(
        new TimeseriesContext(
            "INT32",
            null,
            config.getDefaultInt32Encoding().toString(),
            "LZ4",
            "{\"key1\":\"value1\"}",
            null,
            null,
            null));
    timeseriesContextList.add(
        new TimeseriesContext(
            "DOUBLE",
            "status",
            config.getDefaultDoubleEncoding().toString(),
            "LZ4",
            "{\"key1\":\"value1\"}",
            null,
            null,
            null));
    timeseriesSchemaInfoMap3.put(
        new AlignedPath("root.sg.d2.a", schemas, Collections.emptyList()), timeseriesContextList);
    deviceToTimeseriesSchemaInfoMap.put(new PartialPath("root.sg.d2.a"), timeseriesSchemaInfoMap3);

    return deviceToTimeseriesSchemaInfoMap;
  }

  @Test
  public void testShowDevicesWithTimeCondition() throws IllegalPathException {
    String sql = "show devices where time > 1000";

    QueryId queryId = new QueryId("test");
    // fake initResultNodeContext()
    queryId.genPlanNodeId();

    Map<PartialPath, DeviceContext> deviceContextMap = new HashMap<>();
    deviceContextMap.put(
        new PartialPath("root.sg.d1"), new DeviceContext(false, SchemaConstant.NON_TEMPLATE));
    deviceContextMap.put(
        new PartialPath("root.sg.d2"), new DeviceContext(false, SchemaConstant.NON_TEMPLATE));
    deviceContextMap.put(
        new PartialPath("root.sg.d2.a"), new DeviceContext(true, SchemaConstant.NON_TEMPLATE));

    DeviceRegionScanNode regionScanNode =
        new DeviceRegionScanNode(queryId.genPlanNodeId(), deviceContextMap, false, null);

    PlanNode actualPlan = parseSQLToPlanNode(sql);
    Assert.assertEquals(actualPlan, regionScanNode);
  }

  @Test
  public void testShowDevicesWithTimeConditionWithLimitOffset() throws IllegalPathException {
    String sql = "show devices where time > 1000 limit 20 offset 10";

    QueryId queryId = new QueryId("test");
    // fake initResultNodeContext()
    queryId.genPlanNodeId();

    Map<PartialPath, DeviceContext> deviceContextMap = new HashMap<>();
    deviceContextMap.put(
        new PartialPath("root.sg.d1"), new DeviceContext(false, SchemaConstant.NON_TEMPLATE));
    deviceContextMap.put(
        new PartialPath("root.sg.d2"), new DeviceContext(false, SchemaConstant.NON_TEMPLATE));
    deviceContextMap.put(
        new PartialPath("root.sg.d2.a"), new DeviceContext(true, SchemaConstant.NON_TEMPLATE));

    DeviceRegionScanNode regionScanNode =
        new DeviceRegionScanNode(queryId.genPlanNodeId(), deviceContextMap, false, null);

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

    Map<PartialPath, DeviceContext> deviceContextMap = new HashMap<>();
    deviceContextMap.put(
        new PartialPath("root.sg.d1"), new DeviceContext(false, SchemaConstant.NON_TEMPLATE));
    deviceContextMap.put(
        new PartialPath("root.sg.d2"), new DeviceContext(false, SchemaConstant.NON_TEMPLATE));
    deviceContextMap.put(
        new PartialPath("root.sg.d2.a"), new DeviceContext(true, SchemaConstant.NON_TEMPLATE));

    DeviceRegionScanNode regionScanNode =
        new DeviceRegionScanNode(queryId.genPlanNodeId(), deviceContextMap, true, null);

    PlanNode actualPlan = parseSQLToPlanNode(sql);
    Assert.assertEquals(actualPlan, regionScanNode);
  }

  @Test
  public void testCountTimeseriesWithTimeConditionWithLimitOffset() throws IllegalPathException {
    String sql = "count timeseries where time > 1000";

    QueryId queryId = new QueryId("test");
    // fake initResultNodeContext()
    queryId.genPlanNodeId();

    TimeseriesRegionScanNode regionScanNode =
        new TimeseriesRegionScanNode(
            queryId.genPlanNodeId(), getDeviceToTimeseriesSchemaInfoMap(), true, null);

    PlanNode actualPlan = parseSQLToPlanNode(sql);
    Assert.assertEquals(actualPlan, regionScanNode);
  }

  @Test
  public void serializeDeserializeTest() throws IllegalPathException {

    TimeseriesRegionScanNode timeseriesRegionScanNode =
        new TimeseriesRegionScanNode(
            new PlanNodeId("timeseries_test_id"), getDeviceToTimeseriesSchemaInfoMap(), true, null);

    ByteBuffer buffer = ByteBuffer.allocate(10240);
    timeseriesRegionScanNode.serialize(buffer);
    buffer.flip();
    Assert.assertEquals(timeseriesRegionScanNode, PlanNodeType.deserialize(buffer));
  }
}
