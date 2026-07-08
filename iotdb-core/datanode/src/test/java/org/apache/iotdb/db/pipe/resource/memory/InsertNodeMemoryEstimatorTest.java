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

package org.apache.iotdb.db.pipe.resource.memory;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InsertNodeMemoryEstimatorTest {

  @Test
  public void testInsertRowsNodeLaterRowSizeIsEstimated() throws IllegalPathException {
    InsertRowNode firstRow =
        createTextInsertRowNode("child-1", "root.sg.d1", new String[] {"s1"}, new String[] {"v1"});
    InsertRowNode smallSecondRow =
        createTextInsertRowNode("child-2", "root.sg.d2", new String[] {"s1"}, new String[] {"v2"});
    InsertRowNode largeSecondRow =
        createTextInsertRowNode(
            "child-3",
            "root.sg.device_with_a_longer_path_segment",
            new String[] {"s1", "measurement_with_a_longer_name", "s3"},
            new String[] {"v2", repeatedString("payload", 32), repeatedString("payload", 48)});

    long baselineSize =
        InsertNodeMemoryEstimator.sizeOf(createInsertRowsNode("parent", firstRow, smallSecondRow));
    long largerNodeSize =
        InsertNodeMemoryEstimator.sizeOf(createInsertRowsNode("parent", firstRow, largeSecondRow));

    Assert.assertTrue(largerNodeSize > baselineSize);
  }

  @Test
  public void testInsertRowsNodeResultsAreEstimated() throws IllegalPathException {
    InsertRowsNode node =
        createInsertRowsNode(
            "parent",
            createTextInsertRowNode(
                "child-1", "root.sg.d1", new String[] {"s1"}, new String[] {"v1"}),
            createTextInsertRowNode(
                "child-2", "root.sg.d2", new String[] {"s1"}, new String[] {"v2"}));

    long sizeWithoutResults = InsertNodeMemoryEstimator.sizeOf(node);

    TSStatus statusWithoutSubStatus = createStatus("outer-message");
    node.getResults().put(1, statusWithoutSubStatus);
    long sizeWithResults = InsertNodeMemoryEstimator.sizeOf(node);

    TSStatus statusWithSubStatus = createStatus("outer-message");
    List<TSStatus> subStatusList = new ArrayList<>();
    subStatusList.add(createStatus(repeatedString("inner-message", 16)));
    statusWithSubStatus.setSubStatus(subStatusList);
    node.getResults().put(1, statusWithSubStatus);
    long sizeWithSubStatus = InsertNodeMemoryEstimator.sizeOf(node);

    Assert.assertTrue(sizeWithResults > sizeWithoutResults);
    Assert.assertTrue(sizeWithSubStatus > sizeWithResults);
  }

  @Test
  public void testInsertRowsOfOneDeviceNodeLaterRowSizeIsEstimated() throws IllegalPathException {
    InsertRowNode firstRow =
        createTextInsertRowNode(
            "child-1", "root.sg.d1", new String[] {"s1", "s2"}, new String[] {"v1", "v2"});
    InsertRowNode smallSecondRow =
        createTextInsertRowNode(
            "child-2", "root.sg.d1", new String[] {"s1", "s2"}, new String[] {"v3", "v4"});
    InsertRowNode largeSecondRow =
        createTextInsertRowNode(
            "child-3",
            "root.sg.d1",
            new String[] {"s1", "s2"},
            new String[] {repeatedString("payload", 32), repeatedString("payload", 48)});

    long baselineSize =
        InsertNodeMemoryEstimator.sizeOf(
            createInsertRowsOfOneDeviceNode("parent", firstRow, smallSecondRow));
    long largerNodeSize =
        InsertNodeMemoryEstimator.sizeOf(
            createInsertRowsOfOneDeviceNode("parent", firstRow, largeSecondRow));

    Assert.assertTrue(largerNodeSize > baselineSize);
  }

  @Test
  public void testInsertMultiTabletsNodeLaterTabletSizeIsEstimated() throws IllegalPathException {
    InsertTabletNode firstTablet = createTextInsertTabletNode("child-1", "root.sg.d1", 1, 1, 2);
    InsertTabletNode smallSecondTablet =
        createTextInsertTabletNode("child-2", "root.sg.d2", 1, 1, 2);
    InsertTabletNode largeSecondTablet =
        createTextInsertTabletNode("child-3", "root.sg.d3", 3, 8, 16);

    long baselineSize =
        InsertNodeMemoryEstimator.sizeOf(
            createInsertMultiTabletsNode("parent", firstTablet, smallSecondTablet));
    long largerNodeSize =
        InsertNodeMemoryEstimator.sizeOf(
            createInsertMultiTabletsNode("parent", firstTablet, largeSecondTablet));

    Assert.assertTrue(largerNodeSize > baselineSize);
  }

  @Test
  public void testInsertTabletNodeWithNullColumnIsEstimated() throws IllegalPathException {
    InsertTabletNode tablet = createTextInsertTabletNode("tablet", "root.sg.d1", 2, 4, 8);

    long fullSize = InsertNodeMemoryEstimator.sizeOf(tablet);
    tablet.getColumns()[1] = null;
    long sizeWithNullColumn = InsertNodeMemoryEstimator.sizeOf(tablet);

    Assert.assertTrue(sizeWithNullColumn > 0);
    Assert.assertTrue(sizeWithNullColumn < fullSize);
  }

  @Test
  public void testPlanNodeIdIsEstimated() throws IllegalPathException {
    InsertRowNode shortPlanNodeIdRow =
        createTextInsertRowNode("id", "root.sg.d1", new String[] {"s1"}, new String[] {"v1"});
    InsertRowNode longPlanNodeIdRow =
        createTextInsertRowNode(
            repeatedString("plan-node-id", 12),
            "root.sg.d1",
            new String[] {"s1"},
            new String[] {"v1"});

    Assert.assertTrue(
        InsertNodeMemoryEstimator.sizeOf(longPlanNodeIdRow)
            > InsertNodeMemoryEstimator.sizeOf(shortPlanNodeIdRow));
  }

  private static InsertRowsNode createInsertRowsNode(
      String planNodeId, InsertRowNode... insertRowNodes) {
    InsertRowsNode node = new InsertRowsNode(new PlanNodeId(planNodeId));
    for (int i = 0; i < insertRowNodes.length; i++) {
      node.addOneInsertRowNode(insertRowNodes[i], i);
    }
    node.setTargetPath(insertRowNodes[0].getTargetPath());
    node.setMeasurementSchemas(insertRowNodes[0].getMeasurementSchemas());
    node.setMeasurements(insertRowNodes[0].getMeasurements());
    node.setDataTypes(insertRowNodes[0].getDataTypes());
    return node;
  }

  private static InsertRowsOfOneDeviceNode createInsertRowsOfOneDeviceNode(
      String planNodeId, InsertRowNode... insertRowNodes) {
    InsertRowsOfOneDeviceNode node = new InsertRowsOfOneDeviceNode(new PlanNodeId(planNodeId));
    List<InsertRowNode> rows = new ArrayList<>(Arrays.asList(insertRowNodes));
    List<Integer> indexes = new ArrayList<>();
    for (int i = 0; i < insertRowNodes.length; i++) {
      indexes.add(i);
    }
    node.setInsertRowNodeList(rows);
    node.setInsertRowNodeIndexList(indexes);
    return node;
  }

  private static InsertMultiTabletsNode createInsertMultiTabletsNode(
      String planNodeId, InsertTabletNode... insertTabletNodes) {
    InsertMultiTabletsNode node = new InsertMultiTabletsNode(new PlanNodeId(planNodeId));
    for (int i = 0; i < insertTabletNodes.length; i++) {
      node.addInsertTabletNode(insertTabletNodes[i], i);
    }
    return node;
  }

  private static InsertRowNode createTextInsertRowNode(
      String planNodeId, String devicePath, String[] measurements, String[] values)
      throws IllegalPathException {
    TSDataType[] dataTypes = new TSDataType[measurements.length];
    MeasurementSchema[] measurementSchemas = new MeasurementSchema[measurements.length];
    Object[] rowValues = new Object[measurements.length];
    for (int i = 0; i < measurements.length; i++) {
      dataTypes[i] = TSDataType.TEXT;
      measurementSchemas[i] = new MeasurementSchema(measurements[i], TSDataType.TEXT);
      rowValues[i] = new Binary(values[i], TSFileConfig.STRING_CHARSET);
    }
    return new InsertRowNode(
        new PlanNodeId(planNodeId),
        new PartialPath(devicePath),
        false,
        measurements,
        dataTypes,
        measurementSchemas,
        1L,
        rowValues,
        false);
  }

  private static InsertTabletNode createTextInsertTabletNode(
      String planNodeId, String devicePath, int measurementCount, int rowCount, int repeatCount)
      throws IllegalPathException {
    String[] measurements = new String[measurementCount];
    TSDataType[] dataTypes = new TSDataType[measurementCount];
    MeasurementSchema[] measurementSchemas = new MeasurementSchema[measurementCount];
    Object[] columns = new Object[measurementCount];
    for (int measurementIndex = 0; measurementIndex < measurementCount; measurementIndex++) {
      measurements[measurementIndex] = "s" + measurementIndex;
      dataTypes[measurementIndex] = TSDataType.TEXT;
      measurementSchemas[measurementIndex] =
          new MeasurementSchema(measurements[measurementIndex], TSDataType.TEXT);
      Binary[] values = new Binary[rowCount];
      for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        values[rowIndex] =
            new Binary(
                repeatedString("value-" + measurementIndex + "-" + rowIndex, repeatCount),
                TSFileConfig.STRING_CHARSET);
      }
      columns[measurementIndex] = values;
    }

    long[] times = new long[rowCount];
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      times[rowIndex] = rowIndex;
    }

    return new InsertTabletNode(
        new PlanNodeId(planNodeId),
        new PartialPath(devicePath),
        false,
        measurements,
        dataTypes,
        measurementSchemas,
        times,
        null,
        columns,
        rowCount);
  }

  private static TSStatus createStatus(String message) {
    TSStatus status = new TSStatus();
    status.setCode(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    status.setMessage(message);
    return status;
  }

  private static String repeatedString(String unit, int repeatCount) {
    StringBuilder builder = new StringBuilder(unit.length() * repeatCount);
    for (int i = 0; i < repeatCount; i++) {
      builder.append(unit);
    }
    return builder.toString();
  }
}
