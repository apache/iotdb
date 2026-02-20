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

package org.apache.iotdb.db.queryengine.execution.operator.process.window;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.TreeLinearFillOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKRankingNode;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TopKRankingOperatorTest {
  private static final ExecutorService instanceNotificationExecutor =
      IoTDBThreadPoolFactory.newFixedThreadPool(
          1, "topKRankingOperator-test-instance-notification");

  @Test
  public void testTopKWithPartition() {
    // Input: 4 rows for d1, 3 rows for d2
    // Sort by value (column 2) ascending, top 2 per partition
    long[][] timeArray = {{1, 2, 3, 4, 5, 6, 7}};
    String[][] deviceArray = {{"d1", "d1", "d1", "d1", "d2", "d2", "d2"}};
    int[][] valueArray = {{5, 3, 1, 4, 6, 2, 1}};

    // Expected: top 2 per partition sorted by value ASC
    // d1: value=1(rn=1), value=3(rn=2)
    // d2: value=1(rn=1), value=2(rn=2)
    Map<String, List<int[]>> expectedByDevice = new HashMap<>();
    expectedByDevice.put("d1", Arrays.asList(new int[] {1, 1}, new int[] {3, 2}));
    expectedByDevice.put("d2", Arrays.asList(new int[] {1, 1}, new int[] {2, 2}));

    verifyTopKResultsByPartition(
        timeArray,
        deviceArray,
        valueArray,
        Collections.singletonList(1),
        Collections.singletonList(TSDataType.TEXT),
        Collections.singletonList(2),
        Collections.singletonList(SortOrder.ASC_NULLS_LAST),
        2,
        false,
        expectedByDevice,
        4);
  }

  @Test
  public void testTopKWithPartitionDescending() {
    long[][] timeArray = {{1, 2, 3, 4, 5, 6}};
    String[][] deviceArray = {{"d1", "d1", "d1", "d2", "d2", "d2"}};
    int[][] valueArray = {{5, 3, 1, 6, 2, 4}};

    // top 2 per partition sorted by value DESC
    // d1: value=5(rn=1), value=3(rn=2)
    // d2: value=6(rn=1), value=4(rn=2)
    Map<String, List<int[]>> expectedByDevice = new HashMap<>();
    expectedByDevice.put("d1", Arrays.asList(new int[] {5, 1}, new int[] {3, 2}));
    expectedByDevice.put("d2", Arrays.asList(new int[] {6, 1}, new int[] {4, 2}));

    verifyTopKResultsByPartition(
        timeArray,
        deviceArray,
        valueArray,
        Collections.singletonList(1),
        Collections.singletonList(TSDataType.TEXT),
        Collections.singletonList(2),
        Collections.singletonList(SortOrder.DESC_NULLS_LAST),
        2,
        false,
        expectedByDevice,
        4);
  }

  @Test
  public void testTopKWithoutPartition() {
    // No partition: all rows in one group
    long[][] timeArray = {{1, 2, 3, 4, 5}};
    String[][] deviceArray = {{"d1", "d1", "d2", "d2", "d2"}};
    int[][] valueArray = {{5, 3, 1, 4, 2}};

    // top 3 globally sorted by value ASC: value=1(rn=1), value=2(rn=2), value=3(rn=3)
    int[][] expectedValueAndRn = {{1, 1}, {2, 2}, {3, 3}};

    verifyTopKResultsGlobal(
        timeArray,
        deviceArray,
        valueArray,
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.singletonList(2),
        Collections.singletonList(SortOrder.ASC_NULLS_LAST),
        3,
        false,
        expectedValueAndRn,
        3);
  }

  @Test
  public void testTopKWithMultipleTsBlocks() {
    long[][] timeArray = {{1, 2, 3}, {4, 5}, {6, 7}};
    String[][] deviceArray = {{"d1", "d1", "d1"}, {"d2", "d2"}, {"d2", "d2"}};
    int[][] valueArray = {{5, 3, 1}, {6, 2}, {4, 1}};

    // top 2 per partition sorted by value ASC
    // d1: value=1(rn=1), value=3(rn=2)
    // d2: value=1(rn=1), value=2(rn=2)
    Map<String, List<int[]>> expectedByDevice = new HashMap<>();
    expectedByDevice.put("d1", Arrays.asList(new int[] {1, 1}, new int[] {3, 2}));
    expectedByDevice.put("d2", Arrays.asList(new int[] {1, 1}, new int[] {2, 2}));

    verifyTopKResultsByPartition(
        timeArray,
        deviceArray,
        valueArray,
        Collections.singletonList(1),
        Collections.singletonList(TSDataType.TEXT),
        Collections.singletonList(2),
        Collections.singletonList(SortOrder.ASC_NULLS_LAST),
        2,
        false,
        expectedByDevice,
        4);
  }

  @Test
  public void testTopKWithTopOne() {
    long[][] timeArray = {{1, 2, 3, 4}};
    String[][] deviceArray = {{"d1", "d1", "d2", "d2"}};
    int[][] valueArray = {{5, 3, 6, 2}};

    // top 1 per partition sorted by value ASC
    // d1: value=3(rn=1)
    // d2: value=2(rn=1)
    Map<String, List<int[]>> expectedByDevice = new HashMap<>();
    expectedByDevice.put("d1", Collections.singletonList(new int[] {3, 1}));
    expectedByDevice.put("d2", Collections.singletonList(new int[] {2, 1}));

    verifyTopKResultsByPartition(
        timeArray,
        deviceArray,
        valueArray,
        Collections.singletonList(1),
        Collections.singletonList(TSDataType.TEXT),
        Collections.singletonList(2),
        Collections.singletonList(SortOrder.ASC_NULLS_LAST),
        1,
        false,
        expectedByDevice,
        2);
  }

  /**
   * Verifies top-K results grouped by partition (device). The output order between partitions is
   * not guaranteed, so we group results by device and verify each partition independently.
   */
  private void verifyTopKResultsByPartition(
      long[][] timeArray,
      String[][] deviceArray,
      int[][] valueArray,
      List<Integer> partitionChannels,
      List<TSDataType> partitionTypes,
      List<Integer> sortChannels,
      List<SortOrder> sortOrders,
      int maxRowCountPerPartition,
      boolean partial,
      Map<String, List<int[]>> expectedByDevice,
      int expectedTotalCount) {

    Map<String, List<int[]>> actualByDevice = new HashMap<>();
    int count = 0;

    try (TopKRankingOperator operator =
        genTopKRankingOperator(
            timeArray,
            deviceArray,
            valueArray,
            partitionChannels,
            partitionTypes,
            sortChannels,
            sortOrders,
            maxRowCountPerPartition,
            partial)) {
      while (!operator.isFinished()) {
        if (operator.hasNext()) {
          TsBlock tsBlock = operator.next();
          if (tsBlock != null && !tsBlock.isEmpty()) {
            int numColumns = tsBlock.getValueColumnCount();
            for (int i = 0; i < tsBlock.getPositionCount(); i++, count++) {
              String device =
                  tsBlock
                      .getColumn(1)
                      .getBinary(i)
                      .getStringValue(TSFileConfig.STRING_CHARSET);
              int value = tsBlock.getColumn(2).getInt(i);
              long rowNumber = tsBlock.getColumn(numColumns - 1).getLong(i);
              actualByDevice
                  .computeIfAbsent(device, k -> new ArrayList<>())
                  .add(new int[] {value, (int) rowNumber});
            }
          }
        }
      }
      assertEquals(expectedTotalCount, count);

      for (Map.Entry<String, List<int[]>> entry : expectedByDevice.entrySet()) {
        String device = entry.getKey();
        List<int[]> expectedRows = entry.getValue();
        List<int[]> actualRows = actualByDevice.get(device);

        assertTrue("Missing partition for device: " + device, actualRows != null);
        assertEquals(
            "Row count mismatch for device " + device, expectedRows.size(), actualRows.size());
        for (int i = 0; i < expectedRows.size(); i++) {
          assertEquals(
              "Value mismatch at row " + i + " for device " + device,
              expectedRows.get(i)[0],
              actualRows.get(i)[0]);
          assertEquals(
              "Row number mismatch at row " + i + " for device " + device,
              expectedRows.get(i)[1],
              actualRows.get(i)[1]);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void verifyTopKResultsGlobal(
      long[][] timeArray,
      String[][] deviceArray,
      int[][] valueArray,
      List<Integer> partitionChannels,
      List<TSDataType> partitionTypes,
      List<Integer> sortChannels,
      List<SortOrder> sortOrders,
      int maxRowCountPerPartition,
      boolean partial,
      int[][] expectedValueAndRn,
      int expectedTotalCount) {

    List<int[]> results = new ArrayList<>();
    int count = 0;

    try (TopKRankingOperator operator =
        genTopKRankingOperator(
            timeArray,
            deviceArray,
            valueArray,
            partitionChannels,
            partitionTypes,
            sortChannels,
            sortOrders,
            maxRowCountPerPartition,
            partial)) {
      while (!operator.isFinished()) {
        if (operator.hasNext()) {
          TsBlock tsBlock = operator.next();
          if (tsBlock != null && !tsBlock.isEmpty()) {
            int numColumns = tsBlock.getValueColumnCount();
            for (int i = 0; i < tsBlock.getPositionCount(); i++, count++) {
              int value = tsBlock.getColumn(2).getInt(i);
              long rowNumber = tsBlock.getColumn(numColumns - 1).getLong(i);
              results.add(new int[] {value, (int) rowNumber});
            }
          }
        }
      }
      assertEquals(expectedTotalCount, count);
      for (int i = 0; i < expectedValueAndRn.length; i++) {
        assertEquals(
            "Value mismatch at row " + i, expectedValueAndRn[i][0], results.get(i)[0]);
        assertEquals(
            "Row number mismatch at row " + i, expectedValueAndRn[i][1], results.get(i)[1]);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private DriverContext createDriverContext() {
    QueryId queryId = new QueryId("stub_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNode = new PlanNodeId("1");
    driverContext.addOperatorContext(
        1, planNode, TreeLinearFillOperator.class.getSimpleName());
    return driverContext;
  }

  private TopKRankingOperator genTopKRankingOperator(
      long[][] timeArray,
      String[][] deviceArray,
      int[][] valueArray,
      List<Integer> partitionChannels,
      List<TSDataType> partitionTypes,
      List<Integer> sortChannels,
      List<SortOrder> sortOrders,
      int maxRowCountPerPartition,
      boolean partial) {
    DriverContext driverContext = createDriverContext();

    List<TSDataType> inputDataTypes =
        Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TEXT, TSDataType.INT32);
    List<Integer> outputChannels = new ArrayList<>();
    for (int i = 0; i < inputDataTypes.size(); i++) {
      outputChannels.add(i);
    }

    Operator childOperator = new ChildOperator(timeArray, deviceArray, valueArray, driverContext);
    return new TopKRankingOperator(
        driverContext.getOperatorContexts().get(0),
        childOperator,
        TopKRankingNode.RankingType.ROW_NUMBER,
        inputDataTypes,
        outputChannels,
        partitionChannels,
        partitionTypes,
        sortChannels,
        sortOrders,
        maxRowCountPerPartition,
        partial,
        Optional.empty(),
        10,
        Optional.empty());
  }

  static class ChildOperator implements Operator {
    private int index;
    private final long[][] timeArray;
    private final String[][] deviceArray;
    private final int[][] valueArray;
    private final DriverContext driverContext;

    ChildOperator(
        long[][] timeArray,
        String[][] deviceArray,
        int[][] valueArray,
        DriverContext driverContext) {
      this.timeArray = timeArray;
      this.deviceArray = deviceArray;
      this.valueArray = valueArray;
      this.driverContext = driverContext;
      this.index = 0;
    }

    @Override
    public OperatorContext getOperatorContext() {
      return driverContext.getOperatorContexts().get(0);
    }

    @Override
    public TsBlock next() {
      if (index >= timeArray.length) {
        return null;
      }
      TsBlockBuilder builder =
          new TsBlockBuilder(
              timeArray[index].length,
              Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TEXT, TSDataType.INT32));
      for (int i = 0; i < timeArray[index].length; i++) {
        builder.getColumnBuilder(0).writeLong(timeArray[index][i]);
        builder
            .getColumnBuilder(1)
            .writeBinary(new Binary(deviceArray[index][i], TSFileConfig.STRING_CHARSET));
        builder.getColumnBuilder(2).writeInt(valueArray[index][i]);
      }
      builder.declarePositions(timeArray[index].length);
      index++;
      return builder.build(
          new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, builder.getPositionCount()));
    }

    @Override
    public boolean hasNext() {
      return index < timeArray.length;
    }

    @Override
    public boolean isFinished() {
      return index >= timeArray.length;
    }

    @Override
    public void close() {}

    @Override
    public long calculateMaxPeekMemory() {
      return 0;
    }

    @Override
    public long calculateMaxReturnSize() {
      return 0;
    }

    @Override
    public long calculateRetainedSizeAfterCallingNext() {
      return 0;
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }
  }
}
