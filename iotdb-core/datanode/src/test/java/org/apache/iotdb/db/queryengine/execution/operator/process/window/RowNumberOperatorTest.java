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

import com.google.common.util.concurrent.ListenableFuture;
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RowNumberOperatorTest {
  private static final ExecutorService instanceNotificationExecutor =
      IoTDBThreadPoolFactory.newFixedThreadPool(1, "rowNumberOperator-test-instance-notification");

  @Test
  public void testRowNumberWithPartition() {
    long[][] timeArray = {{1, 2, 3, 4, 5}};
    String[][] deviceArray = {{"d1", "d1", "d2", "d2", "d2"}};
    int[][] valueArray = {{10, 20, 30, 40, 50}};

    long[] expectedTime = {1, 2, 3, 4, 5};
    String[] expectedDevice = {"d1", "d1", "d2", "d2", "d2"};
    int[] expectedValue = {10, 20, 30, 40, 50};
    long[] expectedRn = {1, 2, 1, 2, 3};

    verifyRowNumberResults(
        timeArray,
        deviceArray,
        valueArray,
        Arrays.asList(1),
        Optional.empty(),
        expectedTime,
        expectedDevice,
        expectedValue,
        expectedRn);
  }

  @Test
  public void testRowNumberWithoutPartition() {
    long[][] timeArray = {{1, 2, 3, 4, 5}};
    String[][] deviceArray = {{"d1", "d1", "d2", "d2", "d2"}};
    int[][] valueArray = {{10, 20, 30, 40, 50}};

    long[] expectedTime = {1, 2, 3, 4, 5};
    String[] expectedDevice = {"d1", "d1", "d2", "d2", "d2"};
    int[] expectedValue = {10, 20, 30, 40, 50};
    long[] expectedRn = {1, 2, 3, 4, 5};

    verifyRowNumberResults(
        timeArray,
        deviceArray,
        valueArray,
        Collections.emptyList(),
        Optional.empty(),
        expectedTime,
        expectedDevice,
        expectedValue,
        expectedRn);
  }

  @Test
  public void testRowNumberWithMaxRowsPerPartition() {
    long[][] timeArray = {{1, 2, 3, 4, 5}};
    String[][] deviceArray = {{"d1", "d1", "d2", "d2", "d2"}};
    int[][] valueArray = {{10, 20, 30, 40, 50}};

    // maxRowsPerPartition=2: d1 keeps 2, d2 keeps 2 (third row skipped)
    long[] expectedTime = {1, 2, 3, 4};
    String[] expectedDevice = {"d1", "d1", "d2", "d2"};
    int[] expectedValue = {10, 20, 30, 40};
    long[] expectedRn = {1, 2, 1, 2};

    verifyRowNumberResults(
        timeArray,
        deviceArray,
        valueArray,
        Arrays.asList(1),
        Optional.of(2),
        expectedTime,
        expectedDevice,
        expectedValue,
        expectedRn);
  }

  @Test
  public void testRowNumberPartitionCrossMultiTsBlocks() {
    long[][] timeArray = {{1, 2, 3}, {4, 5, 6, 7}};
    String[][] deviceArray = {{"d1", "d1", "d2"}, {"d2", "d2", "d3", "d3"}};
    int[][] valueArray = {{10, 20, 30}, {40, 50, 60, 70}};

    long[] expectedTime = {1, 2, 3, 4, 5, 6, 7};
    String[] expectedDevice = {"d1", "d1", "d2", "d2", "d2", "d3", "d3"};
    int[] expectedValue = {10, 20, 30, 40, 50, 60, 70};
    long[] expectedRn = {1, 2, 1, 2, 3, 1, 2};

    verifyRowNumberResults(
        timeArray,
        deviceArray,
        valueArray,
        Arrays.asList(1),
        Optional.empty(),
        expectedTime,
        expectedDevice,
        expectedValue,
        expectedRn);
  }

  @Test
  public void testRowNumberWithEmptyInput() throws Exception {
    long[][] timeArray = {};
    String[][] deviceArray = {};
    int[][] valueArray = {};

    DriverContext driverContext = createDriverContext();
    Operator childOperator = new ChildOperator(timeArray, deviceArray, valueArray, driverContext);

    List<TSDataType> inputDataTypes =
        Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TEXT, TSDataType.INT32);
    List<Integer> outputChannels = Arrays.asList(0, 1, 2);

    try (RowNumberOperator operator =
        new RowNumberOperator(
            driverContext.getOperatorContexts().get(0),
            childOperator,
            inputDataTypes,
            outputChannels,
            Collections.singletonList(1),
            Optional.empty(),
            10)) {
      assertTrue(operator.isFinished());
      assertFalse(operator.hasNext());
    }
  }

  @Test
  public void testRowNumberWithSingleRowPartitions() {
    long[][] timeArray = {{1, 2, 3}};
    String[][] deviceArray = {{"d1", "d2", "d3"}};
    int[][] valueArray = {{10, 20, 30}};

    long[] expectedTime = {1, 2, 3};
    String[] expectedDevice = {"d1", "d2", "d3"};
    int[] expectedValue = {10, 20, 30};
    long[] expectedRn = {1, 1, 1};

    verifyRowNumberResults(
        timeArray,
        deviceArray,
        valueArray,
        Arrays.asList(1),
        Optional.empty(),
        expectedTime,
        expectedDevice,
        expectedValue,
        expectedRn);
  }

  private void verifyRowNumberResults(
      long[][] timeArray,
      String[][] deviceArray,
      int[][] valueArray,
      List<Integer> partitionChannels,
      Optional<Integer> maxRowsPerPartition,
      long[] expectedTime,
      String[] expectedDevice,
      int[] expectedValue,
      long[] expectedRn) {
    int count = 0;
    try (RowNumberOperator operator =
        genRowNumberOperator(
            timeArray, deviceArray, valueArray, partitionChannels, maxRowsPerPartition)) {
      ListenableFuture<?> future = operator.isBlocked();
      future.get();
      while (!operator.isFinished() && operator.hasNext()) {
        TsBlock tsBlock = operator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          for (int i = 0; i < tsBlock.getPositionCount(); i++, count++) {
            assertEquals(expectedTime[count], tsBlock.getColumn(0).getLong(i));
            assertEquals(
                expectedDevice[count],
                tsBlock.getColumn(1).getBinary(i).getStringValue(TSFileConfig.STRING_CHARSET));
            assertEquals(expectedValue[count], tsBlock.getColumn(2).getInt(i));
            assertEquals(expectedRn[count], tsBlock.getColumn(3).getLong(i));
          }
        }
      }
      assertEquals(expectedTime.length, count);
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
    driverContext.addOperatorContext(1, planNode, TreeLinearFillOperator.class.getSimpleName());
    return driverContext;
  }

  private RowNumberOperator genRowNumberOperator(
      long[][] timeArray,
      String[][] deviceArray,
      int[][] valueArray,
      List<Integer> partitionChannels,
      Optional<Integer> maxRowsPerPartition) {
    DriverContext driverContext = createDriverContext();

    List<TSDataType> inputDataTypes =
        Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TEXT, TSDataType.INT32);
    List<Integer> outputChannels = new ArrayList<>();
    for (int i = 0; i < inputDataTypes.size(); i++) {
      outputChannels.add(i);
    }

    Operator childOperator = new ChildOperator(timeArray, deviceArray, valueArray, driverContext);
    return new RowNumberOperator(
        driverContext.getOperatorContexts().get(0),
        childOperator,
        inputDataTypes,
        outputChannels,
        partitionChannels,
        maxRowsPerPartition,
        10);
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
