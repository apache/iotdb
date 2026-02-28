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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LimitKRankingOperatorTest {
  private static final ExecutorService instanceNotificationExecutor =
      IoTDBThreadPoolFactory.newFixedThreadPool(1, "limitKRanking-test-instance-notification");

  private static final List<TSDataType> INPUT_DATA_TYPES =
      Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TEXT, TSDataType.INT32);

  @Test
  public void testSingleBlockMultiPartition() {
    // Input: d1 has 3 rows, d2 has 3 rows; K=2 → keep first 2 per partition
    long[][] timeArray = {{1, 2, 3, 4, 5, 6}};
    String[][] deviceArray = {{"d1", "d1", "d1", "d2", "d2", "d2"}};
    int[][] valueArray = {{10, 20, 30, 40, 50, 60}};

    long[] expectTime = {1, 2, 4, 5};
    String[] expectDevice = {"d1", "d1", "d2", "d2"};
    int[] expectValue = {10, 20, 40, 50};

    verifyOperatorOutput(
        timeArray, deviceArray, valueArray, 2, false, expectTime, expectDevice, expectValue, null);
  }

  @Test
  public void testPartitionCrossMultiBlocks() {
    // d1 spans block0 and block1; d2 spans block1 and block2; K=3
    long[][] timeArray = {{1, 2}, {3, 4, 5}, {6, 7, 8}};
    String[][] deviceArray = {{"d1", "d1"}, {"d1", "d1", "d2"}, {"d2", "d2", "d2"}};
    int[][] valueArray = {{10, 20}, {30, 40, 50}, {60, 70, 80}};

    long[] expectTime = {1, 2, 3, 5, 6, 7};
    String[] expectDevice = {"d1", "d1", "d1", "d2", "d2", "d2"};
    int[] expectValue = {10, 20, 30, 50, 60, 70};

    verifyOperatorOutput(
        timeArray, deviceArray, valueArray, 3, false, expectTime, expectDevice, expectValue, null);
  }

  @Test
  public void testWithRowNumber() {
    long[][] timeArray = {{1, 2, 3, 4}};
    String[][] deviceArray = {{"d1", "d1", "d2", "d2"}};
    int[][] valueArray = {{10, 20, 30, 40}};

    long[] expectTime = {1, 2, 3, 4};
    String[] expectDevice = {"d1", "d1", "d2", "d2"};
    int[] expectValue = {10, 20, 30, 40};
    long[] expectRowNumber = {1, 2, 1, 2};

    verifyOperatorOutput(
        timeArray,
        deviceArray,
        valueArray,
        2,
        true,
        expectTime,
        expectDevice,
        expectValue,
        expectRowNumber);
  }

  @Test
  public void testKLargerThanData() {
    // K=10, but only 2 rows per partition → all rows emitted
    long[][] timeArray = {{1, 2, 3, 4}};
    String[][] deviceArray = {{"d1", "d1", "d2", "d2"}};
    int[][] valueArray = {{10, 20, 30, 40}};

    long[] expectTime = {1, 2, 3, 4};
    String[] expectDevice = {"d1", "d1", "d2", "d2"};
    int[] expectValue = {10, 20, 30, 40};

    verifyOperatorOutput(
        timeArray, deviceArray, valueArray, 10, false, expectTime, expectDevice, expectValue, null);
  }

  @Test
  public void testEntireBlockFiltered() {
    // K=2, d1 gets 2 rows in block0 → block1 (all d1) should be fully filtered
    long[][] timeArray = {{1, 2}, {3, 4}};
    String[][] deviceArray = {{"d1", "d1"}, {"d1", "d1"}};
    int[][] valueArray = {{10, 20}, {30, 40}};

    long[] expectTime = {1, 2};
    String[] expectDevice = {"d1", "d1"};
    int[] expectValue = {10, 20};

    verifyOperatorOutput(
        timeArray, deviceArray, valueArray, 2, false, expectTime, expectDevice, expectValue, null);
  }

  @Test
  public void testNoPartition() {
    // No partition columns → global limit K=3
    long[][] timeArray = {{1, 2, 3, 4, 5}};
    String[][] deviceArray = {{"d1", "d1", "d2", "d2", "d2"}};
    int[][] valueArray = {{10, 20, 30, 40, 50}};

    long[] expectTime = {1, 2, 3};
    String[] expectDevice = {"d1", "d1", "d2"};
    int[] expectValue = {10, 20, 30};

    verifyOperatorOutputNoPartition(
        timeArray, deviceArray, valueArray, 3, false, expectTime, expectDevice, expectValue);
  }

  @Test
  public void testPartialFilterInBlock() {
    // Block has mixed: some rows pass, some are filtered
    // d1: 4 rows, K=2 → only first 2 of d1 pass; d2: 2 rows, K=2 → both pass
    long[][] timeArray = {{1, 2, 3, 4, 5, 6}};
    String[][] deviceArray = {{"d1", "d1", "d1", "d1", "d2", "d2"}};
    int[][] valueArray = {{10, 20, 30, 40, 50, 60}};

    long[] expectTime = {1, 2, 5, 6};
    String[] expectDevice = {"d1", "d1", "d2", "d2"};
    int[] expectValue = {10, 20, 50, 60};

    verifyOperatorOutput(
        timeArray, deviceArray, valueArray, 2, false, expectTime, expectDevice, expectValue, null);
  }

  @Test
  public void testNullInputBlock() {
    // ChildOperator can return null TsBlocks
    long[][] timeArray = {{1, 2}, null, {3, 4}};
    String[][] deviceArray = {{"d1", "d1"}, null, {"d2", "d2"}};
    int[][] valueArray = {{10, 20}, null, {30, 40}};

    long[] expectTime = {1, 2, 3, 4};
    String[] expectDevice = {"d1", "d1", "d2", "d2"};
    int[] expectValue = {10, 20, 30, 40};

    verifyOperatorOutput(
        timeArray, deviceArray, valueArray, 5, false, expectTime, expectDevice, expectValue, null);
  }

  // ======================== Helper Methods ========================

  private void verifyOperatorOutput(
      long[][] timeArray,
      String[][] deviceArray,
      int[][] valueArray,
      int k,
      boolean produceRowNumber,
      long[] expectTime,
      String[] expectDevice,
      int[] expectValue,
      long[] expectRowNumber) {
    int count = 0;
    try (LimitKRankingOperator operator =
        createOperator(timeArray, deviceArray, valueArray, k, produceRowNumber, true)) {
      ListenableFuture<?> listenableFuture = operator.isBlocked();
      listenableFuture.get();
      while (!operator.isFinished() && operator.hasNext()) {
        TsBlock tsBlock = operator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++, count++) {
            assertEquals(expectTime[count], tsBlock.getColumn(0).getLong(i));
            assertEquals(
                expectDevice[count],
                tsBlock.getColumn(1).getBinary(i).getStringValue(TSFileConfig.STRING_CHARSET));
            assertEquals(expectValue[count], tsBlock.getColumn(2).getInt(i));
            if (produceRowNumber && expectRowNumber != null) {
              assertEquals(expectRowNumber[count], tsBlock.getColumn(3).getLong(i));
            }
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertEquals(expectTime.length, count);
  }

  private void verifyOperatorOutputNoPartition(
      long[][] timeArray,
      String[][] deviceArray,
      int[][] valueArray,
      int k,
      boolean produceRowNumber,
      long[] expectTime,
      String[] expectDevice,
      int[] expectValue) {
    int count = 0;
    try (LimitKRankingOperator operator =
        createOperator(timeArray, deviceArray, valueArray, k, produceRowNumber, false)) {
      ListenableFuture<?> listenableFuture = operator.isBlocked();
      listenableFuture.get();
      while (!operator.isFinished() && operator.hasNext()) {
        TsBlock tsBlock = operator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++, count++) {
            assertEquals(expectTime[count], tsBlock.getColumn(0).getLong(i));
            assertEquals(
                expectDevice[count],
                tsBlock.getColumn(1).getBinary(i).getStringValue(TSFileConfig.STRING_CHARSET));
            assertEquals(expectValue[count], tsBlock.getColumn(2).getInt(i));
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertEquals(expectTime.length, count);
  }

  private LimitKRankingOperator createOperator(
      long[][] timeArray,
      String[][] deviceArray,
      int[][] valueArray,
      int k,
      boolean produceRowNumber,
      boolean hasPartition) {
    QueryId queryId = new QueryId("stub_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNodeId = new PlanNodeId("1");
    driverContext.addOperatorContext(1, planNodeId, LimitKRankingOperator.class.getSimpleName());

    Operator childOperator = new ChildOperator(timeArray, deviceArray, valueArray, driverContext);

    List<Integer> outputChannels = Arrays.asList(0, 1, 2);
    List<Integer> partitionChannels;
    List<TSDataType> partitionTSDataTypes;
    if (hasPartition) {
      partitionChannels = Collections.singletonList(1);
      partitionTSDataTypes = Collections.singletonList(TSDataType.TEXT);
    } else {
      partitionChannels = Collections.emptyList();
      partitionTSDataTypes = Collections.emptyList();
    }

    return new LimitKRankingOperator(
        driverContext.getOperatorContexts().get(0),
        childOperator,
        INPUT_DATA_TYPES,
        outputChannels,
        partitionChannels,
        partitionTSDataTypes,
        k,
        produceRowNumber,
        16);
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
      if (timeArray[index] == null) {
        index++;
        return null;
      }
      TsBlockBuilder builder =
          new TsBlockBuilder(
              timeArray[index].length,
              Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TEXT, TSDataType.INT32));
      for (int i = 0, size = timeArray[index].length; i < size; i++) {
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
