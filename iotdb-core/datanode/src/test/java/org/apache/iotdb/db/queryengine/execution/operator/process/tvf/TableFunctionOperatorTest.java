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

package org.apache.iotdb.db.queryengine.execution.operator.process.tvf;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.function.PartitionRecognizer;
import org.apache.iotdb.db.queryengine.execution.operator.process.function.partition.PartitionState;
import org.apache.iotdb.db.queryengine.execution.operator.process.function.partition.Slice;
import org.apache.iotdb.udf.api.relational.access.Record;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TableFunctionOperatorTest {
  private static final ExecutorService instanceNotificationExecutor =
      IoTDBThreadPoolFactory.newFixedThreadPool(
          1, "TableFunctionOperator-test-instance-notification");
  private static final Binary D1_BINARY = new Binary("d1", TSFileConfig.STRING_CHARSET);
  private static final Binary D2_BINARY = new Binary("d2", TSFileConfig.STRING_CHARSET);
  private static final Binary D3_BINARY = new Binary("d3", TSFileConfig.STRING_CHARSET);
  private static final Binary D5_BINARY = new Binary("d5", TSFileConfig.STRING_CHARSET);

  @AfterClass
  public static void tearDown() {
    instanceNotificationExecutor.shutdown();
  }

  // child output
  // Time,              deviceId,   temp,     hum
  // 1717171200000         d1       27.1      60
  // 1719763200000         d1       27.2      60
  // ------------------------------------------------ TsBlock-1
  // 1722441600000         d1       27.3      61
  // 1725120000000         d2       29.3      62
  // 1725130000000         d2       29.3      62
  // 1725140000000         d2       29.3      63
  // ------------------------------------------------ TsBlock-2
  // empty block
  // ------------------------------------------------ TsBlock-3
  // null block
  // ------------------------------------------------ TsBlock-4
  // 1722441600000         d3       27.1      63
  // 1722441800000         d3       26.9      64
  // 1722551600000         d5       25.8      64
  // ------------------------------------------------ TsBlock-5
  // 1722552800000         d5       26.9      65
  // 1722553000000         d5       25.8      65
  // ------------------------------------------------ TsBlock-6
  private Operator constructChildOperator(DriverContext driverContext) {
    return new Operator() {

      private final long[][] timeArray =
          new long[][] {
            {1717171200000L, 1719763200000L},
            {1722441600000L, 1725120000000L, 1725130000000L, 1725140000000L},
            {},
            null,
            {1722441600000L, 1722441800000L, 1722551600000L},
            {1722552800000L, 1722553000000L},
          };

      private final double[][] tempArray =
          new double[][] {
            {27.1, 27.2}, {27.3, 29.3, 29.3, 29.3}, {}, null, {27.1, 26.9, 25.8}, {26.9, 25.8},
          };

      private final int[][] humArray =
          new int[][] {
            {60, 60}, {61, 62, 62, 63}, {}, null, {63, 64, 64}, {65, 65},
          };

      private final Binary[][] deviceIdArray =
          new Binary[][] {
            {D1_BINARY, D1_BINARY},
            {D1_BINARY, D2_BINARY, D2_BINARY, D2_BINARY},
            {},
            null,
            {D3_BINARY, D3_BINARY, D5_BINARY},
            {D5_BINARY, D5_BINARY},
          };

      private int index = 0;

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
                Arrays.asList(
                    TSDataType.TIMESTAMP, TSDataType.STRING, TSDataType.DOUBLE, TSDataType.INT32));
        for (int i = 0, size = timeArray[index].length; i < size; i++) {
          builder.getColumnBuilder(0).writeLong(timeArray[index][i]);
          builder.getColumnBuilder(1).writeBinary(deviceIdArray[index][i]);
          builder.getColumnBuilder(2).writeDouble(tempArray[index][i]);
          builder.getColumnBuilder(3).writeInt(humArray[index][i]);
        }
        builder.declarePositions(timeArray[index].length);
        index++;
        return builder.build(
            new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, builder.getPositionCount()));
      }

      @Override
      public boolean hasNext() throws Exception {
        return index < timeArray.length;
      }

      @Override
      public void close() throws Exception {}

      @Override
      public boolean isFinished() throws Exception {
        return index >= tempArray.length;
      }

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
    };
  }

  @Test
  public void testPartitionRecognizer() {
    QueryId queryId = new QueryId("stub_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    try (Operator childOperator = constructChildOperator(driverContext)) {
      PartitionRecognizer partitionRecognizer =
          new PartitionRecognizer(
              Collections.singletonList(1),
              Arrays.asList(0, 1, 3),
              Arrays.asList(0, 1, 2, 3),
              Arrays.asList(TSDataType.TIMESTAMP, TSDataType.STRING, TSDataType.DOUBLE, TSDataType.INT32));
      PartitionState state = partitionRecognizer.nextState();
      Assert.assertEquals(PartitionState.INIT_STATE, state);
      Assert.assertEquals(PartitionState.INIT_STATE, partitionRecognizer.nextState());
      // 1. add first TsBlock, expected NEW_PARTITION and NEED_MORE_DATA
      Assert.assertTrue(childOperator.hasNext());
      partitionRecognizer.addTsBlock(childOperator.next());
      state = partitionRecognizer.nextState();
      Assert.assertEquals(PartitionState.StateType.NEW_PARTITION, state.getStateType());
      checkIteratorSimply(
          state.getSlice(),
          Arrays.asList(
              Arrays.asList(1717171200000L, D1_BINARY, 60),
              Arrays.asList(1719763200000L, D1_BINARY, 60)));
      state = partitionRecognizer.nextState();
      Assert.assertEquals(PartitionState.StateType.NEED_MORE_DATA, state.getStateType());
      // 2. add second TsBlock, expected ITERATING, NEW_PARTITION and NEED_MORE_DATA
      Assert.assertTrue(childOperator.hasNext());
      partitionRecognizer.addTsBlock(childOperator.next());
      state = partitionRecognizer.nextState();
      Assert.assertEquals(PartitionState.StateType.ITERATING, state.getStateType());
      checkIteratorSimply(
          state.getSlice(),
          Collections.singletonList(Arrays.asList(1722441600000L, D1_BINARY, 61)));
      state = partitionRecognizer.nextState();
      Assert.assertEquals(PartitionState.StateType.NEW_PARTITION, state.getStateType());
      checkIteratorSimply(
          state.getSlice(),
          Arrays.asList(
              Arrays.asList(1725120000000L, D2_BINARY, 62),
              Arrays.asList(1725130000000L, D2_BINARY, 62),
              Arrays.asList(1725140000000L, D2_BINARY, 63)));
      state = partitionRecognizer.nextState();
      Assert.assertEquals(PartitionState.StateType.NEED_MORE_DATA, state.getStateType());
      // 3. add third and fourth TsBlock, expected NEED_MORE_DATA
      Assert.assertTrue(childOperator.hasNext());
      partitionRecognizer.addTsBlock(childOperator.next());
      state = partitionRecognizer.nextState();
      Assert.assertEquals(PartitionState.StateType.NEED_MORE_DATA, state.getStateType());
      Assert.assertTrue(childOperator.hasNext());
      partitionRecognizer.addTsBlock(childOperator.next());
      state = partitionRecognizer.nextState();
      Assert.assertEquals(PartitionState.StateType.NEED_MORE_DATA, state.getStateType());
      // 4. add fifth TsBlock, expected NEW_PARTITION, NEW_PARTITION and NEED_MORE_DATA
      Assert.assertTrue(childOperator.hasNext());
      partitionRecognizer.addTsBlock(childOperator.next());
      state = partitionRecognizer.nextState();
      Assert.assertEquals(PartitionState.StateType.NEW_PARTITION, state.getStateType());
      checkIteratorSimply(
          state.getSlice(),
          Arrays.asList(
              Arrays.asList(1722441600000L, D3_BINARY, 63),
              Arrays.asList(1722441800000L, D3_BINARY, 64)));
      state = partitionRecognizer.nextState();
      Assert.assertEquals(PartitionState.StateType.NEW_PARTITION, state.getStateType());
      checkIteratorSimply(
          state.getSlice(),
          Collections.singletonList(Arrays.asList(1722551600000L, D5_BINARY, 64)));
      state = partitionRecognizer.nextState();
      Assert.assertEquals(PartitionState.StateType.NEED_MORE_DATA, state.getStateType());
      // 5. add sixth TsBlock, expected ITERATING and  NEED_MORE_DATA
      Assert.assertTrue(childOperator.hasNext());
      partitionRecognizer.addTsBlock(childOperator.next());
      state = partitionRecognizer.nextState();
      Assert.assertEquals(PartitionState.StateType.ITERATING, state.getStateType());
      checkIteratorSimply(
          state.getSlice(),
          Arrays.asList(
              Arrays.asList(1722552800000L, D5_BINARY, 65),
              Arrays.asList(1722553000000L, D5_BINARY, 65)));
      state = partitionRecognizer.nextState();
      Assert.assertEquals(PartitionState.StateType.NEED_MORE_DATA, state.getStateType());
      // no more data, expected FINISHED
      Assert.assertFalse(childOperator.hasNext());
      partitionRecognizer.noMoreData();
      state = partitionRecognizer.nextState();
      Assert.assertEquals(PartitionState.StateType.FINISHED, state.getStateType());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void checkIteratorSimply(Slice slice, List<List<Object>> expected) {
    Iterator<Record> recordIterable = slice.getRequiredRecordIterator();
    int i = 0;
    while (recordIterable.hasNext()) {
      Record record = recordIterable.next();
      List<Object> recordList = expected.get(i);
      Assert.assertEquals(recordList.size(), record.size());
      for (int j = 0; j < recordList.size(); j++) {
        assertEquals(recordList.get(j), record.getObject(j));
      }
      i++;
    }
    Assert.assertEquals(expected.size(), i);
  }
}
