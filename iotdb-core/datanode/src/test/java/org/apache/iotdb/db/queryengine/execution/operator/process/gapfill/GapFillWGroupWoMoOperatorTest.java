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

package org.apache.iotdb.db.queryengine.execution.operator.process.gapfill;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.MergeSortOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.MergeSortComparator.getComparatorForTable;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class GapFillWGroupWoMoOperatorTest {

  private static final ExecutorService instanceNotificationExecutor =
      IoTDBThreadPoolFactory.newFixedThreadPool(
          1, "GapFillWGroupWoMoOperator-test-instance-notification");

  @AfterClass
  public static void tearDown() {
    instanceNotificationExecutor.shutdown();
  }

  @Test
  public void testGapFillWGroupWoMoOperator() {

    final long[] timeArray =
        new long[] {
          1728849600000L, 1728853200000L, 1728856800000L, 1728860400000L, 1728864000000L,
              1728867600000L, 1728871200000L, 1728874800000L,
          1728849600000L, 1728853200000L, 1728856800000L, 1728860400000L, 1728864000000L,
              1728867600000L, 1728871200000L, 1728874800000L,
          1728849600000L, 1728853200000L, 1728856800000L, 1728860400000L, 1728864000000L,
              1728867600000L, 1728871200000L, 1728874800000L,
          1728849600000L, 1728853200000L, 1728856800000L, 1728860400000L, 1728864000000L,
              1728867600000L, 1728871200000L, 1728874800000L,
        };

    final String[] column1Array =
        new String[] {
          "yangzhou",
          "yangzhou",
          "yangzhou",
          "yangzhou",
          "yangzhou",
          "yangzhou",
          "yangzhou",
          "yangzhou",
          "beijing",
          "beijing",
          "beijing",
          "beijing",
          "beijing",
          "beijing",
          "beijing",
          "beijing",
          "shanghai",
          "shanghai",
          "shanghai",
          "shanghai",
          "shanghai",
          "shanghai",
          "shanghai",
          "shanghai",
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null
        };
    final boolean[] column1IsNull =
        new boolean[] {
          false, false, false, false, false, false, false, false,
          false, false, false, false, false, false, false, false,
          false, false, false, false, false, false, false, false,
          true, true, true, true, true, true, true, true
        };

    final String[] column2Array =
        new String[] {
          "d1", "d1", "d1", "d1", "d1", "d1", "d1", "d1",
          "d2", "d2", "d2", "d2", "d2", "d2", "d2", "d2",
          "d3", "d3", "d3", "d3", "d3", "d3", "d3", "d3",
          "d4", "d4", "d4", "d4", "d4", "d4", "d4", "d4",
        };
    final boolean[] column2IsNull =
        new boolean[] {
          false, false, false, false, false, false, false, false,
          false, false, false, false, false, false, false, false,
          false, false, false, false, false, false, false, false,
          false, false, false, false, false, false, false, false
        };

    final double[] column3Array =
        new double[] {
          0.0, 0.0, 0.0, 27.2, 27.3, 0.0, 0.0, 29.3,
          25.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 28.2,
          0.0, 0.0, 0.0, 0.0, 25.8, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 24.8, 0.0, 0.0, 0.0
        };
    final boolean[] column3IsNull =
        new boolean[] {
          true, true, true, false, false, true, true, false,
          false, true, true, true, true, true, true, false,
          true, true, true, true, false, true, true, true,
          true, true, true, true, false, true, true, true
        };

    try (GapFillWGroupWoMoOperator gapFillOperator = genGapFillWGroupWoMoOperator()) {
      int count = 0;
      ListenableFuture<?> listenableFuture = gapFillOperator.isBlocked();
      listenableFuture.get();
      while (!gapFillOperator.isFinished() && gapFillOperator.hasNext()) {
        TsBlock tsBlock = gapFillOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++, count++) {
            assertFalse(tsBlock.getColumn(0).isNull(i));
            assertEquals(timeArray[count], tsBlock.getColumn(0).getLong(i));
            assertEquals(column1IsNull[count], tsBlock.getColumn(1).isNull(i));
            if (!column1IsNull[count]) {
              assertEquals(
                  column1Array[count],
                  tsBlock.getColumn(1).getBinary(i).getStringValue(TSFileConfig.STRING_CHARSET));
            }
            assertEquals(column2IsNull[count], tsBlock.getColumn(2).isNull(i));
            if (!column2IsNull[count]) {
              assertEquals(
                  column2Array[count],
                  tsBlock.getColumn(2).getBinary(i).getStringValue(TSFileConfig.STRING_CHARSET));
            }
            assertEquals(column3IsNull[count], tsBlock.getColumn(3).isNull(i));
            if (!column3IsNull[count]) {
              assertEquals(column3Array[count], tsBlock.getColumn(3).getDouble(i), 0.00001);
            }
          }
        }
        listenableFuture = gapFillOperator.isBlocked();
        listenableFuture.get();
      }
      assertEquals(timeArray.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private GapFillWGroupWoMoOperator genGapFillWGroupWoMoOperator() {
    // child output
    // Time,             city,       deviceId,   avg_temp
    // 1728856800000     yangzhou       d1       null
    // 1728860400000     yangzhou       d1       27.2
    // 1728864000000     yangzhou       d1       27.3
    // 1728874800000     yangzhou       d1       29.3
    // ------------------------------------------------ TsBlock-1
    // 1728849600000     beijing        d2       25.1
    // ------------------------------------------------ TsBlock-2
    // 1728874800000     beijing        d2       28.2
    // ------------------------------------------------ TsBlock-3
    // 1728864000000     shanghai       d3       25.8
    // 1728864000000     null           d4       24.8
    // ------------------------------------------------ TsBlock-4

    // Construct operator tree
    QueryId queryId = new QueryId("stub_query");

    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNodeId1 = new PlanNodeId("1");
    driverContext.addOperatorContext(1, planNodeId1, MergeSortOperator.class.getSimpleName());
    PlanNodeId planNodeId2 = new PlanNodeId("2");
    driverContext.addOperatorContext(
        2, planNodeId2, GapFillWoGroupWoMoOperator.class.getSimpleName());
    Operator childOperator =
        new Operator() {

          private final long[][] timeArray =
              new long[][] {
                {1728856800000L, 1728860400000L, 1728864000000L, 1728874800000L},
                {1728849600000L},
                {1728874800000L},
                {1728864000000L, 1728864000000L}
              };

          private final String[][] cityArray =
              new String[][] {
                {"yangzhou", "yangzhou", "yangzhou", "yangzhou"},
                {"beijing"},
                {"beijing"},
                {"shanghai", null},
              };

          private final String[][] deviceIdArray =
              new String[][] {
                {"d1", "d1", "d1", "d1"},
                {"d2"},
                {"d2"},
                {"d3", "d4"},
              };

          private final double[][] valueArray =
              new double[][] {
                {0.0, 27.2, 27.3, 29.3},
                {25.1},
                {28.2},
                {25.8, 24.8},
              };

          private final boolean[][] valueIsNull =
              new boolean[][] {
                {true, false, false, false},
                {false},
                {false},
                {false, false},
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
                        TSDataType.TIMESTAMP,
                        TSDataType.STRING,
                        TSDataType.STRING,
                        TSDataType.DOUBLE));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getColumnBuilder(0).writeLong(timeArray[index][i]);
              if (cityArray[index][i] == null) {
                builder.getColumnBuilder(1).appendNull();
              } else {
                builder
                    .getColumnBuilder(1)
                    .writeBinary(new Binary(cityArray[index][i], TSFileConfig.STRING_CHARSET));
              }
              if (deviceIdArray[index][i] == null) {
                builder.getColumnBuilder(2).appendNull();
              } else {
                builder
                    .getColumnBuilder(2)
                    .writeBinary(new Binary(deviceIdArray[index][i], TSFileConfig.STRING_CHARSET));
              }
              if (valueIsNull[index][i]) {
                builder.getColumnBuilder(3).appendNull();
              } else {
                builder.getColumnBuilder(3).writeDouble(valueArray[index][i]);
              }
            }
            builder.declarePositions(timeArray[index].length);
            index++;
            return builder.build(
                new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, builder.getPositionCount()));
          }

          @Override
          public boolean hasNext() throws Exception {
            return index < valueIsNull.length;
          }

          @Override
          public void close() throws Exception {}

          @Override
          public boolean isFinished() throws Exception {
            return index >= valueIsNull.length;
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

    OperatorContext operatorContext = driverContext.getOperatorContexts().get(1);

    List<SortOrder> sortOrderList =
        Arrays.asList(SortOrder.ASC_NULLS_LAST, SortOrder.ASC_NULLS_LAST);
    List<Integer> sortItemIndexList = Arrays.asList(1, 2);
    List<TSDataType> sortItemDataTypeList = Arrays.asList(TSDataType.STRING, TSDataType.STRING);

    Comparator<SortKey> groupKeyComparator =
        getComparatorForTable(sortOrderList, sortItemIndexList, sortItemDataTypeList);

    Set<Integer> groupKeyIndexSet = new HashSet<>();
    groupKeyIndexSet.add(1);
    groupKeyIndexSet.add(2);

    return new GapFillWGroupWoMoOperator(
        operatorContext,
        childOperator,
        0,
        1728849600000L,
        1728874800000L,
        groupKeyComparator,
        Arrays.asList(
            TSDataType.TIMESTAMP, TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE),
        groupKeyIndexSet,
        3600000L);
  }
}
