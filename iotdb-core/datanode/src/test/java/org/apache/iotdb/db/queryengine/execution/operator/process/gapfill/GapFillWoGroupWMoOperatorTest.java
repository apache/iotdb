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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.junit.AfterClass;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class GapFillWoGroupWMoOperatorTest {

  private static final ExecutorService instanceNotificationExecutor =
      IoTDBThreadPoolFactory.newFixedThreadPool(
          1, "GapFillWGroupWoMoOperator-test-instance-notification");

  private final long[] timeArray =
      new long[] {
        1711900800000L,
        1714492800000L,
        1717171200000L,
        1719763200000L,
        1722441600000L,
        1725120000000L,
        1727712000000L,
        1730390400000L,
      };

  @AfterClass
  public static void tearDown() {
    instanceNotificationExecutor.shutdown();
  }

  @Test
  public void testGapFillWGroupWoMoOperator1() {
    final double[] valueArray =
        new double[] {
          0.0, 0.0, 0.0, 27.2, 27.3, 29.3, 0.0, 0.0,
        };
    final boolean[] valueIsNull =
        new boolean[] {
          true, true, true, false, false, false, true, true,
        };
    // child output
    // Time,             city,       deviceId,   avg_temp
    // 1717171200000     yangzhou       d1       null
    // 1719763200000     yangzhou       d1       27.2
    // ------------------------------------------------ TsBlock-1
    // 1722441600000     yangzhou       d1       27.3
    // 1725120000000     yangzhou       d1       29.3
    // ------------------------------------------------ TsBlock-2

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
                {1717171200000L, 1719763200000L},
                {1722441600000L, 1725120000000L},
              };

          private final double[][] valueArray =
              new double[][] {
                {0.0, 27.2},
                {27.3, 29.3},
              };

          private final boolean[][] valueIsNull =
              new boolean[][] {
                {
                  true, false,
                },
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
                    Arrays.asList(TSDataType.TIMESTAMP, TSDataType.DOUBLE));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getColumnBuilder(0).writeLong(timeArray[index][i]);
              if (valueIsNull[index][i]) {
                builder.getColumnBuilder(1).appendNull();
              } else {
                builder.getColumnBuilder(1).writeDouble(valueArray[index][i]);
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

    try (GapFillWoGroupWMoOperator gapFillOperator =
        new GapFillWoGroupWMoOperator(
            operatorContext,
            childOperator,
            0,
            1711900800000L,
            1730390400000L,
            Arrays.asList(TSDataType.TIMESTAMP, TSDataType.DOUBLE),
            1,
            ZoneId.of("Asia/Shanghai"))) {
      int count = 0;
      ListenableFuture<?> listenableFuture = gapFillOperator.isBlocked();
      listenableFuture.get();
      while (!gapFillOperator.isFinished() && gapFillOperator.hasNext()) {
        TsBlock tsBlock = gapFillOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++, count++) {
            assertFalse(tsBlock.getColumn(0).isNull(i));
            assertEquals(timeArray[count], tsBlock.getColumn(0).getLong(i));
            assertEquals(valueIsNull[count], tsBlock.getColumn(1).isNull(i));
            if (!valueIsNull[count]) {
              assertEquals(valueArray[count], tsBlock.getColumn(1).getDouble(i), 0.00001);
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

  @Test
  public void testGapFillWGroupWoMoOperator2() {
    final double[] valueArray =
        new double[] {
          0.0, 0.0, 0.0, 27.2, 27.3, 0.0, 0.0, 29.3,
        };
    final boolean[] valueIsNull =
        new boolean[] {
          true, true, true, false, false, true, true, false,
        };

    // child output
    // Time,             city,       deviceId,   avg_temp
    // 1717171200000     yangzhou       d1       null
    // 1719763200000     yangzhou       d1       27.2
    // ------------------------------------------------ TsBlock-1
    // 1722441600000     yangzhou       d1       27.3
    // 1730390400000     yangzhou       d1       29.3
    // ------------------------------------------------ TsBlock-2

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
                {1717171200000L, 1719763200000L},
                {1722441600000L, 1730390400000L},
              };

          private final double[][] valueArray =
              new double[][] {
                {
                  0.0, 27.2,
                },
                {27.3, 29.3},
              };

          private final boolean[][] valueIsNull =
              new boolean[][] {
                {
                  true, false,
                },
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
                    Arrays.asList(TSDataType.TIMESTAMP, TSDataType.DOUBLE));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getColumnBuilder(0).writeLong(timeArray[index][i]);
              if (valueIsNull[index][i]) {
                builder.getColumnBuilder(1).appendNull();
              } else {
                builder.getColumnBuilder(1).writeDouble(valueArray[index][i]);
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

    try (GapFillWoGroupWMoOperator gapFillOperator =
        new GapFillWoGroupWMoOperator(
            operatorContext,
            childOperator,
            0,
            1711900800000L,
            1730390400000L,
            Arrays.asList(TSDataType.TIMESTAMP, TSDataType.DOUBLE),
            1,
            ZoneId.of("Asia/Shanghai"))) {
      int count = 0;
      ListenableFuture<?> listenableFuture = gapFillOperator.isBlocked();
      listenableFuture.get();
      while (!gapFillOperator.isFinished() && gapFillOperator.hasNext()) {
        TsBlock tsBlock = gapFillOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++, count++) {
            assertFalse(tsBlock.getColumn(0).isNull(i));
            assertEquals(timeArray[count], tsBlock.getColumn(0).getLong(i));
            assertEquals(valueIsNull[count], tsBlock.getColumn(1).isNull(i));
            if (!valueIsNull[count]) {
              assertEquals(valueArray[count], tsBlock.getColumn(1).getDouble(i), 0.00001);
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
}
