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
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.rank.RankFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo;
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
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TableWindowOperatorTest {
  private static final ExecutorService instanceNotificationExecutor =
      IoTDBThreadPoolFactory.newFixedThreadPool(1, "windowOperator-test-instance-notification");

  private final long[] column1 = new long[] {1, 2, 3, 4, 5, 6, 7};
  private final String[] column2 = new String[] {"d1", "d1", "d2", "d2", "d2", "d2", "d2"};
  private final int[] column3 = new int[] {1, 2, 3, 4, 5, 6, 7};
  private final long[] column4 = new long[] {1, 2, 1, 2, 3, 4, 5};

  @Test
  public void testOneTsBlockWithMultiPartition() {
    long[][] timeArray =
        new long[][] {
          {1, 2, 3, 4, 5, 6, 7},
        };
    String[][] deviceIdArray =
        new String[][] {
          {"d1", "d1", "d2", "d2", "d2", "d2", "d2"},
        };
    int[][] valueArray =
        new int[][] {
          {1, 2, 3, 4, 5, 6, 7},
        };

    int count = 0;
    try (TableWindowOperator windowOperator =
        genWindowOperator(timeArray, deviceIdArray, valueArray)) {
      ListenableFuture<?> listenableFuture = windowOperator.isBlocked();
      listenableFuture.get();
      while (!windowOperator.isFinished() && windowOperator.hasNext()) {
        TsBlock tsBlock = windowOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++, count++) {
            assertEquals(column1[count], tsBlock.getColumn(0).getLong(i));
            assertEquals(
                column2[count],
                tsBlock.getColumn(1).getBinary(i).getStringValue(TSFileConfig.STRING_CHARSET));
            assertEquals(column3[count], tsBlock.getColumn(2).getInt(i));
            assertEquals(column4[count], tsBlock.getColumn(3).getLong(i));
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testPartitionCrossMultiTsBlock() {
    long[][] timeArray =
        new long[][] {
          {1, 2},
          {3, 4},
          {5},
          {6, 7},
        };
    String[][] deviceIdArray =
        new String[][] {
          {"d1", "d1"},
          {"d2", "d2"},
          {"d2"},
          {"d2", "d2"},
        };
    int[][] valueArray =
        new int[][] {
          {1, 2},
          {3, 4},
          {5},
          {6, 7},
        };

    int count = 0;
    try (TableWindowOperator windowOperator =
        genWindowOperator(timeArray, deviceIdArray, valueArray)) {
      ListenableFuture<?> listenableFuture = windowOperator.isBlocked();
      listenableFuture.get();
      while (!windowOperator.isFinished() && windowOperator.hasNext()) {
        TsBlock tsBlock = windowOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++, count++) {
            assertEquals(column1[count], tsBlock.getColumn(0).getLong(i));
            assertEquals(
                column2[count],
                tsBlock.getColumn(1).getBinary(i).getStringValue(TSFileConfig.STRING_CHARSET));
            assertEquals(column3[count], tsBlock.getColumn(2).getInt(i));
            assertEquals(column4[count], tsBlock.getColumn(3).getLong(i));
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testMixedPartition() {
    long[][] timeArray =
        new long[][] {
          {1, 2, 3, 4},
          {5, 6, 7},
        };
    String[][] deviceIdArray =
        new String[][] {
          {"d1", "d1", "d2", "d2"},
          {"d2", "d2", "d2"},
        };
    int[][] valueArray =
        new int[][] {
          {1, 2, 3, 4},
          {5, 6, 7},
        };

    int count = 0;
    try (TableWindowOperator windowOperator =
        genWindowOperator(timeArray, deviceIdArray, valueArray)) {
      ListenableFuture<?> listenableFuture = windowOperator.isBlocked();
      listenableFuture.get();
      while (!windowOperator.isFinished() && windowOperator.hasNext()) {
        TsBlock tsBlock = windowOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++, count++) {
            assertEquals(column1[count], tsBlock.getColumn(0).getLong(i));
            assertEquals(
                column2[count],
                tsBlock.getColumn(1).getBinary(i).getStringValue(TSFileConfig.STRING_CHARSET));
            assertEquals(column3[count], tsBlock.getColumn(2).getInt(i));
            assertEquals(column4[count], tsBlock.getColumn(3).getLong(i));
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testMixedPartition2() {
    long[][] timeArray =
        new long[][] {
          {1, 2, 3},
          {4, 5},
          {6},
        };
    String[][] deviceIdArray =
        new String[][] {
          {"d1", "d1", "d2"},
          {"d2", "d3"},
          {"d3"},
        };
    int[][] valueArray =
        new int[][] {
          {1, 2, 3},
          {4, 5},
          {6},
        };

    long[] expectColumn1 = new long[] {1, 2, 3, 4, 5, 6};
    String[] expectColumn2 = new String[] {"d1", "d1", "d2", "d2", "d3", "d3"};
    int[] expectColumn4 = new int[] {1, 2, 3, 4, 5, 6};
    long[] expectColumn5 = new long[] {1, 2, 1, 2, 1, 2};

    int count = 0;
    try (TableWindowOperator windowOperator =
        genWindowOperator(timeArray, deviceIdArray, valueArray)) {
      ListenableFuture<?> listenableFuture = windowOperator.isBlocked();
      listenableFuture.get();
      while (!windowOperator.isFinished() && windowOperator.hasNext()) {
        TsBlock tsBlock = windowOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++, count++) {
            assertEquals(expectColumn1[count], tsBlock.getColumn(0).getLong(i));
            assertEquals(
                expectColumn2[count],
                tsBlock.getColumn(1).getBinary(i).getStringValue(TSFileConfig.STRING_CHARSET));
            assertEquals(expectColumn4[count], tsBlock.getColumn(2).getInt(i));
            assertEquals(expectColumn5[count], tsBlock.getColumn(3).getLong(i));
          }
        }
      }
      assertEquals(6, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  static class ChildOperator implements Operator {
    private int index;

    private final long[][] timeArray;
    private final String[][] deviceIdArray;
    private final int[][] valueArray;
    private final DriverContext driverContext;

    ChildOperator(
        long[][] timeArray,
        String[][] deviceIdArray,
        int[][] valueArray,
        DriverContext driverContext) {
      this.timeArray = timeArray;
      this.deviceIdArray = deviceIdArray;
      this.valueArray = valueArray;
      this.driverContext = driverContext;

      this.index = 0;
    }

    @Override
    public OperatorContext getOperatorContext() {
      return driverContext.getOperatorContexts().get(0);
    }

    @Override
    public TsBlock next() throws Exception {
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
            .writeBinary(new Binary(deviceIdArray[index][i], TSFileConfig.STRING_CHARSET));
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
    public void close() {
      // do nothing
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
  }

  private TableWindowOperator genWindowOperator(
      long[][] timeArray, String[][] deviceIdArray, int[][] valueArray) {
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

    List<TSDataType> inputDataTypes =
        Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TEXT, TSDataType.INT32);
    List<TSDataType> outputDataTypes =
        Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TEXT, TSDataType.INT32, TSDataType.INT64);
    ArrayList<Integer> outputChannels = new ArrayList<>();
    for (int i = 0; i < inputDataTypes.size(); i++) {
      outputChannels.add(i);
    }
    WindowFunction windowFunction = new RankFunction();
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.ROWS,
            FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING,
            FrameInfo.FrameBoundType.CURRENT_ROW);

    Operator childOperator = new ChildOperator(timeArray, deviceIdArray, valueArray, driverContext);
    return new TableWindowOperator(
        driverContext.getOperatorContexts().get(0),
        childOperator,
        inputDataTypes,
        outputDataTypes,
        outputChannels,
        Collections.singletonList(windowFunction),
        Collections.singletonList(frameInfo),
        Collections.singletonList(1),
        Collections.singletonList(2));
  }
}
