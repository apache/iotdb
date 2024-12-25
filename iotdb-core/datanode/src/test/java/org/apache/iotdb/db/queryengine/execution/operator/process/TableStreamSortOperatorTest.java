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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.MergeSortComparator.getComparatorForTable;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.apache.iotdb.db.utils.EnvironmentUtils.cleanDir;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class TableStreamSortOperatorTest {

  private static final String sortTmpPrefixPath =
      "target" + File.separator + "sort" + File.separator + "tmp";

  private static final ExecutorService instanceNotificationExecutor =
      IoTDBThreadPoolFactory.newFixedThreadPool(1, "sortOperator-test-instance-notification");

  private final long[] timeArray =
      new long[] {
        3L, 4L, 2L, 1L, 3L, 1L, 2L, 4L, 5L, 5L, 2L, 3L, 1L, 4L, 2L, 3L, 1L, 4L, 5L, 1L, 2L, 3L, 4L,
        5L, 4L, 1L, 2L, 5L, 3L, 5L, 4L, 3L, 2L, 1L
      };
  private final String[] column1Array =
      new String[] {
        null,
        null,
        null,
        null,
        "beijing",
        "beijing",
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
        "shanghai",
        "shanghai",
        "yangzhou",
        "yangzhou",
        "yangzhou",
        "yangzhou",
        "yangzhou",
        "yangzhou",
        "yangzhou",
        "yangzhou",
        "yangzhou",
        "yangzhou"
      };
  private final boolean[] column1IsNull =
      new boolean[] {
        true, true, true, true, false, false, false, false, false, false, false, false, false,
        false, false, false, false, false, false, false, false, false, false, false, false, false,
        false, false, false, false, false, false, false, false
      };
  private final String[] column2Array =
      new String[] {
        "d1", "d1", "d1", "d1", "d1", "d1", "d1", "d1", "d1", "d2", "d2", "d2", "d2", "d2", "d1",
        "d1", "d1", "d1", "d1", "d2", "d2", "d2", "d2", "d2", "d1", "d1", "d1", "d1", "d1", "d2",
        "d2", "d2", "d2", "d2"
      };
  private final boolean[] column2IsNull =
      new boolean[] {
        false, false, false, false, false, false, false, false, false, false, false, false, false,
        false, false, false, false, false, false, false, false, false, false, false, false, false,
        false, false, false, false, false, false, false, false
      };
  private final int[] column3Array =
      new int[] {
        6, 7, 8, 9, 0, 111, 112, 114, 115, 0, 121, 122, 123, 124, 0, 11, 12, 14, 15, 21, 22, 23, 24,
        25, 0, 11, 12, 13, 15, 21, 22, 23, 24, 25
      };
  private final boolean[] column3IsNull =
      new boolean[] {
        false, false, false, false, true, false, false, false, false, true, false, false, false,
        false, true, false, false, false, false, false, false, false, false, false, true, false,
        false, false, false, false, false, false, false, false
      };

  @After
  public void cleanUp() throws IOException {
    cleanDir(sortTmpPrefixPath);
  }

  @AfterClass
  public static void tearDown() {
    instanceNotificationExecutor.shutdown();
  }

  @Test
  public void allInMemoryTest() {

    try (TableStreamSortOperator streamSortOperator = genStreamSortOperator(1000)) {
      int count = 0;
      ListenableFuture<?> listenableFuture = streamSortOperator.isBlocked();
      listenableFuture.get();
      while (!streamSortOperator.isFinished() && streamSortOperator.hasNext()) {
        TsBlock tsBlock = streamSortOperator.next();
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
              assertEquals(column3Array[count], tsBlock.getColumn(3).getInt(i));
            }
          }
        }
        listenableFuture = streamSortOperator.isBlocked();
        listenableFuture.get();
      }
      assertEquals(timeArray.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void allInMemoryTes2() {
    int maxTsBlockLineNumber = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockLineNumber(2);
    try (TableStreamSortOperator tableStreamSortOperator = genStreamSortOperator(2)) {
      int count = 0;
      ListenableFuture<?> listenableFuture = tableStreamSortOperator.isBlocked();
      listenableFuture.get();
      while (!tableStreamSortOperator.isFinished() && tableStreamSortOperator.hasNext()) {
        TsBlock tsBlock = tableStreamSortOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          assertEquals(2, tsBlock.getPositionCount());
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
              assertEquals(column3Array[count], tsBlock.getColumn(3).getInt(i));
            }
          }
        }
        listenableFuture = tableStreamSortOperator.isBlocked();
        listenableFuture.get();
      }
      assertEquals(timeArray.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      TSFileDescriptor.getInstance().getConfig().setMaxTsBlockLineNumber(maxTsBlockLineNumber);
    }
  }

  @Test
  public void someInDiskTest() {

    long sortBufferSize = IoTDBDescriptor.getInstance().getConfig().getSortBufferSize();
    int maxTsBlockSizeInBytes =
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    IoTDBDescriptor.getInstance().getConfig().setSortBufferSize(510);
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(50);
    try (TableStreamSortOperator tableStreamSortOperator = genStreamSortOperator(1000)) {
      int count = 0;
      ListenableFuture<?> listenableFuture = tableStreamSortOperator.isBlocked();
      listenableFuture.get();
      while (!tableStreamSortOperator.isFinished() && tableStreamSortOperator.hasNext()) {
        TsBlock tsBlock = tableStreamSortOperator.next();
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
              assertEquals(column3Array[count], tsBlock.getColumn(3).getInt(i));
            }
          }
        }
        listenableFuture = tableStreamSortOperator.isBlocked();
        listenableFuture.get();
      }
      assertEquals(timeArray.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSortBufferSize(sortBufferSize);
      TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(maxTsBlockSizeInBytes);
    }
  }

  @Test
  public void someInDiskTest2() {
    long sortBufferSize = IoTDBDescriptor.getInstance().getConfig().getSortBufferSize();
    int maxTsBlockSizeInBytes =
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    int maxTsBlockLineNumber = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();
    IoTDBDescriptor.getInstance().getConfig().setSortBufferSize(500);
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(50);
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockLineNumber(2);
    try (TableStreamSortOperator tableStreamSortOperator = genStreamSortOperator(2)) {

      int count = 0;
      ListenableFuture<?> listenableFuture = tableStreamSortOperator.isBlocked();
      listenableFuture.get();
      while (!tableStreamSortOperator.isFinished() && tableStreamSortOperator.hasNext()) {
        TsBlock tsBlock = tableStreamSortOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          assertEquals(2, tsBlock.getPositionCount());
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
              assertEquals(column3Array[count], tsBlock.getColumn(3).getInt(i));
            }
          }
        }
        listenableFuture = tableStreamSortOperator.isBlocked();
        listenableFuture.get();
      }
      assertEquals(timeArray.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSortBufferSize(sortBufferSize);
      TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(maxTsBlockSizeInBytes);
      TSFileDescriptor.getInstance().getConfig().setMaxTsBlockLineNumber(maxTsBlockLineNumber);
    }
  }

  private TableStreamSortOperator genStreamSortOperator(int maxLinesToOutput) {
    // child output
    // Time, city,       deviceId,   s1
    // 1     null           d1       9
    // 2     null           d1       8
    // ---------------------- TsBlock-1
    // 3     null           d1       6
    // 4     null           d1       7
    // 1     beijing        d1       111
    // 2     beijing        d1       112
    // 3     beijing        d1       null
    // 4     beijing        d1       114
    // 5     beijing        d1       115
    // ---------------------- TsBlock-2
    // 1     beijing        d2       123
    // 2     beijing        d2       121
    // 3     beijing        d2       122
    // 4     beijing        d2       124
    // 5     beijing        d2       null
    // ---------------------- TsBlock-3
    // null
    // ---------------------- TsBlock-4
    // 1     shanghai       d1       12
    // 2     shanghai       d1       null
    // 3     shanghai       d1       11
    // 4     shanghai       d1       14
    // 5     shanghai       d1       15
    // ---------------------- TsBlock-5
    // 1     shanghai       d2       21
    // 2     shanghai       d2       22
    // 3     shanghai       d2       23
    // 4     shanghai       d2       24
    // 5     shanghai       d2       25
    // 1     yangzhou       d1       11
    // 2     yangzhou       d1       12
    // ---------------------- TsBlock-6
    // 3     yangzhou       d1       15
    // 4     yangzhou       d1       null
    // 5     yangzhou       d1       13
    // ---------------------- TsBlock-7
    // empty
    // ---------------------- TsBlock-8
    // 1     yangzhou       d2       25
    // 2     yangzhou       d2       24
    // 3     yangzhou       d2       23
    // 4     yangzhou       d2       22
    // ---------------------- TsBlock-9
    // 5     yangzhou       d2       21
    // ---------------------- TsBlock-10

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
    driverContext.addOperatorContext(1, planNodeId1, TableScanOperator.class.getSimpleName());
    PlanNodeId planNodeId2 = new PlanNodeId("2");
    driverContext.addOperatorContext(2, planNodeId2, TableStreamSortOperator.class.getSimpleName());
    Operator childOperator =
        new Operator() {

          private final long[][] timeArray =
              new long[][] {
                {1L, 2L},
                {3L, 4L, 1L, 2L, 3L, 4L, 5L},
                {1L, 2L, 3L, 4L, 5L},
                null,
                {1L, 2L, 3L, 4L, 5L},
                {1L, 2L, 3L, 4L, 5L, 1L, 2L},
                {3L, 4L, 5L},
                {},
                {1L, 2L, 3L, 4L},
                {5L}
              };

          private final String[][] cityArray =
              new String[][] {
                {null, null},
                {null, null, "beijing", "beijing", "beijing", "beijing", "beijing"},
                {"beijing", "beijing", "beijing", "beijing", "beijing"},
                null,
                {"shanghai", "shanghai", "shanghai", "shanghai", "shanghai"},
                {
                  "shanghai", "shanghai", "shanghai", "shanghai", "shanghai", "yangzhou", "yangzhou"
                },
                {"yangzhou", "yangzhou", "yangzhou"},
                {},
                {"yangzhou", "yangzhou", "yangzhou", "yangzhou"},
                {"yangzhou"}
              };

          private final String[][] deviceIdArray =
              new String[][] {
                {"d1", "d1"},
                {"d1", "d1", "d1", "d1", "d1", "d1", "d1"},
                {"d2", "d2", "d2", "d2", "d2"},
                null,
                {"d1", "d1", "d1", "d1", "d1"},
                {"d2", "d2", "d2", "d2", "d2", "d1", "d1"},
                {"d1", "d1", "d1"},
                {},
                {"d2", "d2", "d2", "d2"},
                {"d2"}
              };

          private final int[][] valueArray =
              new int[][] {
                {9, 8},
                {6, 7, 111, 112, 0, 114, 115},
                {123, 121, 122, 124, 0},
                null,
                {12, 0, 11, 14, 15},
                {21, 22, 23, 24, 25, 11, 12},
                {15, 0, 13},
                {},
                {25, 24, 23, 22},
                {21}
              };

          private final boolean[][] valueIsNull =
              new boolean[][] {
                {false, false},
                {false, false, false, false, true, false, false},
                {false, false, false, false, true},
                null,
                {false, true, false, false, false},
                {false, false, false, false, false, false, false},
                {false, true, false},
                {},
                {false, false, false, false},
                {false}
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
                        TSDataType.TIMESTAMP, TSDataType.TEXT, TSDataType.TEXT, TSDataType.INT32));
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
                builder.getColumnBuilder(3).writeInt(valueArray[index][i]);
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
    String filePrefix =
        sortTmpPrefixPath
            + File.separator
            + operatorContext
                .getDriverContext()
                .getFragmentInstanceContext()
                .getId()
                .getFragmentInstanceId()
            + File.separator
            + operatorContext.getDriverContext().getPipelineId()
            + File.separator;

    List<SortOrder> sortOrderList =
        Arrays.asList(
            SortOrder.ASC_NULLS_FIRST, SortOrder.ASC_NULLS_FIRST, SortOrder.ASC_NULLS_FIRST);
    List<Integer> sortItemIndexList = Arrays.asList(1, 2, 3);
    List<TSDataType> sortItemDataTypeList =
        Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.INT32);

    Comparator<SortKey> comparator =
        getComparatorForTable(sortOrderList, sortItemIndexList, sortItemDataTypeList);

    Comparator<SortKey> streamKeyComparator =
        getComparatorForTable(
            sortOrderList.subList(0, 2),
            sortItemIndexList.subList(0, 2),
            sortItemDataTypeList.subList(0, 2));

    return new TableStreamSortOperator(
        operatorContext,
        childOperator,
        Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TEXT, TSDataType.TEXT, TSDataType.INT32),
        filePrefix,
        comparator,
        streamKeyComparator,
        maxLinesToOutput);
  }
}
