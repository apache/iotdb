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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.comparator.JoinKeyComparatorFactory;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.AsofMergeSortInnerJoinOperator;
import org.apache.iotdb.db.queryengine.plan.planner.memory.ThreadSafeMemoryReservationManager;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.BinaryType;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AsofInnerJoinOperatorTest {
  private final Binary d1 = new Binary("d1".getBytes());
  private final Binary d2 = new Binary("d2".getBytes());

  @Test
  public void testAsofInnerJoin() {
    // left table
    // Time, device, s1
    // 4     d1,     4
    // 6     d1,     6
    // 9     d1,     9
    // ----------- TsBlock-1
    // 13    d1,     13
    // 17    d1,     17
    // ----------- TsBlock-2
    // 22    d2,     22
    // 25    d2,     25
    // ----------- TsBlock-3

    // right table
    // Time, device, s2
    // 1     d1,     10
    // 2     d1,     20
    // 3     d1,     30
    // ----------- TsBlock-1
    // 4     d1,     40
    // 5     d1,     50
    // 10    d1,     100
    // ----------- TsBlock-2
    // 13    d2,     130
    // 16    d2,     160
    // ----------- TsBlock-3
    // 26    d2,     260
    // 27    d2,     270
    // ----------- TsBlock-4

    // join condition1
    // left.time>right.time

    // result table
    // Time1,s1,   Time2,s2
    // 25,   25,   16,   160
    // 22,   22,   16,   160
    // 17,   17,   16,   160
    // 13    13,   10,   100
    // 9,    9,    5,    50
    // 6,    6,    5,    50
    // 4,    4,    3,    30
    testExecute(
        true,
        false,
        false,
        new long[] {25, 22, 17, 13, 9, 6, 4},
        new int[] {25, 22, 17, 13, 9, 6, 4},
        new long[] {16, 16, 16, 10, 5, 5, 3},
        new int[] {160, 160, 160, 100, 50, 50, 30},
        new boolean[][] {
          {false, false, false, false, false, false, false},
          {false, false, false, false, false, false, false}
        });

    // join condition2
    // left.time>=right.time

    // result table
    // Time1,s1,   Time2,s2
    // 25,   25,   16,   160
    // 22,   22,   16,   160
    // 17,   17,   16,   160
    // 13    13,   13,   130
    // 9,    9,    5,    50
    // 6,    6,    5,    50
    // 4,    4,    4,    40
    testExecute(
        true,
        true,
        false,
        new long[] {25, 22, 17, 13, 9, 6, 4},
        new int[] {25, 22, 17, 13, 9, 6, 4},
        new long[] {16, 16, 16, 13, 5, 5, 4},
        new int[] {160, 160, 160, 130, 50, 50, 40},
        new boolean[][] {
          {false, false, false, false, false, false, false},
          {false, false, false, false, false, false, false}
        });

    // join condition3
    // left.time<right.time

    // Time1,s1,   Time2,s2
    // 4,    4,    5,    50
    // 6,    6,    10,   100
    // 9,    9,    10,   100
    // 13    13,   16,   160
    // 17,   17,   26,   260
    // 22,   22,   26,   260
    // 25,   25,   26,   260
    testExecute(
        false,
        false,
        false,
        new long[] {4, 6, 9, 13, 17, 22, 25},
        new int[] {4, 6, 9, 13, 17, 22, 25},
        new long[] {5, 10, 10, 16, 26, 26, 26},
        new int[] {50, 100, 100, 160, 260, 260, 260},
        new boolean[][] {
          {false, false, false, false, false, false, false},
          {false, false, false, false, false, false, false}
        });

    // join condition4
    // left.time<=right.time

    // Time1,s1,   Time2,s2
    // 4,    4,    4,    40
    // 6,    6,    10,   100
    // 9,    9,    10,   100
    // 13    13,   13,   130
    // 17,   17,   26,   260
    // 22,   22,   26,   260
    // 25,   25,   26,   260
    testExecute(
        false,
        true,
        false,
        new long[] {4, 6, 9, 13, 17, 22, 25},
        new int[] {4, 6, 9, 13, 17, 22, 25},
        new long[] {4, 10, 10, 13, 26, 26, 26},
        new int[] {40, 100, 100, 130, 260, 260, 260},
        new boolean[][] {
          {false, false, false, false, false, false, false},
          {false, false, false, false, false, false, false}
        });

    // left table
    // Time, device, s1
    // 4     d1,     4
    // 6     d1,     6
    // 9     d1,     9
    // ----------- TsBlock-1
    // 13    d1,     13
    // 17    d1,     17
    // ----------- TsBlock-2
    // 22    d2,     22
    // 25    d2,     25
    // ----------- TsBlock-3

    // right table
    // Time, device, s2
    // 1     d1,     10
    // 2     d1,     20
    // 3     d1,     30
    // ----------- TsBlock-1
    // 4     d1,     40
    // 5     d1,     50
    // 10    d1,     100
    // ----------- TsBlock-2
    // 13    d2,     130
    // 16    d2,     160
    // ----------- TsBlock-3
    // 26    d2,     260
    // 27    d2,     270
    // ----------- TsBlock-4

    // join condition5
    // left.device=right.device and left.time>right.time

    // result table
    // result table
    // Time1,s1,   Time2,s2
    // 13    13,   10,   100
    // 9,    9,    5,    50
    // 6,    6,    5,    50
    // 4,    4,    3,    30
    // 25,   25,   16,   160
    // 22,   22,   16,   160
    testExecute(
        true,
        false,
        true,
        new long[] {
          17, 13, 9, 6, 4, 25, 22,
        },
        new int[] {
          17, 13, 9, 6, 4, 25, 22,
        },
        new long[] {
          10, 10, 5, 5, 3, 16, 16,
        },
        new int[] {
          100, 100, 50, 50, 30, 160, 160,
        },
        new boolean[][] {
          {false, false, false, false, false, false, false},
          {false, false, false, false, false, false, false}
        });

    // join condition6
    // left.device=right.device and left.time>=right.time

    // result table
    // 17,   17,   10,   100
    // 13    13,   10,   100
    // 9,    9,    5,    50
    // 6,    6,    5,    50
    // 4,    4,    4,    40
    // 25,   25,   16,   160
    // 22,   22,   16,   160
    testExecute(
        true,
        true,
        true,
        new long[] {
          17, 13, 9, 6, 4, 25, 22,
        },
        new int[] {
          17, 13, 9, 6, 4, 25, 22,
        },
        new long[] {
          10, 10, 5, 5, 4, 16, 16,
        },
        new int[] {
          100, 100, 50, 50, 40, 160, 160,
        },
        new boolean[][] {
          {false, false, false, false, false, false, false},
          {false, false, false, false, false, false, false}
        });

    // join condition7
    // left.device=right.device and left.time<right.time

    // Time1,s1,   Time2,s2
    // 4,    4,    5,    50
    // 6,    6,    10,   100
    // 9,    9,    10,   100
    // 22,   22,   26,   260
    // 25,   25,   26,   260
    testExecute(
        false,
        false,
        true,
        new long[] {4, 6, 9, 22, 25},
        new int[] {4, 6, 9, 22, 25},
        new long[] {5, 10, 10, 26, 26},
        new int[] {50, 100, 100, 260, 260},
        new boolean[][] {{false, false, false, false, false}, {false, false, false, false, false}});

    // join condition8
    // left.device=right.device and left.time<=right.time

    // Time1,s1,   Time2,s2
    // 4,    4,    4,    40
    // 6,    6,    10,   100
    // 9,    9,    10,   100
    // 22,   22,   26,   260
    // 25,   25,   26,   260
    testExecute(
        false,
        true,
        true,
        new long[] {4, 6, 9, 22, 25},
        new int[] {4, 6, 9, 22, 25},
        new long[] {4, 10, 10, 26, 26},
        new int[] {40, 100, 100, 260, 260},
        new boolean[][] {{false, false, false, false, false}, {false, false, false, false, false}});
  }

  // left.time>right.time: containsGreaterThan=true, ignoreEqual=false
  // left.time<=right.time: containsGreaterThan=false, ignoreEqual=true
  private void testExecute(
      boolean containsGreaterThan,
      boolean ignoreEqual,
      boolean deviceEqual,
      long[] time1Array,
      int[] column1Array,
      long[] time2Array,
      int[] column2Array,
      boolean[][] columnsIsNull) {
    FragmentInstanceContext fragmentInstanceContext = Mockito.mock(FragmentInstanceContext.class);
    Mockito.when(fragmentInstanceContext.getMemoryReservationContext())
        .thenReturn(new ThreadSafeMemoryReservationManager(new QueryId("1"), "test"));
    DriverContext driverContext = Mockito.mock(DriverContext.class);
    Mockito.when(driverContext.getFragmentInstanceContext()).thenReturn(fragmentInstanceContext);
    OperatorContext operatorContext = Mockito.mock(OperatorContext.class);
    Mockito.when(operatorContext.getMaxRunTime()).thenReturn(new Duration(1, TimeUnit.SECONDS));
    Mockito.when(operatorContext.getDriverContext()).thenReturn(driverContext);

    Operator leftChild =
        new Operator() {
          private final long[][] timeArray =
              containsGreaterThan
                  ? (deviceEqual
                      ? new long[][] {
                        {17L, 13L},
                        {9L, 6L, 4L},
                        {25L, 22L}
                      }
                      : new long[][] {
                        {25L, 22L},
                        {17L, 13L},
                        {9L, 6L, 4L}
                      })
                  : new long[][] {
                    {4L, 6L, 9L},
                    {13L, 17L},
                    {22L, 25L}
                  };

          private final Binary[][] deviceArray =
              containsGreaterThan
                  ? (deviceEqual
                      ? new Binary[][] {
                        new Binary[] {d1, d1},
                        new Binary[] {d1, d1, d1},
                        new Binary[] {d2, d2}
                      }
                      : new Binary[][] {
                        new Binary[] {d2, d2}, new Binary[] {d1, d1}, new Binary[] {d1, d1, d1}
                      })
                  : new Binary[][] {
                    new Binary[] {d1, d1, d1},
                    new Binary[] {d1, d1},
                    new Binary[] {d2, d2},
                  };

          private final int[][] valueArray =
              containsGreaterThan
                  ? (deviceEqual
                      ? new int[][] {
                        {17, 13}, {9, 6, 4}, {25, 22},
                      }
                      : new int[][] {
                        {25, 22}, {17, 13}, {9, 6, 4},
                      })
                  : new int[][] {
                    {4, 6, 9},
                    {13, 17},
                    {22, 25}
                  };

          private final boolean[][][] valueIsNull =
              containsGreaterThan
                  ? (deviceEqual
                      ? new boolean[][][] {
                        {
                          {false, false},
                          {false, false, false},
                          {false, false}
                        }
                      }
                      : new boolean[][][] {
                        {
                          {false, false},
                          {false, false},
                          {false, false, false}
                        }
                      })
                  : new boolean[][][] {
                    {
                      {false, false, false},
                      {false, false},
                      {false, false}
                    }
                  };

          private int index = 0;

          @Override
          public OperatorContext getOperatorContext() {
            return operatorContext;
          }

          @Override
          public TsBlock next() {
            TsBlockBuilder builder =
                new TsBlockBuilder(
                    timeArray[index].length,
                    Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TEXT, TSDataType.INT32));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getColumnBuilder(0).writeLong(timeArray[index][i]);
              builder.getColumnBuilder(1).writeBinary(deviceArray[index][i]);
              if (valueIsNull[0][index][i]) {
                builder.getColumnBuilder(2).appendNull();
              } else {
                builder.getColumnBuilder(2).writeInt(valueArray[index][i]);
              }
            }
            builder.declarePositions(timeArray[index].length);
            return builder.build(
                new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, timeArray[index++].length));
          }

          @Override
          public boolean hasNext() {
            return index < 3;
          }

          @Override
          public void close() {}

          @Override
          public boolean isFinished() {
            return index >= 3;
          }

          @Override
          public long calculateMaxPeekMemory() {
            return 64 * 1024;
          }

          @Override
          public long calculateMaxReturnSize() {
            return 64 * 1024;
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

    Operator rightChild =
        new Operator() {
          private final long[][] timeArray =
              containsGreaterThan
                  ? (deviceEqual
                      ? new long[][] {
                        {10L, 5L, 4L},
                        {3L, 2L, 1L},
                        {27L, 26L},
                        {16L, 13L},
                      }
                      : new long[][] {
                        {27L, 26L},
                        {16L, 13L},
                        {10L, 5L, 4L},
                        {3L, 2L, 1L},
                      })
                  : new long[][] {
                    {1L, 2L, 3L},
                    {4L, 5L, 10L},
                    {13L, 16L},
                    {26L, 27L}
                  };

          private final Binary[][] deviceArray =
              containsGreaterThan && !deviceEqual
                  ? new Binary[][] {
                    new Binary[] {d2, d2},
                    new Binary[] {d2, d2},
                    new Binary[] {d1, d1, d1},
                    new Binary[] {d1, d1, d1}
                  }
                  : new Binary[][] {
                    new Binary[] {d1, d1, d1},
                    new Binary[] {d1, d1, d1},
                    new Binary[] {d2, d2},
                    new Binary[] {d2, d2}
                  };

          private final int[][] valueArray =
              containsGreaterThan
                  ? (deviceEqual
                      ? new int[][] {
                        {100, 50, 40},
                        {30, 20, 10},
                        {270, 260},
                        {160, 130},
                      }
                      : new int[][] {
                        {270, 260},
                        {160, 130},
                        {100, 50, 40},
                        {30, 20, 10},
                      })
                  : new int[][] {
                    {10, 20, 30},
                    {40, 50, 100},
                    {130, 160},
                    {260, 270}
                  };

          private final boolean[][][] valueIsNull =
              containsGreaterThan && !deviceEqual
                  ? new boolean[][][] {
                    {
                      {false, false},
                      {false, false},
                      {false, false, false},
                      {false, false, false}
                    }
                  }
                  : new boolean[][][] {
                    {
                      {false, false, false},
                      {false, false, false},
                      {false, false},
                      {false, false}
                    }
                  };

          private int index = 0;

          @Override
          public OperatorContext getOperatorContext() {
            return operatorContext;
          }

          @Override
          public TsBlock next() {
            TsBlockBuilder builder =
                new TsBlockBuilder(
                    timeArray[index].length,
                    Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TEXT, TSDataType.INT32));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getColumnBuilder(0).writeLong(timeArray[index][i]);
              builder.getColumnBuilder(1).writeBinary(deviceArray[index][i]);
              if (valueIsNull[0][index][i]) {
                builder.getColumnBuilder(2).appendNull();
              } else {
                builder.getColumnBuilder(2).writeInt(valueArray[index][i]);
              }
            }
            builder.declarePositions(timeArray[index].length);
            return builder.build(
                new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, timeArray[index++].length));
          }

          @Override
          public boolean hasNext() {
            return index < 4;
          }

          @Override
          public void close() {}

          @Override
          public boolean isFinished() {
            return index >= 4;
          }

          @Override
          public long calculateMaxPeekMemory() {
            return 64 * 1024;
          }

          @Override
          public long calculateMaxReturnSize() {
            return 64 * 1024;
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

    AsofMergeSortInnerJoinOperator asofInnerJoinOperator =
        new AsofMergeSortInnerJoinOperator(
            operatorContext,
            leftChild,
            deviceEqual ? new int[] {1, 0} : new int[] {0},
            new int[] {0, 2},
            rightChild,
            deviceEqual ? new int[] {1, 0} : new int[] {0},
            new int[] {0, 2},
            JoinKeyComparatorFactory.getAsofComparators(
                deviceEqual ? Collections.singletonList(BinaryType.TEXT) : Collections.emptyList(),
                ignoreEqual,
                !containsGreaterThan),
            Arrays.asList(
                TSDataType.TIMESTAMP, TSDataType.INT32, TSDataType.TIMESTAMP, TSDataType.INT32));

    try {
      int count = 0;
      ListenableFuture<?> listenableFuture = asofInnerJoinOperator.isBlocked();
      listenableFuture.get();
      while (!asofInnerJoinOperator.isFinished() && asofInnerJoinOperator.hasNext()) {
        TsBlock tsBlock = asofInnerJoinOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++, count++) {
            assertEquals(time1Array[count], tsBlock.getColumn(0).getLong(i));
            assertEquals(columnsIsNull[0][count], tsBlock.getColumn(1).isNull(i));
            if (!columnsIsNull[1][count]) {
              assertEquals(column1Array[count], tsBlock.getColumn(1).getInt(i));
            }

            assertEquals(time2Array[count], tsBlock.getColumn(2).getLong(i));
            assertEquals(columnsIsNull[1][count], tsBlock.getColumn(3).isNull(i));
            if (!columnsIsNull[1][count]) {
              assertEquals(column2Array[count], tsBlock.getColumn(3).getLong(i));
            }
          }
        }
        listenableFuture = asofInnerJoinOperator.isBlocked();
        listenableFuture.get();
      }
      assertEquals(time1Array.length, count);
      assertEquals(time2Array.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
