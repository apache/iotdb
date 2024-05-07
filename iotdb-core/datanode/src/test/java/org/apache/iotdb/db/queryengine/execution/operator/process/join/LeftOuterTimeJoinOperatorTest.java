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

package org.apache.iotdb.db.queryengine.execution.operator.process.join;

import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.DescTimeComparator;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LeftOuterTimeJoinOperatorTest {

  @Test
  public void testLeftOuterJoin1() {
    // left table
    // Time, s1
    // 4     4
    // 6     6
    // 9     9
    // ----------- TsBlock-1
    // 13    13
    // 17    17
    // ----------- TsBlock-2
    // 22    22
    // 25    25
    // ----------- TsBlock-3

    // right table
    // Time, s2
    // 1     10
    // 2     20
    // 3     30
    // ----------- TsBlock-1
    // 4     40
    // 5     50
    // 10    100
    // ----------- TsBlock-2
    // 13    130
    // 16    160
    // ----------- TsBlock-3
    // 26    260
    // 27    270
    // ----------- TsBlock-4

    // result table
    // Time, s1,    s2
    // 4      4     40
    // 6      6     null
    // 9      9     null
    // 13     13    130
    // 17     17    null
    // 22     22    null
    // 25     25    null

    OperatorContext operatorContext = Mockito.mock(OperatorContext.class);
    Mockito.when(operatorContext.getMaxRunTime()).thenReturn(new Duration(1, TimeUnit.SECONDS));

    Operator leftChild =
        new Operator() {
          private final long[][] timeArray =
              new long[][] {
                {4L, 6L, 9L},
                {13L, 17L},
                {22L, 25L}
              };

          private final int[][] valueArray =
              new int[][] {
                {4, 6, 9},
                {13, 17},
                {22, 25}
              };

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
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
                    timeArray[index].length, Collections.singletonList(TSDataType.INT32));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
              if (valueIsNull[0][index][i]) {
                builder.getColumnBuilder(0).appendNull();
              } else {
                builder.getColumnBuilder(0).writeInt(valueArray[index][i]);
              }
            }
            builder.declarePositions(timeArray[index].length);
            index++;
            return builder.build();
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
              new long[][] {
                {1L, 2L, 3L},
                {4L, 5L, 10L},
                {13L, 16L},
                {26L, 27L}
              };

          private final long[][] valueArray =
              new long[][] {
                {10L, 20L, 30L},
                {40L, 50L, 100L},
                {130L, 160L},
                {260L, 270L}
              };

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
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
                    timeArray[index].length, Collections.singletonList(TSDataType.INT64));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
              if (valueIsNull[0][index][i]) {
                builder.getColumnBuilder(0).appendNull();
              } else {
                builder.getColumnBuilder(0).writeLong(valueArray[index][i]);
              }
            }
            builder.declarePositions(timeArray[index].length);
            index++;
            return builder.build();
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

    LeftOuterTimeJoinOperator leftOuterTimeJoinOperator =
        new LeftOuterTimeJoinOperator(
            operatorContext,
            leftChild,
            1,
            rightChild,
            Arrays.asList(TSDataType.INT32, TSDataType.INT64),
            new AscTimeComparator());

    assertEquals(
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes() + 64 * 1024 * 2,
        leftOuterTimeJoinOperator.calculateMaxPeekMemory());
    assertEquals(
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes(),
        leftOuterTimeJoinOperator.calculateMaxReturnSize());
    assertEquals(64 * 1024 * 2, leftOuterTimeJoinOperator.calculateRetainedSizeAfterCallingNext());

    long[] timeArray = new long[] {4L, 6L, 9L, 13L, 17L, 22L, 25L};
    int[] column1Array = new int[] {4, 6, 9, 13, 17, 22, 25};
    boolean[] column1IsNull = new boolean[] {false, false, false, false, false, false, false};
    long[] column2Array = new long[] {40L, 0L, 0L, 130L, 0L, 0L, 0L};
    boolean[] column2IsNull = new boolean[] {false, true, true, false, true, true, true};

    try {
      int count = 0;
      ListenableFuture<?> listenableFuture = leftOuterTimeJoinOperator.isBlocked();
      listenableFuture.get();
      while (!leftOuterTimeJoinOperator.isFinished() && leftOuterTimeJoinOperator.hasNext()) {
        TsBlock tsBlock = leftOuterTimeJoinOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++, count++) {
            assertEquals(timeArray[count], tsBlock.getTimeByIndex(i));
            assertEquals(column1IsNull[count], tsBlock.getColumn(0).isNull(i));
            if (!column1IsNull[count]) {
              assertEquals(column1Array[count], tsBlock.getColumn(0).getInt(i));
            }
            assertEquals(column2IsNull[count], tsBlock.getColumn(1).isNull(i));
            if (!column2IsNull[count]) {
              assertEquals(column2Array[count], tsBlock.getColumn(1).getLong(i));
            }
          }
        }
        listenableFuture = leftOuterTimeJoinOperator.isBlocked();
        listenableFuture.get();
      }
      assertEquals(timeArray.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testLeftOuterJoin2() {
    // left table
    // Time, s1,     s2
    // 25    null    26
    // 22    22      null
    // ---------------------- TsBlock-1
    // null
    // ---------------------- TsBlock-2
    // 19    19      20
    // 18    18      null
    // 15    null    16
    // ---------------------- TsBlock-3
    // empty
    // ---------------------- TsBlock-4
    // 9     null    null
    // 7     7       null
    // 6     null    7
    // 3     3       4
    // ---------------------- TsBlock-5
    // empty
    // ---------------------- TsBlock-6

    // right table
    // Time, s3,      s4
    // 21    210.0    false
    // 20    200.0    null
    // ---------------------- TsBlock-1
    // empty
    // ---------------------- TsBlock-2
    // 19    190.0    true
    // 18    180.0    null
    // 15    null     false
    // 14    null     null
    // 8     80.0     true
    // 7     null     false
    // ---------------------- TsBlock-3
    // null
    // ---------------------- TsBlock-4
    // 5     50.0     true
    // ---------------------- TsBlock-5
    // 4     40.0     null
    // ---------------------- TsBlock-6
    // 3     30.0     false
    // ---------------------- TsBlock-7
    // 2     20.0     true
    // 1     10.0     false
    // ---------------------- TsBlock-8
    // empty
    // ---------------------- TsBlock-9

    // result table
    // Time, s1,     s2,     s3,     s4
    // 25    null    26      null    null
    // 22    22      null    null    null
    // 19    19      20      190.0   true
    // 18    18      null    180.0   null
    // 15    null    16      null    false
    // 9     null    null    null    null
    // 7     7       null    null    false
    // 6     null    7       null    null
    // 3     3       4       30.0    false

    OperatorContext operatorContext = Mockito.mock(OperatorContext.class);
    Mockito.when(operatorContext.getMaxRunTime()).thenReturn(new Duration(1, TimeUnit.SECONDS));

    Operator leftChild =
        new Operator() {
          private final long[][] timeArray =
              new long[][] {{25L, 22L}, null, {19L, 18L, 15L}, {}, {9L, 7L, 6L, 3L}, {}};

          private final int[][] value1Array =
              new int[][] {{0, 22}, null, {19, 18, 0}, {}, {0, 7, 0, 3}, {}};

          private final long[][] value2Array =
              new long[][] {{26L, 0L}, null, {20L, 0L, 16L}, {}, {0L, 0L, 7L, 4L}, {}};

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
                {{true, false}, null, {false, false, true}, {}, {true, false, true, false}, {}},
                {{false, true}, null, {false, true, false}, {}, {true, true, false, false}, {}}
              };

          private int index = 0;

          @Override
          public OperatorContext getOperatorContext() {
            return operatorContext;
          }

          @Override
          public TsBlock next() {
            if (timeArray[index] == null) {
              index++;
              return null;
            }
            TsBlockBuilder builder =
                new TsBlockBuilder(
                    timeArray[index].length, Arrays.asList(TSDataType.INT32, TSDataType.INT64));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
              if (valueIsNull[0][index][i]) {
                builder.getColumnBuilder(0).appendNull();
              } else {
                builder.getColumnBuilder(0).writeInt(value1Array[index][i]);
              }
              if (valueIsNull[1][index][i]) {
                builder.getColumnBuilder(1).appendNull();
              } else {
                builder.getColumnBuilder(1).writeLong(value2Array[index][i]);
              }
            }
            builder.declarePositions(timeArray[index].length);
            index++;
            return builder.build();
          }

          @Override
          public boolean hasNext() {
            return index < 6;
          }

          @Override
          public void close() {}

          @Override
          public boolean isFinished() {
            return index >= 6;
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
              new long[][] {
                {21L, 20L}, {}, {19L, 18L, 15L, 14L, 8L, 7L}, null, {5L}, {4L}, {3L}, {2L, 1L}, {}
              };

          private final float[][] value1Array =
              new float[][] {
                {210.0f, 200.0f},
                {},
                {190.0f, 180.0f, 0.0f, 0.0f, 80.0f, 0.0f},
                null,
                {50.0f},
                {40.0f},
                {30.0f},
                {20.0f, 10.0f},
                {}
              };

          private final boolean[][] value2Array =
              new boolean[][] {
                {false, false},
                {},
                {true, false, false, false, true, false},
                null,
                {true},
                {false},
                {false},
                {true, false},
                {}
              };

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
                {
                  {false, false},
                  {},
                  {false, false, true, true, false, true},
                  null,
                  {false},
                  {false},
                  {false},
                  {false, false},
                  {}
                },
                {
                  {false, true},
                  {},
                  {false, true, false, true, false, false},
                  null,
                  {false},
                  {true},
                  {false},
                  {false, false},
                  {}
                }
              };

          private int index = 0;

          @Override
          public OperatorContext getOperatorContext() {
            return operatorContext;
          }

          @Override
          public TsBlock next() {
            if (timeArray[index] == null) {
              index++;
              return null;
            }
            TsBlockBuilder builder =
                new TsBlockBuilder(
                    timeArray[index].length, Arrays.asList(TSDataType.FLOAT, TSDataType.BOOLEAN));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
              if (valueIsNull[0][index][i]) {
                builder.getColumnBuilder(0).appendNull();
              } else {
                builder.getColumnBuilder(0).writeFloat(value1Array[index][i]);
              }
              if (valueIsNull[1][index][i]) {
                builder.getColumnBuilder(1).appendNull();
              } else {
                builder.getColumnBuilder(1).writeBoolean(value2Array[index][i]);
              }
            }
            builder.declarePositions(timeArray[index].length);
            index++;
            return builder.build();
          }

          @Override
          public boolean hasNext() {
            return index < 9;
          }

          @Override
          public void close() {}

          @Override
          public boolean isFinished() {
            return index >= 9;
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

    LeftOuterTimeJoinOperator leftOuterTimeJoinOperator =
        new LeftOuterTimeJoinOperator(
            operatorContext,
            leftChild,
            2,
            rightChild,
            Arrays.asList(TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.BOOLEAN),
            new DescTimeComparator());

    long[] timeArray = new long[] {25L, 22L, 19L, 18L, 15L, 9L, 7L, 6L, 3L};
    int[] column1Array = new int[] {0, 22, 19, 18, 0, 0, 7, 0, 3};
    boolean[] column1IsNull =
        new boolean[] {true, false, false, false, true, true, false, true, false};
    long[] column2Array = new long[] {26L, 0L, 20L, 0L, 16L, 0L, 0L, 7L, 4L};
    boolean[] column2IsNull =
        new boolean[] {false, true, false, true, false, true, true, false, false};
    float[] column3Array = new float[] {0.0f, 0.0f, 190.0f, 180.0f, 0.0f, 0.0f, 0.0f, 0.0f, 30.0f};
    boolean[] column3IsNull =
        new boolean[] {true, true, false, false, true, true, true, true, false};
    boolean[] column4Array =
        new boolean[] {false, false, true, false, false, false, false, false, false};
    boolean[] column4IsNull =
        new boolean[] {true, true, false, true, false, true, false, true, false};

    try {
      int count = 0;
      ListenableFuture<?> listenableFuture = leftOuterTimeJoinOperator.isBlocked();
      listenableFuture.get();
      while (!leftOuterTimeJoinOperator.isFinished() && leftOuterTimeJoinOperator.hasNext()) {
        TsBlock tsBlock = leftOuterTimeJoinOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++, count++) {
            assertEquals(timeArray[count], tsBlock.getTimeByIndex(i));
            assertEquals(column1IsNull[count], tsBlock.getColumn(0).isNull(i));
            if (!column1IsNull[count]) {
              assertEquals(column1Array[count], tsBlock.getColumn(0).getInt(i));
            }
            assertEquals(column2IsNull[count], tsBlock.getColumn(1).isNull(i));
            if (!column2IsNull[count]) {
              assertEquals(column2Array[count], tsBlock.getColumn(1).getLong(i));
            }
            assertEquals(column3IsNull[count], tsBlock.getColumn(2).isNull(i));
            if (!column3IsNull[count]) {
              assertEquals(column3Array[count], tsBlock.getColumn(2).getFloat(i), 0.000001);
            }
            assertEquals(column4IsNull[count], tsBlock.getColumn(3).isNull(i));
            if (!column4IsNull[count]) {
              assertEquals(column4Array[count], tsBlock.getColumn(3).getBoolean(i));
            }
          }
        }
        listenableFuture = leftOuterTimeJoinOperator.isBlocked();
        listenableFuture.get();
      }
      assertEquals(timeArray.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testLeftOuterJoin3() {
    // left table
    // Time, s1
    // 4     4
    // 6     6
    // 9     9
    // ----------- TsBlock-1
    // 13    13
    // 17    17
    // ----------- TsBlock-2
    // 22    22
    // 25    25
    // ----------- TsBlock-3
    // 100   100
    // 101   101
    // ----------- TsBlock-4
    // 110   110
    // 111   111
    // ----------- TsBlock-5

    // right table
    // Time, s2
    // 1     10
    // 2     20
    // 3     30
    // ----------- TsBlock-1
    // 4     40
    // 5     50
    // 10    100
    // ----------- TsBlock-2
    // 13    130
    // 16    160
    // ----------- TsBlock-3
    // 26    260
    // 27    270
    // ----------- TsBlock-4

    // result table
    // Time, s1,    s2
    // 4      4     40
    // 6      6     null
    // 9      9     null
    // 13     13    130
    // 17     17    null
    // 22     22    null
    // 25     25    null
    // 100   100    null
    // 101   101    null
    // 110   110    null
    // 111   111    null

    OperatorContext operatorContext = Mockito.mock(OperatorContext.class);
    Mockito.when(operatorContext.getMaxRunTime()).thenReturn(new Duration(1, TimeUnit.SECONDS));

    Operator leftChild =
        new Operator() {
          private final long[][] timeArray =
              new long[][] {
                {4L, 6L, 9L},
                {13L, 17L},
                {22L, 25L},
                {100L, 101L},
                {110L, 111L}
              };

          private final int[][] valueArray =
              new int[][] {
                {4, 6, 9},
                {13, 17},
                {22, 25},
                {100, 101},
                {110, 111}
              };

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
                {
                  {false, false, false},
                  {false, false},
                  {false, false},
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
                    timeArray[index].length, Collections.singletonList(TSDataType.INT32));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
              if (valueIsNull[0][index][i]) {
                builder.getColumnBuilder(0).appendNull();
              } else {
                builder.getColumnBuilder(0).writeInt(valueArray[index][i]);
              }
            }
            builder.declarePositions(timeArray[index].length);
            index++;
            return builder.build();
          }

          @Override
          public boolean hasNext() {
            return index < 5;
          }

          @Override
          public void close() {}

          @Override
          public boolean isFinished() {
            return index >= 5;
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
              new long[][] {
                {1L, 2L, 3L},
                {4L, 5L, 10L},
                {13L, 16L},
                {26L, 27L}
              };

          private final long[][] valueArray =
              new long[][] {
                {10L, 20L, 30L},
                {40L, 50L, 100L},
                {130L, 160L},
                {260L, 270L}
              };

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
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
                    timeArray[index].length, Collections.singletonList(TSDataType.INT64));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
              if (valueIsNull[0][index][i]) {
                builder.getColumnBuilder(0).appendNull();
              } else {
                builder.getColumnBuilder(0).writeLong(valueArray[index][i]);
              }
            }
            builder.declarePositions(timeArray[index].length);
            index++;
            return builder.build();
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

    LeftOuterTimeJoinOperator leftOuterTimeJoinOperator =
        new LeftOuterTimeJoinOperator(
            operatorContext,
            leftChild,
            1,
            rightChild,
            Arrays.asList(TSDataType.INT32, TSDataType.INT64),
            new AscTimeComparator());

    assertEquals(
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes() + 64 * 1024 * 2,
        leftOuterTimeJoinOperator.calculateMaxPeekMemory());
    assertEquals(
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes(),
        leftOuterTimeJoinOperator.calculateMaxReturnSize());
    assertEquals(64 * 1024 * 2, leftOuterTimeJoinOperator.calculateRetainedSizeAfterCallingNext());

    long[] timeArray = new long[] {4L, 6L, 9L, 13L, 17L, 22L, 25L, 100L, 101L, 110L, 111L};
    int[] column1Array = new int[] {4, 6, 9, 13, 17, 22, 25, 100, 101, 110, 111};
    boolean[] column1IsNull =
        new boolean[] {false, false, false, false, false, false, false, false, false, false, false};
    long[] column2Array = new long[] {40L, 0L, 0L, 130L, 0L, 0L, 0L, 0L, 0L, 0L, 0L};
    boolean[] column2IsNull =
        new boolean[] {false, true, true, false, true, true, true, true, true, true, true};

    try {
      int count = 0;
      ListenableFuture<?> listenableFuture = leftOuterTimeJoinOperator.isBlocked();
      listenableFuture.get();
      while (!leftOuterTimeJoinOperator.isFinished() && leftOuterTimeJoinOperator.hasNext()) {
        TsBlock tsBlock = leftOuterTimeJoinOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++, count++) {
            assertEquals(timeArray[count], tsBlock.getTimeByIndex(i));
            assertEquals(column1IsNull[count], tsBlock.getColumn(0).isNull(i));
            if (!column1IsNull[count]) {
              assertEquals(column1Array[count], tsBlock.getColumn(0).getInt(i));
            }
            assertEquals(column2IsNull[count], tsBlock.getColumn(1).isNull(i));
            if (!column2IsNull[count]) {
              assertEquals(column2Array[count], tsBlock.getColumn(1).getLong(i));
            }
          }
        }
        listenableFuture = leftOuterTimeJoinOperator.isBlocked();
        listenableFuture.get();
      }
      assertEquals(timeArray.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
