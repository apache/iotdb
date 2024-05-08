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
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class InnerTimeJoinOperatorTest {

  @Test
  public void testInnerJoin1() {
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
    // 4,     4,    40
    // 13     13,   130

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

    Map<InputLocation, Integer> outputColumnMap = new HashMap<>();
    outputColumnMap.put(new InputLocation(0, 0), 0);
    outputColumnMap.put(new InputLocation(1, 0), 1);

    InnerTimeJoinOperator innerTimeJoinOperator =
        new InnerTimeJoinOperator(
            operatorContext,
            Arrays.asList(leftChild, rightChild),
            Arrays.asList(TSDataType.INT32, TSDataType.INT64),
            new AscTimeComparator(),
            outputColumnMap);

    assertEquals(
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes() + 64 * 1024 * 2,
        innerTimeJoinOperator.calculateMaxPeekMemory());
    assertEquals(
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes(),
        innerTimeJoinOperator.calculateMaxReturnSize());
    assertEquals(64 * 1024, innerTimeJoinOperator.calculateRetainedSizeAfterCallingNext());

    long[] timeArray = new long[] {4L, 13L};
    int[] column1Array = new int[] {4, 13};
    boolean[] column1IsNull = new boolean[] {false, false};
    long[] column2Array = new long[] {40L, 130L};
    boolean[] column2IsNull = new boolean[] {false, false};

    try {
      int count = 0;
      ListenableFuture<?> listenableFuture = innerTimeJoinOperator.isBlocked();
      listenableFuture.get();
      while (!innerTimeJoinOperator.isFinished() && innerTimeJoinOperator.hasNext()) {
        TsBlock tsBlock = innerTimeJoinOperator.next();
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
        listenableFuture = innerTimeJoinOperator.isBlocked();
        listenableFuture.get();
      }
      assertEquals(timeArray.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testInnerJoin2() {
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
    // 19    19      20      190.0   true
    // 18    18      null    180.0   null
    // 15    null    16      null    false
    // 7     7       null    null    false
    // 3     3       4       30.0    false

    OperatorContext operatorContext = Mockito.mock(OperatorContext.class);
    Mockito.when(operatorContext.getMaxRunTime()).thenReturn(new Duration(1000, TimeUnit.SECONDS));

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

    Map<InputLocation, Integer> outputColumnMap = new HashMap<>();
    outputColumnMap.put(new InputLocation(0, 0), 0);
    outputColumnMap.put(new InputLocation(0, 1), 1);
    outputColumnMap.put(new InputLocation(1, 0), 2);
    outputColumnMap.put(new InputLocation(1, 1), 3);

    InnerTimeJoinOperator innerTimeJoinOperator =
        new InnerTimeJoinOperator(
            operatorContext,
            Arrays.asList(leftChild, rightChild),
            Arrays.asList(TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.BOOLEAN),
            new DescTimeComparator(),
            outputColumnMap);

    long[] timeArray = new long[] {19L, 18L, 15L, 7L, 3L};
    int[] column1Array = new int[] {19, 18, 0, 7, 3};
    boolean[] column1IsNull = new boolean[] {false, false, true, false, false};
    long[] column2Array = new long[] {20L, 0L, 16L, 0L, 4L};
    boolean[] column2IsNull = new boolean[] {false, true, false, true, false};
    float[] column3Array = new float[] {190.0f, 180.0f, 0.0f, 0.0f, 30.0f};
    boolean[] column3IsNull = new boolean[] {false, false, true, true, false};
    boolean[] column4Array = new boolean[] {true, false, false, false, false};
    boolean[] column4IsNull = new boolean[] {false, true, false, false, false};

    try {
      int count = 0;
      ListenableFuture<?> listenableFuture = innerTimeJoinOperator.isBlocked();
      listenableFuture.get();
      while (!innerTimeJoinOperator.isFinished() && innerTimeJoinOperator.hasNext()) {
        TsBlock tsBlock = innerTimeJoinOperator.next();
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
        listenableFuture = innerTimeJoinOperator.isBlocked();
        listenableFuture.get();
      }
      assertEquals(timeArray.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testInnerJoin3() {
    // child-1
    // Time, s1,     s2
    // 100   100     200
    // 90    90      180
    // 80    80      160
    // 70    70      140
    // 60    60      120
    // 50    50      100
    // 40    40      80
    // 30    30      60
    // 20    20      40
    // 10    10      20
    // 0     0       0
    // ---------------------- TsBlock-1

    // child-2
    // Time,   s3,        s4
    // 1000    3000.0     false
    // 500     500.0      true
    // 100     300.0      null
    // ------------------------- TsBlock-1
    // 99      99.0       true
    // 95      95.0       null
    // 90      null       false
    // ------------------------- TsBlock-2
    // 50      150.0      true
    // 48      48.0       true
    // 20      60.0       null
    // 10      null       false
    // ------------------------- TsBlock-3

    // result table
    // Time, s1,     s2,     s3,     s4
    // 100   100     200     300.0   null
    // 90    90      180     null    false
    // 50    50      100     150.0   true
    // 20    20      40      60.0    null
    // 10    10      20      null    false

    OperatorContext operatorContext = Mockito.mock(OperatorContext.class);
    Mockito.when(operatorContext.getMaxRunTime()).thenReturn(new Duration(1000, TimeUnit.SECONDS));

    Operator child1 =
        new Operator() {
          private final long[][] timeArray =
              new long[][] {{100L, 90L, 80L, 70L, 60L, 50L, 40L, 30L, 20L, 10L, 0L}};

          private final int[][] value1Array =
              new int[][] {{100, 90, 80, 70, 60, 50, 40, 30, 20, 10, 0}};

          private final long[][] value2Array =
              new long[][] {{200L, 180L, 160, 140, 120, 100L, 80L, 60L, 40L, 20L, 0L}};

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
                {{false, false, false, false, false, false, false, false, false, false, false}},
                {{false, false, false, false, false, false, false, false, false, false, false}}
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
            return index < 1;
          }

          @Override
          public void close() {}

          @Override
          public boolean isFinished() {
            return index >= 1;
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

    Operator child2 =
        new Operator() {
          private final long[][] timeArray =
              new long[][] {{1000L, 500L, 100L}, {99L, 95L, 90L}, {50L, 48L, 20L, 10L}};

          private final float[][] value1Array =
              new float[][] {
                {3000.0f, 500.0f, 300.0f},
                {99.0f, 95.0f, 0.0f},
                {150.0f, 48.0f, 60.0f, 0.0f}
              };

          private final boolean[][] value2Array =
              new boolean[][] {
                {false, true, false},
                {true, false, false},
                {true, true, false, false}
              };

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
                {
                  {false, false, false},
                  {false, false, true},
                  {false, false, false, true}
                },
                {
                  {false, false, true},
                  {false, true, false},
                  {false, false, true, false}
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

    Map<InputLocation, Integer> outputColumnMap = new HashMap<>();
    outputColumnMap.put(new InputLocation(0, 0), 0);
    outputColumnMap.put(new InputLocation(0, 1), 1);
    outputColumnMap.put(new InputLocation(1, 0), 2);
    outputColumnMap.put(new InputLocation(1, 1), 3);

    InnerTimeJoinOperator innerTimeJoinOperator =
        new InnerTimeJoinOperator(
            operatorContext,
            Arrays.asList(child1, child2),
            Arrays.asList(TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.BOOLEAN),
            new DescTimeComparator(),
            outputColumnMap);

    long[] timeArray = new long[] {100L, 90L, 50L, 20L, 10L};
    int[] column1Array = new int[] {100, 90, 50, 20, 10};
    boolean[] column1IsNull = new boolean[] {false, false, false, false, false};
    long[] column2Array = new long[] {200L, 180L, 100L, 40L, 20L};
    boolean[] column2IsNull = new boolean[] {false, false, false, false, false};
    float[] column3Array = new float[] {300.0f, 0.0f, 150.0f, 60.0f, 0.0f};
    boolean[] column3IsNull = new boolean[] {false, true, false, false, true};
    boolean[] column4Array = new boolean[] {false, false, true, false, false};
    boolean[] column4IsNull = new boolean[] {true, false, false, true, false};

    try {
      int count = 0;
      ListenableFuture<?> listenableFuture = innerTimeJoinOperator.isBlocked();
      listenableFuture.get();
      while (!innerTimeJoinOperator.isFinished() && innerTimeJoinOperator.hasNext()) {
        TsBlock tsBlock = innerTimeJoinOperator.next();
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
        listenableFuture = innerTimeJoinOperator.isBlocked();
        listenableFuture.get();
      }
      assertEquals(timeArray.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testInnerJoin4() {
    // child-1
    // Time, s1,     s2
    // 100   100     200
    // 90    90      180
    // 80    80      160
    // 70    70      140
    // 60    60      120
    // 50    50      100
    // 40    40      80
    // 30    30      60
    // 20    20      40
    // 10    10      20
    // 0     0       0
    // ---------------------- TsBlock-1

    // child-2
    // Time,   s3,        s4
    // 1000    3000.0     false
    // 500     500.0      true
    // 100     300.0      null
    // ------------------------- TsBlock-1
    // 99      99.0       true
    // 95      95.0       null
    // 90      null       false
    // ------------------------- TsBlock-2
    // 50      150.0      true
    // 48      48.0       true
    // 20      60.0       null
    // 10      null       false
    // ------------------------- TsBlock-3

    // child-3
    // Time,   s5,
    // 1000    "iotdb"
    // 500     "ty"
    // 101     "zm"
    // --------------------- TsBlock-1
    // 99      "ty"
    // 95      "love"
    // 80      "zm"
    // 60      "2018-05-06"
    // --------------------- TsBlock-2
    // 40      "1997-09-09"
    // 22      "1995-04-21"
    // 11      "2022-04-21"
    // 0       "2023-12-30"
    // --------------------- TsBlock-3

    // result table
    // Time, s1,     s2,     s3,     s4,     s5
    // empty

    OperatorContext operatorContext = Mockito.mock(OperatorContext.class);
    Mockito.when(operatorContext.getMaxRunTime()).thenReturn(new Duration(1000, TimeUnit.SECONDS));

    Operator child1 =
        new Operator() {
          private final long[][] timeArray =
              new long[][] {{100L, 90L, 80L, 70L, 60L, 50L, 40L, 30L, 20L, 10L, 0L}};

          private final int[][] value1Array =
              new int[][] {{100, 90, 80, 70, 60, 50, 40, 30, 20, 10, 0}};

          private final long[][] value2Array =
              new long[][] {{200L, 180L, 160, 140, 120, 100L, 80L, 60L, 40L, 20L, 0L}};

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
                {{false, false, false, false, false, false, false, false, false, false, false}},
                {{false, false, false, false, false, false, false, false, false, false, false}}
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
            return index < 1;
          }

          @Override
          public void close() {}

          @Override
          public boolean isFinished() {
            return index >= 1;
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

    Operator child2 =
        new Operator() {
          private final long[][] timeArray =
              new long[][] {{1000L, 500L, 100L}, {99L, 95L, 90L}, {50L, 48L, 20L, 10L}};

          private final float[][] value1Array =
              new float[][] {
                {3000.0f, 500.0f, 300.0f},
                {99.0f, 95.0f, 0.0f},
                {150.0f, 48.0f, 60.0f, 0.0f}
              };

          private final boolean[][] value2Array =
              new boolean[][] {
                {false, true, false},
                {true, false, false},
                {true, true, false, false}
              };

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
                {
                  {false, false, false},
                  {false, false, true},
                  {false, false, false, true}
                },
                {
                  {false, false, true},
                  {false, true, false},
                  {false, false, true, false}
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

    Operator child3 =
        new Operator() {
          private final long[][] timeArray =
              new long[][] {{1000L, 500L, 101L}, {99L, 95L, 80L, 60L}, {40L, 22L, 11L, 0L}};

          private final Binary[][] value1Array =
              new Binary[][] {
                {
                  new Binary("iotdb".getBytes(StandardCharsets.UTF_8)),
                  new Binary("ty".getBytes(StandardCharsets.UTF_8)),
                  new Binary("zm".getBytes(StandardCharsets.UTF_8))
                },
                {
                  new Binary("ty".getBytes(StandardCharsets.UTF_8)),
                  new Binary("love".getBytes(StandardCharsets.UTF_8)),
                  new Binary("zm".getBytes(StandardCharsets.UTF_8)),
                  new Binary("2018-05-06".getBytes(StandardCharsets.UTF_8))
                },
                {
                  new Binary("1997-09-09".getBytes(StandardCharsets.UTF_8)),
                  new Binary("1995-04-21".getBytes(StandardCharsets.UTF_8)),
                  new Binary("2022-04-21".getBytes(StandardCharsets.UTF_8)),
                  new Binary("2023-12-30".getBytes(StandardCharsets.UTF_8))
                }
              };

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
                {
                  {false, false, false},
                  {false, false, false, false},
                  {false, false, false, false}
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
                new TsBlockBuilder(timeArray[index].length, Arrays.asList(TSDataType.TEXT));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
              if (valueIsNull[0][index][i]) {
                builder.getColumnBuilder(0).appendNull();
              } else {
                builder.getColumnBuilder(0).writeBinary(value1Array[index][i]);
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

    Map<InputLocation, Integer> outputColumnMap = new HashMap<>();
    outputColumnMap.put(new InputLocation(0, 0), 0);
    outputColumnMap.put(new InputLocation(0, 1), 1);
    outputColumnMap.put(new InputLocation(1, 0), 2);
    outputColumnMap.put(new InputLocation(1, 1), 3);
    outputColumnMap.put(new InputLocation(2, 0), 4);

    InnerTimeJoinOperator innerTimeJoinOperator =
        new InnerTimeJoinOperator(
            operatorContext,
            Arrays.asList(child1, child2, child3),
            Arrays.asList(
                TSDataType.INT32,
                TSDataType.INT64,
                TSDataType.FLOAT,
                TSDataType.BOOLEAN,
                TSDataType.TEXT),
            new DescTimeComparator(),
            outputColumnMap);

    try {
      int count = 0;
      ListenableFuture<?> listenableFuture = innerTimeJoinOperator.isBlocked();
      listenableFuture.get();
      while (!innerTimeJoinOperator.isFinished() && innerTimeJoinOperator.hasNext()) {
        TsBlock tsBlock = innerTimeJoinOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          count += tsBlock.getPositionCount();
        }
        listenableFuture = innerTimeJoinOperator.isBlocked();
        listenableFuture.get();
      }
      assertEquals(0, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testInnerJoin5() {
    // child-1
    // Time, s1,     s2
    // 100   100     200
    // 90    90      180
    // 80    80      160
    // 70    70      140
    // 60    60      120
    // 50    50      100
    // 40    40      80
    // 30    30      60
    // 20    20      40
    // 10    10      20
    // 0     0       0
    // ---------------------- TsBlock-1

    // child-2
    // Time,   s3,        s4
    // 1000    3000.0     false
    // 500     500.0      true
    // 100     300.0      null
    // ------------------------- TsBlock-1
    // 99      99.0       true
    // 95      95.0       null
    // 90      null       false
    // ------------------------- TsBlock-2
    // 50      150.0      true
    // 48      48.0       true
    // 20      60.0       null
    // 10      null       false
    // ------------------------- TsBlock-3

    // child-3
    // Time,   s5,
    // 1000    "iotdb"
    // 500     "ty"
    // 101     "zm"
    // --------------------- TsBlock-1
    // 99      "ty"
    // 90      "love"
    // 80      "zm"
    // 60      "2018-05-06"
    // --------------------- TsBlock-2
    // 40      "1997-09-09"
    // 22      "1995-04-21"
    // 11      "2022-04-21"
    // 0       "2023-12-30"
    // --------------------- TsBlock-3

    // result table
    // Time, s1,     s2,     s3,     s4,     s5
    // 90    90      180     null    false   "love"

    OperatorContext operatorContext = Mockito.mock(OperatorContext.class);
    Mockito.when(operatorContext.getMaxRunTime()).thenReturn(new Duration(1000, TimeUnit.SECONDS));

    Operator child1 =
        new Operator() {
          private final long[][] timeArray =
              new long[][] {{100L, 90L, 80L, 70L, 60L, 50L, 40L, 30L, 20L, 10L, 0L}};

          private final int[][] value1Array =
              new int[][] {{100, 90, 80, 70, 60, 50, 40, 30, 20, 10, 0}};

          private final long[][] value2Array =
              new long[][] {{200L, 180L, 160, 140, 120, 100L, 80L, 60L, 40L, 20L, 0L}};

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
                {{false, false, false, false, false, false, false, false, false, false, false}},
                {{false, false, false, false, false, false, false, false, false, false, false}}
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
            return index < 1;
          }

          @Override
          public void close() {}

          @Override
          public boolean isFinished() {
            return index >= 1;
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

    Operator child2 =
        new Operator() {
          private final long[][] timeArray =
              new long[][] {{1000L, 500L, 100L}, {99L, 95L, 90L}, {50L, 48L, 20L, 10L}};

          private final float[][] value1Array =
              new float[][] {
                {3000.0f, 500.0f, 300.0f},
                {99.0f, 95.0f, 0.0f},
                {150.0f, 48.0f, 60.0f, 0.0f}
              };

          private final boolean[][] value2Array =
              new boolean[][] {
                {false, true, false},
                {true, false, false},
                {true, true, false, false}
              };

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
                {
                  {false, false, false},
                  {false, false, true},
                  {false, false, false, true}
                },
                {
                  {false, false, true},
                  {false, true, false},
                  {false, false, true, false}
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

    Operator child3 =
        new Operator() {
          private final long[][] timeArray =
              new long[][] {{1000L, 500L, 101L}, {99L, 90L, 80L, 60L}, {40L, 22L, 11L, 0L}};

          private final Binary[][] value1Array =
              new Binary[][] {
                {
                  new Binary("iotdb".getBytes(StandardCharsets.UTF_8)),
                  new Binary("ty".getBytes(StandardCharsets.UTF_8)),
                  new Binary("zm".getBytes(StandardCharsets.UTF_8))
                },
                {
                  new Binary("ty".getBytes(StandardCharsets.UTF_8)),
                  new Binary("love".getBytes(StandardCharsets.UTF_8)),
                  new Binary("zm".getBytes(StandardCharsets.UTF_8)),
                  new Binary("2018-05-06".getBytes(StandardCharsets.UTF_8))
                },
                {
                  new Binary("1997-09-09".getBytes(StandardCharsets.UTF_8)),
                  new Binary("1995-04-21".getBytes(StandardCharsets.UTF_8)),
                  new Binary("2022-04-21".getBytes(StandardCharsets.UTF_8)),
                  new Binary("2023-12-30".getBytes(StandardCharsets.UTF_8))
                }
              };

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
                {
                  {false, false, false},
                  {false, false, false, false},
                  {false, false, false, false}
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
                new TsBlockBuilder(timeArray[index].length, Arrays.asList(TSDataType.TEXT));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
              if (valueIsNull[0][index][i]) {
                builder.getColumnBuilder(0).appendNull();
              } else {
                builder.getColumnBuilder(0).writeBinary(value1Array[index][i]);
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

    Map<InputLocation, Integer> outputColumnMap = new HashMap<>();
    outputColumnMap.put(new InputLocation(0, 0), 0);
    outputColumnMap.put(new InputLocation(0, 1), 1);
    outputColumnMap.put(new InputLocation(1, 0), 2);
    outputColumnMap.put(new InputLocation(1, 1), 3);
    outputColumnMap.put(new InputLocation(2, 0), 4);

    InnerTimeJoinOperator innerTimeJoinOperator =
        new InnerTimeJoinOperator(
            operatorContext,
            Arrays.asList(child1, child2, child3),
            Arrays.asList(
                TSDataType.INT32,
                TSDataType.INT64,
                TSDataType.FLOAT,
                TSDataType.BOOLEAN,
                TSDataType.TEXT),
            new DescTimeComparator(),
            outputColumnMap);

    try {
      int count = 0;
      ListenableFuture<?> listenableFuture = innerTimeJoinOperator.isBlocked();
      listenableFuture.get();
      while (!innerTimeJoinOperator.isFinished() && innerTimeJoinOperator.hasNext()) {
        TsBlock tsBlock = innerTimeJoinOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          count += tsBlock.getPositionCount();
          assertEquals(90, tsBlock.getTimeByIndex(0));
          assertFalse(tsBlock.getColumn(0).isNull(0));
          assertEquals(90, tsBlock.getColumn(0).getInt(0));
          assertFalse(tsBlock.getColumn(1).isNull(0));
          assertEquals(180L, tsBlock.getColumn(1).getLong(0));
          assertTrue(tsBlock.getColumn(2).isNull(0));
          assertFalse(tsBlock.getColumn(3).isNull(0));
          assertFalse(tsBlock.getColumn(3).getBoolean(0));
          assertFalse(tsBlock.getColumn(4).isNull(0));
          assertEquals(
              "love", tsBlock.getColumn(4).getBinary(0).getStringValue(StandardCharsets.UTF_8));
        }
        listenableFuture = innerTimeJoinOperator.isBlocked();
        listenableFuture.get();
      }
      assertEquals(1, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
