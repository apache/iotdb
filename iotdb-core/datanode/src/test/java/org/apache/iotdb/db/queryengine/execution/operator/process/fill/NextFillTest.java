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

package org.apache.iotdb.db.queryengine.execution.operator.process.fill;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.execution.operator.process.TableNextFillWithGroupOperator;
import org.apache.iotdb.calc.execution.operator.process.fill.ILinearFill;
import org.apache.iotdb.calc.plan.planner.CommonOperatorUtils;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.TimeDuration;
import org.junit.Test;

import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NextFillTest {

  @Test
  public void testNextFillAcrossTsBlocks() {
    ILinearFill fill =
        CommonOperatorUtils.getNextFill(
            1, ImmutableList.of(TSDataType.INT32), null, ZoneId.systemDefault())[0];
    IntColumn valueColumn =
        new IntColumn(3, Optional.of(new boolean[] {true, true, true}), new int[3]);
    LongColumn timeColumn = new LongColumn(3, Optional.empty(), new long[] {1, 2, 3});
    IntColumn nextValueColumn =
        new IntColumn(2, Optional.of(new boolean[] {true, false}), new int[] {0, 9});
    LongColumn nextTimeColumn = new LongColumn(2, Optional.empty(), new long[] {4, 5});

    assertTrue(fill.needPrepareForNext(2, valueColumn, 2));
    assertTrue(fill.prepareForNext(3, 2, nextTimeColumn, nextValueColumn));

    IntColumn result = (IntColumn) fill.fill(timeColumn, valueColumn, 0);
    for (int i = 0; i < result.getPositionCount(); i++) {
      assertFalse(result.isNull(i));
      assertEquals(9, result.getInt(i));
    }
  }

  @Test
  public void testNextFillTimeBoundAndHelperNull() {
    ILinearFill fill =
        CommonOperatorUtils.getNextFill(
            1, ImmutableList.of(TSDataType.INT32), new TimeDuration(0, 2), ZoneId.systemDefault())[
            0];
    IntColumn valueColumn =
        new IntColumn(3, Optional.of(new boolean[] {true, true, true}), new int[3]);
    LongColumn timeColumn =
        new LongColumn(3, Optional.of(new boolean[] {false, true, false}), new long[] {1, 0, 3});
    IntColumn nextValueColumn = new IntColumn(1, Optional.empty(), new int[] {40});
    LongColumn nextTimeColumn = new LongColumn(1, Optional.empty(), new long[] {4});

    assertTrue(fill.needPrepareForNext(2, valueColumn, 2));
    assertTrue(fill.prepareForNext(3, 2, nextTimeColumn, nextValueColumn));

    IntColumn result = (IntColumn) fill.fill(timeColumn, valueColumn, 0);
    assertTrue(result.isNull(0));
    assertTrue(result.isNull(1));
    assertFalse(result.isNull(2));
    assertEquals(40, result.getInt(2));
  }

  @Test
  public void testObjectNextFillUsesBinaryFill() {
    ILinearFill fill =
        CommonOperatorUtils.getNextFill(
            1, ImmutableList.of(TSDataType.OBJECT), null, ZoneId.systemDefault())[0];
    Binary objectValue = new Binary("object-value", TSFileConfig.STRING_CHARSET);
    BinaryColumn valueColumn =
        new BinaryColumn(
            2,
            Optional.of(new boolean[] {true, false}),
            new Binary[] {Binary.EMPTY_VALUE, objectValue});
    LongColumn timeColumn = new LongColumn(2, Optional.empty(), new long[] {1, 2});

    BinaryColumn result = (BinaryColumn) fill.fill(timeColumn, valueColumn, 0);
    assertFalse(result.isNull(0));
    assertEquals(objectValue, result.getBinary(0));
    assertEquals(objectValue, result.getBinary(1));
  }

  @Test
  public void testNextFillWithGroupDoesNotUseNextGroupAfterContinuedGroup() throws Exception {
    List<TSDataType> dataTypes = ImmutableList.of(TSDataType.TEXT, TSDataType.INT32);
    CommonOperatorContext operatorContext = new TestOperatorContext();
    TableNextFillWithGroupOperator operator =
        new TableNextFillWithGroupOperator(
            operatorContext,
            CommonOperatorUtils.getNextFill(2, dataTypes, null, ZoneId.systemDefault()),
            new TsBlockSourceOperator(
                operatorContext,
                ImmutableList.of(
                    buildBlock(new String[] {"a"}, new Integer[] {1}),
                    buildBlock(new String[] {"a", "b"}, new Integer[] {null, 9}))),
            -1,
            false,
            (left, right) ->
                left.tsBlock
                    .getColumn(0)
                    .getBinary(left.rowIndex)
                    .toString()
                    .compareTo(right.tsBlock.getColumn(0).getBinary(right.rowIndex).toString()),
            dataTypes);

    assertTrue(operator.hasNext());
    TsBlock firstBlock = operator.next();
    assertEquals(1, firstBlock.getPositionCount());
    assertEquals(1, firstBlock.getColumn(1).getInt(0));

    assertTrue(operator.hasNext());
    TsBlock secondBlock = operator.next();
    assertEquals(2, secondBlock.getPositionCount());
    assertEquals("a", secondBlock.getColumn(0).getBinary(0).toString());
    assertTrue(secondBlock.getColumn(1).isNull(0));
    assertEquals("b", secondBlock.getColumn(0).getBinary(1).toString());
    assertEquals(9, secondBlock.getColumn(1).getInt(1));
  }

  private static TsBlock buildBlock(String[] groups, Integer[] values) {
    TsBlockBuilder builder =
        new TsBlockBuilder(groups.length, ImmutableList.of(TSDataType.TEXT, TSDataType.INT32));
    for (int i = 0; i < groups.length; i++) {
      builder.getColumnBuilder(0).writeBinary(new Binary(groups[i], TSFileConfig.STRING_CHARSET));
      if (values[i] == null) {
        builder.getColumnBuilder(1).appendNull();
      } else {
        builder.getColumnBuilder(1).writeInt(values[i]);
      }
    }
    builder.declarePositions(groups.length);
    return builder.build(
        new RunLengthEncodedColumn(CommonOperatorUtils.TIME_COLUMN_TEMPLATE, groups.length));
  }

  private static class TsBlockSourceOperator implements Operator {

    private final CommonOperatorContext operatorContext;
    private final List<TsBlock> tsBlocks;
    private int index;

    private TsBlockSourceOperator(CommonOperatorContext operatorContext, List<TsBlock> tsBlocks) {
      this.operatorContext = operatorContext;
      this.tsBlocks = tsBlocks;
    }

    @Override
    public CommonOperatorContext getOperatorContext() {
      return operatorContext;
    }

    @Override
    public TsBlock next() {
      return index < tsBlocks.size() ? tsBlocks.get(index++) : null;
    }

    @Override
    public boolean hasNext() {
      return index < tsBlocks.size();
    }

    @Override
    public void close() {
      // No resources.
    }

    @Override
    public boolean isFinished() {
      return index >= tsBlocks.size();
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

  private static class TestOperatorContext extends CommonOperatorContext {

    private TestOperatorContext() {
      super(0, new PlanNodeId("test"), "test");
    }

    @Override
    public MemoryReservationManager getMemoryReservationContext() {
      return null;
    }

    @Override
    public int getFragmentId() {
      return 0;
    }

    @Override
    public int getPipelineId() {
      return 0;
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }
  }
}
