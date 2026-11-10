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

package org.apache.iotdb.calc.execution.operator.process;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class UnpivotOperatorTest {
  private static final class Context extends CommonOperatorContext {
    private Context() {
      super(0, new PlanNodeId("unpivot"), "test");
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

  private static final class Input implements Operator {
    private final Context context = new Context();
    private final TsBlock[] blocks;
    private int index;
    private boolean closed;

    private Input(TsBlock... blocks) {
      this.blocks = blocks;
    }

    @Override
    public CommonOperatorContext getOperatorContext() {
      return context;
    }

    @Override
    public ListenableFuture<?> isBlocked() {
      return NOT_BLOCKED;
    }

    @Override
    public boolean hasNext() {
      return index < blocks.length;
    }

    @Override
    public TsBlock next() {
      if (!hasNext()) {
        throw new IllegalStateException();
      }
      return blocks[index++];
    }

    @Override
    public boolean isFinished() {
      return !hasNext();
    }

    @Override
    public void close() {
      closed = true;
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

  private static TsBlock input() {
    // The logical time is a value column; the block's dedicated time slot is unrelated.
    TsBlockBuilder builder =
        new TsBlockBuilder(
            Arrays.asList(TSDataType.INT64, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32));
    for (int row = 0; row < 2; row++) {
      builder.getTimeColumnBuilder().writeLong(999);
      builder.getColumnBuilder(0).writeLong(10L + row);
      builder.getColumnBuilder(1).writeInt(20 + row);
      if (row == 0) {
        builder.getColumnBuilder(2).writeInt(30);
      } else {
        builder.getColumnBuilder(2).appendNull();
      }
      builder.getColumnBuilder(3).writeInt(40 + row);
      builder.declarePosition();
    }
    return builder.build();
  }

  private static UnpivotOperator operator(Input input, boolean includeNulls) {
    return new UnpivotOperator(
        new Context(),
        input,
        new int[] {0},
        new int[][] {{1, 2}, {1, 3}},
        Arrays.asList("first", "second"),
        includeNulls,
        Arrays.asList(TSDataType.INT64, TSDataType.STRING, TSDataType.INT32, TSDataType.INT32));
  }

  @Test
  public void testRowMajorOrderNullsAndLogicalTime() throws Exception {
    Input child = new Input(input());
    UnpivotOperator operator = operator(child, false);
    TsBlock output = operator.next();
    assertEquals(3, output.getPositionCount());
    assertEquals(10L, output.getColumn(0).getLong(0));
    assertEquals("first", output.getColumn(1).getBinary(0).toString());
    assertEquals(30, output.getColumn(3).getInt(0));
    assertEquals("second", output.getColumn(1).getBinary(1).toString());
    assertEquals(11L, output.getColumn(0).getLong(2));
    assertEquals(41, output.getColumn(3).getInt(2));
    assertFalse(output.getTimeByIndex(0) == 999L);
    assertTrue(operator.isFinished());
    assertFalse(operator.hasNext());
    operator.close();
    assertTrue(child.closed);
  }

  @Test
  public void testIncludeNullsAndEmptyInput() throws Exception {
    UnpivotOperator included = operator(new Input(input()), true);
    TsBlock output = included.next();
    assertEquals(4, output.getPositionCount());
    assertTrue(output.getColumn(3).isNull(2));
    included.close();

    UnpivotOperator empty = operator(new Input(), false);
    assertNull(empty.next());
    assertTrue(empty.isFinished());
    empty.close();
  }

  @Test
  public void testBlockBoundary() throws Exception {
    UnpivotOperator operator = operator(new Input(input(), input()), false);
    assertEquals(6, operator.next().getPositionCount());
    assertTrue(operator.isFinished());
    operator.close();
  }
}
