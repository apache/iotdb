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

package org.apache.iotdb.calc.execution.operator.process.gapfill;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.plan.planner.CommonOperatorUtils;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GapFillSaturatedEndpointTest {

  private static final long STEP = 3600000L;

  private static final class TestContext extends CommonOperatorContext {
    private TestContext(String operatorType) {
      super(0, new PlanNodeId("gapfill"), operatorType);
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

  /** Child operator that emits one single-row TsBlock and then reports itself exhausted. */
  private static final class SingleRowChild implements Operator {
    private final CommonOperatorContext context = new TestContext("test-child");
    private final long timestamp;
    private final double value;
    private boolean emitted = false;

    private SingleRowChild(long timestamp, double value) {
      this.timestamp = timestamp;
      this.value = value;
    }

    @Override
    public CommonOperatorContext getOperatorContext() {
      return context;
    }

    @Override
    public TsBlock next() {
      if (emitted) {
        return null;
      }
      TsBlockBuilder builder =
          new TsBlockBuilder(1, Arrays.asList(TSDataType.TIMESTAMP, TSDataType.DOUBLE));
      builder.getColumnBuilder(0).writeLong(timestamp);
      builder.getColumnBuilder(1).writeDouble(value);
      builder.declarePositions(1);
      emitted = true;
      return builder.build(
          new RunLengthEncodedColumn(
              CommonOperatorUtils.TIME_COLUMN_TEMPLATE, builder.getPositionCount()));
    }

    @Override
    public boolean hasNext() {
      return !emitted;
    }

    @Override
    public boolean isFinished() {
      return emitted;
    }

    @Override
    public void close() {}

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

  @Test
  public void endpointAtLongMaxValueIsEmittedExactlyOnceAndOperatorFinishes() throws Exception {
    // startTime is three date_bin steps below Long.MAX_VALUE, so nextDateBin() saturates at
    // Long.MAX_VALUE right after the endpoint has been emitted.
    final long startTime = Long.MAX_VALUE - 3 * STEP;
    final long endTime = Long.MAX_VALUE;

    final List<Long> actualTimes = new ArrayList<>();
    try (Operator gapFillOperator =
        new GapFillWoGroupWoMoOperator(
            new TestContext("gapfill"),
            new SingleRowChild(startTime, 1.0),
            0,
            startTime,
            endTime,
            Arrays.asList(TSDataType.TIMESTAMP, TSDataType.DOUBLE),
            STEP)) {

      // Guard against the pre-fix behaviour where next() kept re-emitting the endpoint forever.
      for (int i = 0; i < 10 && !gapFillOperator.isFinished(); i++) {
        if (!gapFillOperator.hasNext()) {
          break;
        }
        TsBlock block = gapFillOperator.next();
        assertTrue(block != null && !block.isEmpty());
        for (int row = 0, size = block.getPositionCount(); row < size; row++) {
          actualTimes.add(block.getColumn(0).getLong(row));
        }
      }

      assertEquals(
          Arrays.asList(startTime, startTime + STEP, startTime + 2 * STEP, Long.MAX_VALUE),
          actualTimes);
      assertEquals(1, actualTimes.stream().filter(t -> t == Long.MAX_VALUE).count());
      assertTrue(
          "operator must finish once the endpoint has been emitted", gapFillOperator.isFinished());
    }
  }

  @Test
  public void realRowAtSaturatedEndpointIsNotDuplicated() throws Exception {
    // The real row itself sits on the inclusive endpoint; no extra gap row may be emitted there.
    final long startTime = Long.MAX_VALUE - 2 * STEP;

    final List<Long> actualTimes = new ArrayList<>();
    try (Operator gapFillOperator =
        new GapFillWoGroupWoMoOperator(
            new TestContext("gapfill"),
            new SingleRowChild(Long.MAX_VALUE, 7.0),
            0,
            startTime,
            Long.MAX_VALUE,
            Arrays.asList(TSDataType.TIMESTAMP, TSDataType.DOUBLE),
            STEP)) {

      for (int i = 0; i < 10 && !gapFillOperator.isFinished(); i++) {
        if (!gapFillOperator.hasNext()) {
          break;
        }
        TsBlock block = gapFillOperator.next();
        assertTrue(block != null && !block.isEmpty());
        for (int row = 0, size = block.getPositionCount(); row < size; row++) {
          actualTimes.add(block.getColumn(0).getLong(row));
        }
      }

      assertEquals(Arrays.asList(startTime, startTime + STEP, Long.MAX_VALUE), actualTimes);
      assertTrue(gapFillOperator.isFinished());
    }
  }
}
