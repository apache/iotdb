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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AccumulatorFactory;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.TableAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.rate.GroupedNaiveDeltaAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.rate.GroupedNaiveIncreaseAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.rate.GroupedNaiveIrateAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.rate.GroupedNaiveRateAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.rate.GroupedOrderedDeltaAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.rate.GroupedOrderedIncreaseAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.rate.GroupedOrderedIrateAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.rate.GroupedOrderedRateAccumulator;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class RateAccumulatorFactoryTest {

  private static final TAggregationType[] FUNCTION_TYPES = {
    TAggregationType.RATE, TAggregationType.INCREASE, TAggregationType.IRATE, TAggregationType.DELTA
  };

  private static final Class<?>[] ORDERED_CLASSES = {
    OrderedRateAccumulator.class,
    OrderedIncreaseAccumulator.class,
    OrderedIrateAccumulator.class,
    OrderedDeltaAccumulator.class
  };

  private static final Class<?>[] NAIVE_CLASSES = {
    NaiveRateAccumulator.class,
    NaiveIncreaseAccumulator.class,
    NaiveIrateAccumulator.class,
    NaiveDeltaAccumulator.class
  };

  private static final Class<?>[] GROUPED_ORDERED_CLASSES = {
    GroupedOrderedRateAccumulator.class,
    GroupedOrderedIncreaseAccumulator.class,
    GroupedOrderedIrateAccumulator.class,
    GroupedOrderedDeltaAccumulator.class
  };

  private static final Class<?>[] GROUPED_NAIVE_CLASSES = {
    GroupedNaiveRateAccumulator.class,
    GroupedNaiveIncreaseAccumulator.class,
    GroupedNaiveIrateAccumulator.class,
    GroupedNaiveDeltaAccumulator.class
  };

  @Test
  public void testOrderedImplementationRequiresSingleStepAndAscendingTimeInput() {
    MemoryReservationManager memoryManager = new NoOpMemoryReservationManager();
    for (int index = 0; index < FUNCTION_TYPES.length; index++) {
      TableAccumulator ordered =
          AccumulatorFactory.createBuiltinAccumulator(
              FUNCTION_TYPES[index],
              Collections.singletonList(TSDataType.DOUBLE),
              AggregationNode.Step.SINGLE,
              true,
              memoryManager);
      TableAccumulator unordered =
          AccumulatorFactory.createBuiltinAccumulator(
              FUNCTION_TYPES[index],
              Collections.singletonList(TSDataType.DOUBLE),
              AggregationNode.Step.SINGLE,
              false,
              memoryManager);
      TableAccumulator partial =
          AccumulatorFactory.createBuiltinAccumulator(
              FUNCTION_TYPES[index],
              Collections.singletonList(TSDataType.DOUBLE),
              AggregationNode.Step.PARTIAL,
              true,
              memoryManager);

      assertEquals(ORDERED_CLASSES[index], ordered.getClass());
      assertEquals(NAIVE_CLASSES[index], unordered.getClass());
      assertEquals(NAIVE_CLASSES[index], partial.getClass());
    }
  }

  @Test
  public void testGroupedOrderedImplementationRequiresSingleStepAndAscendingTimeInput() {
    MemoryReservationManager memoryManager = new NoOpMemoryReservationManager();
    for (int index = 0; index < FUNCTION_TYPES.length; index++) {
      GroupedAccumulator ordered =
          createGroupedAccumulator(
              FUNCTION_TYPES[index], AggregationNode.Step.SINGLE, true, memoryManager);
      GroupedAccumulator unordered =
          createGroupedAccumulator(
              FUNCTION_TYPES[index], AggregationNode.Step.SINGLE, false, memoryManager);
      GroupedAccumulator partial =
          createGroupedAccumulator(
              FUNCTION_TYPES[index], AggregationNode.Step.PARTIAL, true, memoryManager);

      assertEquals(GROUPED_ORDERED_CLASSES[index], ordered.getClass());
      assertEquals(GROUPED_NAIVE_CLASSES[index], unordered.getClass());
      assertEquals(GROUPED_NAIVE_CLASSES[index], partial.getClass());
    }
  }

  private static GroupedAccumulator createGroupedAccumulator(
      TAggregationType functionType,
      AggregationNode.Step step,
      boolean inputOrderedByTimeAscending,
      MemoryReservationManager memoryManager) {
    return AccumulatorFactory.createGroupedAccumulator(
        functionType.name().toLowerCase(),
        functionType,
        Collections.singletonList(TSDataType.DOUBLE),
        Collections.emptyList(),
        Collections.emptyMap(),
        true,
        false,
        step,
        inputOrderedByTimeAscending,
        memoryManager,
        null);
  }

  private static final class NoOpMemoryReservationManager implements MemoryReservationManager {

    @Override
    public void reserveMemoryCumulatively(long size) {}

    @Override
    public void reserveMemoryImmediately() {}

    @Override
    public void reserveMemoryImmediately(long size) {}

    @Override
    public void releaseMemoryCumulatively(long size) {}

    @Override
    public void releaseMemoryImmediately(long size) {}

    @Override
    public void releaseAllReservedMemory() {}

    @Override
    public Pair<Long, Long> releaseMemoryVirtually(long size) {
      return new Pair<>(0L, 0L);
    }

    @Override
    public void reserveMemoryVirtually(long bytesToBeReserved, long bytesAlreadyReserved) {}

    @Override
    public void setHighestPriority(boolean isHighestPriority) {}
  }
}
