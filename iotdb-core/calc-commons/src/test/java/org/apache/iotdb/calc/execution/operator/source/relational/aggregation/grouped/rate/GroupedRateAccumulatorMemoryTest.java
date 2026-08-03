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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.rate;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedAccumulator;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GroupedRateAccumulatorMemoryTest {

  @Test
  public void testGroupedStateIsNotReservedByAccumulator() {
    TrackingMemoryReservationManager memoryManager = new TrackingMemoryReservationManager();
    GroupedAccumulator[] accumulators = {
      new GroupedNaiveRateAccumulator(TSDataType.DOUBLE, memoryManager),
      new GroupedNaiveIncreaseAccumulator(TSDataType.DOUBLE, memoryManager),
      new GroupedNaiveIrateAccumulator(TSDataType.DOUBLE, memoryManager),
      new GroupedNaiveDeltaAccumulator(TSDataType.DOUBLE, memoryManager),
      new GroupedOrderedRateAccumulator(TSDataType.DOUBLE),
      new GroupedOrderedIncreaseAccumulator(TSDataType.DOUBLE),
      new GroupedOrderedIrateAccumulator(TSDataType.DOUBLE),
      new GroupedOrderedDeltaAccumulator(TSDataType.DOUBLE)
    };

    for (GroupedAccumulator accumulator : accumulators) {
      accumulator.setGroupCount(32);
      accumulator.reset();
    }

    assertEquals(0, memoryManager.cumulativeReservation);
    assertEquals(0, memoryManager.cumulativeRelease);
  }

  private static final class TrackingMemoryReservationManager implements MemoryReservationManager {

    private long cumulativeReservation;
    private long cumulativeRelease;

    @Override
    public void reserveMemoryCumulatively(long size) {
      cumulativeReservation += size;
    }

    @Override
    public void reserveMemoryImmediately() {}

    @Override
    public void reserveMemoryImmediately(long size) {}

    @Override
    public void releaseMemoryCumulatively(long size) {
      cumulativeRelease += size;
    }

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
