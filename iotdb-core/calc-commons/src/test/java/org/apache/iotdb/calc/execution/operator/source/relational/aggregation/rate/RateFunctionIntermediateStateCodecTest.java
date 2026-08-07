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

import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.exception.SemanticException;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RateFunctionIntermediateStateCodecTest {

  @Test
  public void testRejectsIntermediateStateLargerThanMaximumTsBlockSize() {
    int previousMaxSize = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    try {
      TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(40);
      TimeValueBuffer samples = new TimeValueBuffer();
      samples.add(1, 1.0);
      samples.add(2, 2.0);
      TrackingMemoryReservationManager memoryManager = new TrackingMemoryReservationManager();

      assertThrows(
          SemanticException.class,
          () ->
              RateFunctionIntermediateStateCodec.encode(
                  RateFunctionType.RATE,
                  0,
                  3,
                  samples,
                  new BinaryColumnBuilder(null, 1),
                  memoryManager));
      assertEquals(0, memoryManager.totalImmediateReservation);
    } finally {
      TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(previousMaxSize);
    }
  }

  @Test
  public void testRoundTripReservesEncodingAndDecodingMemory() {
    TimeValueBuffer samples = new TimeValueBuffer();
    samples.add(1, 10.0);
    samples.add(2, 20.0);
    TrackingMemoryReservationManager memoryManager = new TrackingMemoryReservationManager();
    BinaryColumnBuilder output = new BinaryColumnBuilder(null, 1);

    RateFunctionIntermediateStateCodec.encode(
        RateFunctionType.RATE, 0, 3, samples, output, memoryManager);

    assertEquals(56, memoryManager.totalImmediateReservation);
    assertEquals(0, memoryManager.outstandingReservation);
    Column encoded = output.build();
    memoryManager.reset();

    try (RateFunctionIntermediateStateCodec.DecodedState decoded =
        RateFunctionIntermediateStateCodec.decode(
            RateFunctionType.RATE, encoded.getBinary(0), memoryManager)) {
      assertEquals(0, decoded.getWindowStart());
      assertEquals(3, decoded.getWindowEnd());
      assertEquals(2, decoded.getSamples().size());
      assertEquals(1, decoded.getSamples().getTime(0));
      assertEquals(10.0, decoded.getSamples().getValue(0), 0.0);
      assertTrue(memoryManager.outstandingReservation > 0);
    }
    assertEquals(0, memoryManager.outstandingReservation);
  }

  @Test
  public void testInvalidStateDoesNotReserveDecodeMemory() {
    TrackingMemoryReservationManager memoryManager = new TrackingMemoryReservationManager();

    assertThrows(
        SemanticException.class,
        () ->
            RateFunctionIntermediateStateCodec.decode(
                RateFunctionType.RATE, new Binary(new byte[7]), memoryManager));

    assertEquals(0, memoryManager.totalImmediateReservation);
    assertEquals(0, memoryManager.outstandingReservation);
  }

  private static final class TrackingMemoryReservationManager implements MemoryReservationManager {

    private long totalImmediateReservation;
    private long outstandingReservation;

    @Override
    public void reserveMemoryCumulatively(long size) {
      outstandingReservation += size;
    }

    @Override
    public void reserveMemoryImmediately() {}

    @Override
    public void reserveMemoryImmediately(long size) {
      totalImmediateReservation += size;
      outstandingReservation += size;
    }

    @Override
    public void releaseMemoryCumulatively(long size) {
      outstandingReservation -= size;
    }

    @Override
    public void releaseAllReservedMemory() {
      outstandingReservation = 0;
    }

    @Override
    public Pair<Long, Long> releaseMemoryVirtually(long size) {
      return new Pair<>(0L, 0L);
    }

    @Override
    public void reserveMemoryVirtually(long bytesToBeReserved, long bytesAlreadyReserved) {}

    @Override
    public void setHighestPriority(boolean isHighestPriority) {}

    private void reset() {
      totalImmediateReservation = 0;
      outstandingReservation = 0;
    }
  }
}
