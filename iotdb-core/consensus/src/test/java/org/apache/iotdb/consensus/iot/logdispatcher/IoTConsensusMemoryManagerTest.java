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

package org.apache.iotdb.consensus.iot.logdispatcher;

import org.apache.iotdb.commons.memory.AtomicLongMemoryBlock;
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IoTConsensusMemoryManagerTest {

  private IMemoryBlock previousMemoryBlock;
  private double previousMaxMemoryRatioForQueue;
  private long memoryBlockSize = 16 * 1024L;

  @Before
  public void setUp() throws Exception {
    previousMemoryBlock = IoTConsensusMemoryManager.getInstance().getMemoryBlock();
    previousMaxMemoryRatioForQueue =
        IoTConsensusMemoryManager.getInstance().getMaxMemoryRatioForQueue();
    IoTConsensusMemoryManager.getInstance()
        .setMemoryBlock(new AtomicLongMemoryBlock("Test", null, memoryBlockSize));
    IoTConsensusMemoryManager.getInstance().reset();
  }

  @After
  public void tearDown() throws Exception {
    IoTConsensusMemoryManager.getInstance().reset();
    IoTConsensusMemoryManager.getInstance().setMemoryBlock(previousMemoryBlock);
    IoTConsensusMemoryManager.getInstance()
        .updateMaxMemoryRatioForQueue(previousMaxMemoryRatioForQueue);
  }

  @Test
  public void testSingleReserveAndRelease() {
    testReserveAndRelease(1);
  }

  @Test
  public void testMultiReserveAndRelease() {
    testReserveAndRelease(3);
  }

  @Test
  public void testRawAndSerializedMemoryAreBothReservedOnce() {
    final IndexedConsensusRequest request =
        new IndexedConsensusRequest(
            1, Collections.singletonList(new SizedConsensusRequest(10, 20)));

    assertEquals(10L, request.getRetainedMemorySize());
    request.buildSerializedRequests();
    request.buildSerializedRequests();
    assertEquals(20L, request.getMemorySize());
    assertEquals(30L, request.getRetainedMemorySize());
    assertEquals(1, request.getSerializedRequests().size());
    request.clearRequests();
    assertTrue(request.getRequests().isEmpty());
    assertEquals(20L, request.getRetainedMemorySize());

    assertTrue(IoTConsensusMemoryManager.getInstance().reserve(request));
    assertTrue(IoTConsensusMemoryManager.getInstance().reserve(request));
    assertEquals(
        20L, IoTConsensusMemoryManager.getInstance().getMemoryBlock().getUsedMemoryInBytes());

    IoTConsensusMemoryManager.getInstance().free(request);
    assertEquals(
        20L, IoTConsensusMemoryManager.getInstance().getMemoryBlock().getUsedMemoryInBytes());
    IoTConsensusMemoryManager.getInstance().free(request);
    assertEquals(
        0L, IoTConsensusMemoryManager.getInstance().getMemoryBlock().getUsedMemoryInBytes());
  }

  @Test
  public void testUnserializedRequestReservesRawMemory() {
    final IndexedConsensusRequest request =
        new IndexedConsensusRequest(
            1, Collections.singletonList(new SizedConsensusRequest(10, 20)));

    assertTrue(IoTConsensusMemoryManager.getInstance().reserve(request));
    assertEquals(
        10L, IoTConsensusMemoryManager.getInstance().getMemoryBlock().getUsedMemoryInBytes());
    IoTConsensusMemoryManager.getInstance().free(request);
    assertEquals(
        0L, IoTConsensusMemoryManager.getInstance().getMemoryBlock().getUsedMemoryInBytes());
  }

  @Test
  public void testClearUnserializedRequest() {
    final IndexedConsensusRequest request =
        new IndexedConsensusRequest(
            1, Collections.singletonList(new SizedConsensusRequest(10, 20)));

    request.clearRequests();
    assertTrue(request.getRequests().isEmpty());
    assertEquals(0L, request.getRetainedMemorySize());
  }

  @Test
  public void testUpdateMaxMemoryRatioForQueue() {
    final IndexedConsensusRequest request =
        new IndexedConsensusRequest(
            1,
            Collections.singletonList(
                new ByteBufferConsensusRequest(ByteBuffer.allocate((int) (memoryBlockSize / 3)))));
    request.buildSerializedRequests();

    IoTConsensusMemoryManager.getInstance().updateMaxMemoryRatioForQueue(0.25);
    assertFalse(IoTConsensusMemoryManager.getInstance().reserve(request));

    IoTConsensusMemoryManager.getInstance().updateMaxMemoryRatioForQueue(0.5);
    assertTrue(IoTConsensusMemoryManager.getInstance().reserve(request));
    IoTConsensusMemoryManager.getInstance().free(request);
  }

  private void testReserveAndRelease(int numReservation) {
    int allocationSize = 1;
    long allocatedSize = 0;
    List<IndexedConsensusRequest> requestList = new ArrayList<>();
    while (true) {
      IndexedConsensusRequest request =
          new IndexedConsensusRequest(
              0,
              Collections.singletonList(
                  new ByteBufferConsensusRequest(ByteBuffer.allocate(allocationSize))));
      request.buildSerializedRequests();
      if (allocatedSize + allocationSize
          <= memoryBlockSize
              * IoTConsensusMemoryManager.getInstance().getMaxMemoryRatioForQueue()) {
        for (int i = 0; i < numReservation; i++) {
          assertTrue(IoTConsensusMemoryManager.getInstance().reserve(request));
          requestList.add(request);
        }
      } else {
        for (int i = 0; i < numReservation; i++) {
          assertFalse(IoTConsensusMemoryManager.getInstance().reserve(request));
        }
        break;
      }
      allocatedSize += allocationSize;
    }

    assertTrue(
        IoTConsensusMemoryManager.getInstance().getMemorySizeInByte()
            <= memoryBlockSize
                * IoTConsensusMemoryManager.getInstance().getMaxMemoryRatioForQueue());
    for (IndexedConsensusRequest indexedConsensusRequest : requestList) {
      IoTConsensusMemoryManager.getInstance().free(indexedConsensusRequest);
    }
    assertEquals(0, IoTConsensusMemoryManager.getInstance().getMemorySizeInByte());
  }

  private static final class SizedConsensusRequest implements IConsensusRequest {

    private final long rawMemorySize;
    private final int serializedMemorySize;

    private SizedConsensusRequest(final long rawMemorySize, final int serializedMemorySize) {
      this.rawMemorySize = rawMemorySize;
      this.serializedMemorySize = serializedMemorySize;
    }

    @Override
    public ByteBuffer serializeToByteBuffer() {
      return ByteBuffer.allocate(serializedMemorySize);
    }

    @Override
    public long getMemorySize() {
      return rawMemorySize;
    }
  }
}
