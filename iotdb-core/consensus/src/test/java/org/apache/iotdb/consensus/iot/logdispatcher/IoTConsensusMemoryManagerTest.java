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

import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.iot.thrift.TLogEntry;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IoTConsensusMemoryManagerTest {

  @Test
  public void testAllocateQueue() {
    IoTConsensusMemoryManager memoryManager = IoTConsensusMemoryManager.getInstance();
    long maxMemory = memoryManager.getMaxMemorySizeForQueueInByte();

    long occupiedMemory = 0;
    IndexedConsensusRequest request;
    List<IndexedConsensusRequest> requestList = new ArrayList<>();
    while (occupiedMemory <= maxMemory) {
      request =
          new IndexedConsensusRequest(
              0,
              Collections.singletonList(
                  new ByteBufferConsensusRequest(ByteBuffer.wrap(new byte[4 * 1024 * 1024]))));
      request.buildSerializedRequests();
      long requestSize = request.getMemorySize();
      if (occupiedMemory + requestSize < maxMemory) {
        boolean reserved = memoryManager.reserve(request);
        assertTrue(reserved);
        occupiedMemory += requestSize;
        assertEquals(occupiedMemory, memoryManager.getQueueMemorySizeInByte());
        assertEquals(occupiedMemory, memoryManager.getMemorySizeInByte());
        requestList.add(request);
      } else {
        assertFalse(memoryManager.reserve(request));
        break;
      }
    }
    assertTrue(memoryManager.getMemorySizeInByte() <= maxMemory);

    for (IndexedConsensusRequest indexedConsensusRequest : requestList) {
      memoryManager.free(indexedConsensusRequest);
      occupiedMemory -= indexedConsensusRequest.getMemorySize();
      assertEquals(occupiedMemory, memoryManager.getMemorySizeInByte());
      assertEquals(occupiedMemory, memoryManager.getQueueMemorySizeInByte());
    }
  }

  @Test
  public void testAllocateBatch() {
    IoTConsensusMemoryManager memoryManager = IoTConsensusMemoryManager.getInstance();
    long maxMemory = memoryManager.getQueueMemorySizeInByte();

    long occupiedMemory = 0;

    Batch batch;
    int batchSize = 5;
    List<Batch> batchList = new ArrayList<>();
    while (occupiedMemory < maxMemory) {
      batch = new Batch(IoTConsensusConfig.newBuilder().build());
      for (int i = 0; i < batchSize; i++) {
        IndexedConsensusRequest request;
        request =
            new IndexedConsensusRequest(
                0,
                Collections.singletonList(
                    new ByteBufferConsensusRequest(ByteBuffer.wrap(new byte[1024 * 1024]))));
        batch.addTLogEntry(
            new TLogEntry(
                request.getSerializedRequests(),
                request.getSearchIndex(),
                false,
                request.getMemorySize()));
      }

      long requestSize = batch.getMemorySize();
      if (occupiedMemory + requestSize < maxMemory) {
        assertTrue(memoryManager.reserve(batch));
        occupiedMemory += requestSize;
        assertEquals(occupiedMemory, memoryManager.getMemorySizeInByte());
        batchList.add(batch);
      } else {
        assertFalse(memoryManager.reserve(batch));
      }
    }
    assertTrue(memoryManager.getMemorySizeInByte() <= maxMemory);

    for (Batch b : batchList) {
      memoryManager.free(b);
      occupiedMemory -= b.getMemorySize();
      assertEquals(occupiedMemory, memoryManager.getMemorySizeInByte());
    }
  }

  @Test
  public void testAllocateMixed() {
    IoTConsensusMemoryManager memoryManager = IoTConsensusMemoryManager.getInstance();
    long maxMemory = memoryManager.getMaxMemorySizeForQueueInByte();

    long occupiedMemory = 0;
    IndexedConsensusRequest request;
    List<IndexedConsensusRequest> requestList = new ArrayList<>();
    Batch batch;
    int batchSize = 5;
    List<Batch> batchList = new ArrayList<>();

    int i = 0;
    while (occupiedMemory <= maxMemory) {
      if (i % 2 == 0) {
        request =
            new IndexedConsensusRequest(
                0,
                Collections.singletonList(
                    new ByteBufferConsensusRequest(ByteBuffer.wrap(new byte[4 * 1024 * 1024]))));
        request.buildSerializedRequests();
        long requestSize = request.getMemorySize();
        if (occupiedMemory + requestSize < maxMemory) {
          boolean reserved = memoryManager.reserve(request);
          assertTrue(reserved);
          occupiedMemory += requestSize;
          assertEquals(occupiedMemory, memoryManager.getMemorySizeInByte());
          requestList.add(request);
        } else {
          assertFalse(memoryManager.reserve(request));
          break;
        }
      } else {
        batch = new Batch(IoTConsensusConfig.newBuilder().build());
        for (int j = 0; j < batchSize; j++) {
          IndexedConsensusRequest batchRequest;
          batchRequest =
              new IndexedConsensusRequest(
                  0,
                  Collections.singletonList(
                      new ByteBufferConsensusRequest(ByteBuffer.wrap(new byte[1024 * 1024]))));
          batch.addTLogEntry(
              new TLogEntry(
                  batchRequest.getSerializedRequests(),
                  batchRequest.getSearchIndex(),
                  false,
                  batchRequest.getMemorySize()));
        }

        long requestSize = batch.getMemorySize();
        if (occupiedMemory + requestSize < maxMemory) {
          assertTrue(memoryManager.reserve(batch));
          occupiedMemory += requestSize;
          assertEquals(occupiedMemory, memoryManager.getMemorySizeInByte());
          batchList.add(batch);
        } else {
          assertFalse(memoryManager.reserve(batch));
        }
      }
      i++;
    }
    assertTrue(memoryManager.getMemorySizeInByte() <= maxMemory);

    while (!requestList.isEmpty() || !batchList.isEmpty()) {
      if (!requestList.isEmpty()) {
        request = requestList.remove(0);
        memoryManager.free(request);
        occupiedMemory -= request.getMemorySize();
        assertEquals(occupiedMemory, memoryManager.getMemorySizeInByte());
        i--;
      }
      if (!batchList.isEmpty()) {
        batch = batchList.remove(0);
        memoryManager.free(batch);
        occupiedMemory -= batch.getMemorySize();
        assertEquals(occupiedMemory, memoryManager.getMemorySizeInByte());
        i--;
      }
    }
    assertEquals(0, i);
    assertEquals(0, memoryManager.getMemorySizeInByte());
    assertEquals(0, memoryManager.getQueueMemorySizeInByte());
  }
}
