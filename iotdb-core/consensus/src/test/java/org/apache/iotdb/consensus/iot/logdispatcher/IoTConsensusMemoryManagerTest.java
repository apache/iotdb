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
  private long memoryBlockSize = 16 * 1024L;

  @Before
  public void setUp() throws Exception {
    previousMemoryBlock = IoTConsensusMemoryManager.getInstance().getMemoryBlock();
    IoTConsensusMemoryManager.getInstance()
        .setMemoryBlock(new AtomicLongMemoryBlock("Test", null, memoryBlockSize));
  }

  @After
  public void tearDown() throws Exception {
    IoTConsensusMemoryManager.getInstance().setMemoryBlock(previousMemoryBlock);
  }

  @Test
  public void testSingleReserveAndRelease() {
    testReserveAndRelease(1);
  }

  @Test
  public void testMultiReserveAndRelease() {
    testReserveAndRelease(3);
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
}
