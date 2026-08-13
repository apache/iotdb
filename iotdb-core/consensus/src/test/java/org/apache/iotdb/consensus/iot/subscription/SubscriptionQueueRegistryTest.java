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

package org.apache.iotdb.consensus.iot.subscription;

import org.apache.iotdb.commons.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SubscriptionQueueRegistryTest {

  @Test
  public void testOfferWithoutQueuesDoesNotSerializeRequest() {
    final SubscriptionQueueRegistry registry = new SubscriptionQueueRegistry("test");
    final IndexedConsensusRequest request = newRequest();

    assertFalse(registry.offer(request));
    assertTrue(request.getSerializedRequests().isEmpty());
  }

  @Test
  public void testOfferSerializesRequestBeforeQueueAdmission() {
    final SubscriptionQueueRegistry registry = new SubscriptionQueueRegistry("test");
    final InspectingQueue queue = new InspectingQueue();
    registry.register(
        queue,
        new SubscriptionWalRetentionPolicy(
            "test",
            SubscriptionWalRetentionPolicy.UNBOUNDED,
            SubscriptionWalRetentionPolicy.UNBOUNDED));
    final IndexedConsensusRequest request = newRequest();

    assertTrue(registry.offer(request));
    assertEquals(1, request.getSerializedRequests().size());
    assertEquals(1, queue.getSerializedRequestCountAtOffer());
    assertSame(request, queue.poll());
  }

  private static IndexedConsensusRequest newRequest() {
    return new IndexedConsensusRequest(
        1, Collections.singletonList(new ByteBufferConsensusRequest(ByteBuffer.allocate(1))));
  }

  private static final class ByteBufferConsensusRequest implements IConsensusRequest {

    private final ByteBuffer buffer;

    private ByteBufferConsensusRequest(final ByteBuffer buffer) {
      this.buffer = buffer;
    }

    @Override
    public ByteBuffer serializeToByteBuffer() {
      return buffer;
    }

    @Override
    public long getMemorySize() {
      return buffer.capacity();
    }
  }

  private static final class InspectingQueue extends ArrayBlockingQueue<IndexedConsensusRequest> {

    private int serializedRequestCountAtOffer = -1;

    private InspectingQueue() {
      super(1);
    }

    @Override
    public boolean offer(final IndexedConsensusRequest request) {
      serializedRequestCountAtOffer = request.getSerializedRequests().size();
      return super.offer(request);
    }

    private int getSerializedRequestCountAtOffer() {
      return serializedRequestCountAtOffer;
    }
  }
}
