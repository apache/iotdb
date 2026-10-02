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
    final ByteBufferConsensusRequest inner = new ByteBufferConsensusRequest(ByteBuffer.allocate(1));
    final IndexedConsensusRequest request = newRequest(inner);

    assertFalse(registry.offer(request));
    assertEquals(0, inner.getSerializationCount());
  }

  @Test
  public void testOfferSerializesRequestBeforeQueueAdmission() {
    final SubscriptionQueueRegistry registry = new SubscriptionQueueRegistry("test");
    final InspectingQueue queue = new InspectingQueue();
    register(registry, queue);
    final ByteBufferConsensusRequest inner = new ByteBufferConsensusRequest(ByteBuffer.allocate(1));
    final IndexedConsensusRequest request = newRequest(inner);

    assertTrue(registry.offer(request));
    assertEquals(1, inner.getSerializationCount());
    assertEquals(1, request.getSerializedRequests().size());
    assertEquals(1, queue.getSerializedRequestCountAtOffer());
    assertSame(request, queue.poll());
  }

  @Test
  public void testOfferDefersSerializationOfDeferredRequest() {
    final SubscriptionQueueRegistry registry = new SubscriptionQueueRegistry("test");
    final ArrayBlockingQueue<IndexedConsensusRequest> queue = new ArrayBlockingQueue<>(1);
    register(registry, queue);
    final ByteBufferConsensusRequest inner =
        new ByteBufferConsensusRequest(ByteBuffer.allocate(1), true);
    final IndexedConsensusRequest request = newRequest(inner);

    assertTrue(registry.offer(request));
    assertEquals(
        "a request that deferred its serialization must stay unexpanded while it waits in the queue",
        0,
        inner.getSerializationCount());
    assertEquals(1, request.getSerializedRequests().size());
    assertEquals(1, inner.getSerializationCount());
  }

  private static void register(
      final SubscriptionQueueRegistry registry,
      final ArrayBlockingQueue<IndexedConsensusRequest> queue) {
    registry.register(
        queue,
        new SubscriptionWalRetentionPolicy(
            "test",
            SubscriptionWalRetentionPolicy.UNBOUNDED,
            SubscriptionWalRetentionPolicy.UNBOUNDED));
  }

  private static IndexedConsensusRequest newRequest(final IConsensusRequest inner) {
    return new IndexedConsensusRequest(1, Collections.singletonList(inner));
  }

  private static final class ByteBufferConsensusRequest implements IConsensusRequest {

    private final ByteBuffer buffer;
    private final boolean deferred;
    private int serializationCount;

    private ByteBufferConsensusRequest(final ByteBuffer buffer) {
      this(buffer, false);
    }

    private ByteBufferConsensusRequest(final ByteBuffer buffer, final boolean deferred) {
      this.buffer = buffer;
      this.deferred = deferred;
    }

    @Override
    public ByteBuffer serializeToByteBuffer() {
      serializationCount++;
      return buffer;
    }

    @Override
    public long getMemorySize() {
      return buffer.capacity();
    }

    @Override
    public boolean isSerializationDeferred() {
      return deferred;
    }

    private int getSerializationCount() {
      return serializationCount;
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
