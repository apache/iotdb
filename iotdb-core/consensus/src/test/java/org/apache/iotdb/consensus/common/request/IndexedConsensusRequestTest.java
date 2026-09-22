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

package org.apache.iotdb.consensus.common.request;

import org.apache.iotdb.commons.request.IConsensusRequest;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

public class IndexedConsensusRequestTest {

  @Test
  public void testDeferredRequestIsMaterializedOnlyWhenItIsNeeded() {
    final CountingRequest deferred = new CountingRequest(true);
    final IndexedConsensusRequest request =
        new IndexedConsensusRequest(1L, 1L, Collections.singletonList(deferred));

    Assert.assertTrue(request.hasDeferredRequests());
    Assert.assertEquals(
        "a deferred request must keep its source object, not its bytes, while it is queued",
        0,
        deferred.serializationCount);
    Assert.assertEquals(64L, request.getRetainedMemorySize());

    // The bytes are materialized when a batch is built for sending
    Assert.assertFalse(request.getSerializedRequests().isEmpty());
    Assert.assertEquals(1, deferred.serializationCount);
    Assert.assertEquals(64L, request.getMemorySize());
    Assert.assertEquals(96L, request.getRetainedMemorySize());

    // and are reused afterwards instead of being rebuilt for every retry
    Assert.assertFalse(request.getSerializedRequests().isEmpty());
    Assert.assertEquals(1, deferred.serializationCount);
  }

  @Test
  public void testQueueReservationSurvivesDeferredSerialization() {
    final CountingRequest deferred = new CountingRequest(true);
    final IndexedConsensusRequest request =
        new IndexedConsensusRequest(1L, 1L, Collections.singletonList(deferred));

    // The queue reserves the reference form
    Assert.assertEquals(64L, request.getQueueReservedMemorySize());

    // Materializing the payloads must not change what a release returns
    Assert.assertFalse(request.getSerializedRequests().isEmpty());
    Assert.assertEquals(96L, request.getRetainedMemorySize());
    Assert.assertEquals(64L, request.getQueueReservedMemorySize());
  }

  @Test
  public void testRequestThatSerializesEagerlyIsNotTreatedAsDeferred() {
    final CountingRequest eager = new CountingRequest(false);
    final IndexedConsensusRequest request =
        new IndexedConsensusRequest(1L, 1L, Collections.singletonList(eager));

    Assert.assertFalse(request.hasDeferredRequests());
    request.buildSerializedRequests();
    Assert.assertEquals(1, eager.serializationCount);
    Assert.assertEquals(64L, request.getMemorySize());
  }

  private static class CountingRequest implements IConsensusRequest {

    private final boolean deferred;
    private int serializationCount;

    private CountingRequest(final boolean deferred) {
      this.deferred = deferred;
    }

    @Override
    public ByteBuffer serializeToByteBuffer() {
      serializationCount++;
      return ByteBuffer.allocate(32);
    }

    @Override
    public long getMemorySize() {
      return 64L;
    }

    @Override
    public boolean isSerializationDeferred() {
      return deferred;
    }
  }
}
