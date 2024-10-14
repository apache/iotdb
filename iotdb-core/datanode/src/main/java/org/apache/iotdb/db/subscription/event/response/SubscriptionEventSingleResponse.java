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

package org.apache.iotdb.db.subscription.event.response;

import org.apache.iotdb.db.subscription.event.cache.CachedSubscriptionPollResponse;
import org.apache.iotdb.db.subscription.event.cache.SubscriptionPollResponseCache;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SubscriptionEventSingleResponse implements SubscriptionEventResponse {

  private final CachedSubscriptionPollResponse response;

  public SubscriptionEventSingleResponse(
      final short responseType,
      final SubscriptionPollPayload payload,
      final SubscriptionCommitContext commitContext) {
    this.response = new CachedSubscriptionPollResponse(responseType, payload, commitContext);
  }

  public SubscriptionEventSingleResponse(final SubscriptionPollResponse response) {
    this.response = new CachedSubscriptionPollResponse(response);
  }

  @Override
  public SubscriptionPollResponse getCurrentResponse() {
    return response;
  }

  @Override
  public void prefetchRemainingResponses() {
    // do nothing
  }

  @Override
  public void fetchNextResponse() {
    // do nothing
  }

  @Override
  public void trySerializeCurrentResponse() {
    SubscriptionPollResponseCache.getInstance().trySerialize(response);
  }

  @Override
  public void trySerializeRemainingResponses() {
    // do nothing
  }

  @Override
  public ByteBuffer getCurrentResponseByteBuffer() throws IOException {
    return SubscriptionPollResponseCache.getInstance().serialize(response);
  }

  @Override
  public void resetCurrentResponseByteBuffer() {
    SubscriptionPollResponseCache.getInstance().invalidate(response);
  }

  @Override
  public void reset() {
    resetCurrentResponseByteBuffer();
  }

  @Override
  public void cleanUp() {
    resetCurrentResponseByteBuffer();
  }

  @Override
  public boolean isCommittable() {
    return true;
  }
}
