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
import org.apache.iotdb.rpc.subscription.payload.poll.ErrorPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.TerminationPayload;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The {@link SubscriptionEventSingleResponse} class represents a single subscription event response
 * that wraps a cached {@link SubscriptionPollResponse}. The actual payload of the response can be
 * either a {@link TerminationPayload} or an {@link ErrorPayload}.
 */
public class SubscriptionEventSingleResponse
    implements SubscriptionEventResponse<CachedSubscriptionPollResponse> {

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
  public CachedSubscriptionPollResponse getCurrentResponse() {
    return response;
  }

  @Override
  public void prefetchRemainingResponses() {
    // do nothing
  }

  @Override
  public void fetchNextResponse(final long offset) {
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
  public void invalidateCurrentResponseByteBuffer() {
    SubscriptionPollResponseCache.getInstance().invalidate(response);
  }

  @Override
  public void nack() {
    invalidateCurrentResponseByteBuffer();
  }

  @Override
  public void cleanUp() {
    invalidateCurrentResponseByteBuffer();
  }

  @Override
  public boolean isCommittable() {
    return true;
  }

  /////////////////////////////// stringify ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionEventSingleResponse" + coreReportMessage();
  }

  protected Map<String, String> coreReportMessage() {
    final Map<String, String> result = new HashMap<>();
    final CachedSubscriptionPollResponse currentResponse = getCurrentResponse();
    result.put(
        "currentResponse",
        Objects.nonNull(currentResponse) ? currentResponse.toString() : "<unknown>");
    return result;
  }
}
