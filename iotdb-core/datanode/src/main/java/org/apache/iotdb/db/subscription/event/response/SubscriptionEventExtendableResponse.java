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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * The {@code SubscriptionEventExtendableResponse} class represents a subscription event response
 * that can dynamically change as new responses are fetched. It maintains a list of {@link
 * CachedSubscriptionPollResponse} objects and provides methods for managing and serializing these
 * responses.
 */
public abstract class SubscriptionEventExtendableResponse
    implements SubscriptionEventResponse<CachedSubscriptionPollResponse> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionEventTabletResponse.class);

  private final Deque<CachedSubscriptionPollResponse> responses;
  protected volatile boolean hasNoMore = false;

  protected SubscriptionEventExtendableResponse() {
    this.responses = new ConcurrentLinkedDeque<>();
  }

  @Override
  public CachedSubscriptionPollResponse getCurrentResponse() {
    return peekFirst();
  }

  @Override
  public void trySerializeCurrentResponse() {
    SubscriptionPollResponseCache.getInstance().trySerialize(getCurrentResponse());
  }

  @Override
  public void trySerializeRemainingResponses() {
    responses.stream()
        .skip(1)
        .filter(response -> Objects.isNull(response.getByteBuffer()))
        .findFirst()
        .ifPresent(response -> SubscriptionPollResponseCache.getInstance().trySerialize(response));
  }

  @Override
  public ByteBuffer getCurrentResponseByteBuffer() throws IOException {
    return SubscriptionPollResponseCache.getInstance().serialize(getCurrentResponse());
  }

  @Override
  public void invalidateCurrentResponseByteBuffer() {
    SubscriptionPollResponseCache.getInstance().invalidate(getCurrentResponse());
  }

  @Override
  public void cleanUp() {
    CachedSubscriptionPollResponse response;
    while (Objects.nonNull(response = poll())) {
      SubscriptionPollResponseCache.getInstance().invalidate(response);
    }

    hasNoMore = false;
  }

  @Override
  public boolean isCommittable() {
    return hasNoMore && size() == 1;
  }

  /////////////////////////////// utility ///////////////////////////////

  protected void offer(final CachedSubscriptionPollResponse response) {
    responses.addLast(response);
  }

  protected CachedSubscriptionPollResponse poll() {
    return responses.isEmpty() ? null : responses.removeFirst();
  }

  protected CachedSubscriptionPollResponse peekFirst() {
    return responses.isEmpty() ? null : responses.getFirst();
  }

  protected CachedSubscriptionPollResponse peekLast() {
    return responses.isEmpty() ? null : responses.getLast();
  }

  protected int size() {
    return responses.size();
  }

  protected boolean isEmpty() {
    return responses.isEmpty();
  }

  /////////////////////////////// stringify ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionEventExtendableResponse" + coreReportMessage();
  }

  protected Map<String, String> coreReportMessage() {
    final Map<String, String> result = new HashMap<>();
    final CachedSubscriptionPollResponse currentResponse = getCurrentResponse();
    result.put(
        "currentResponse",
        Objects.nonNull(currentResponse) ? currentResponse.toString() : "<unknown>");
    result.put("hasNoMore", String.valueOf(hasNoMore));
    return result;
  }
}
