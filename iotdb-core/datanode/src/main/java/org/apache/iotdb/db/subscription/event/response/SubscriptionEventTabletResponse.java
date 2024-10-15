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

import org.apache.iotdb.db.subscription.event.batch.SubscriptionPipeTabletEventBatch;
import org.apache.iotdb.db.subscription.event.cache.CachedSubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SubscriptionEventTabletResponse
    extends SubscriptionEventExtendableResponse<CachedSubscriptionPollResponse> {

  public SubscriptionEventTabletResponse(
      final SubscriptionPipeTabletEventBatch batch,
      final SubscriptionCommitContext commitContext) {}

  @Override
  public CachedSubscriptionPollResponse getCurrentResponse() {
    return null;
  }

  @Override
  public void prefetchRemainingResponses() throws IOException {}

  @Override
  public void fetchNextResponse() throws IOException {}

  @Override
  public void trySerializeCurrentResponse() {}

  @Override
  public void trySerializeRemainingResponses() {}

  @Override
  public ByteBuffer getCurrentResponseByteBuffer() throws IOException {
    return null;
  }

  @Override
  public void resetCurrentResponseByteBuffer() {}

  @Override
  public void reset() {}

  @Override
  public void cleanUp() {}

  @Override
  public boolean isCommittable() {
    return false;
  }
}
