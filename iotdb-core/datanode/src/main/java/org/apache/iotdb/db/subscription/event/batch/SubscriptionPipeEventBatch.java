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

package org.apache.iotdb.db.subscription.event.batch;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingQueue;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class SubscriptionPipeEventBatch {

  protected final SubscriptionPrefetchingQueue prefetchingQueue;
  protected final int maxDelayInMs;
  protected final long maxBatchSizeInBytes;

  protected List<SubscriptionEvent> events = null;

  protected SubscriptionPipeEventBatch(
      final SubscriptionPrefetchingQueue prefetchingQueue,
      final int maxDelayInMs,
      final long maxBatchSizeInBytes) {
    this.prefetchingQueue = prefetchingQueue;
    this.maxDelayInMs = maxDelayInMs;
    this.maxBatchSizeInBytes = maxBatchSizeInBytes;
  }

  public abstract List<SubscriptionEvent> onEvent() throws Exception;

  public abstract List<SubscriptionEvent> onEvent(final EnrichedEvent event) throws Exception;

  public abstract void cleanUp();

  public boolean isSealed() {
    return Objects.nonNull(events);
  }

  /////////////////////////////// stringify ///////////////////////////////

  protected Map<String, String> coreReportMessage() {
    final Map<String, String> result = new HashMap<>();
    result.put("prefetchingQueue", prefetchingQueue.coreReportMessage().toString());
    result.put("maxDelayInMs", String.valueOf(maxDelayInMs));
    result.put("maxBatchSizeInBytes", String.valueOf(maxBatchSizeInBytes));
    return result;
  }
}
