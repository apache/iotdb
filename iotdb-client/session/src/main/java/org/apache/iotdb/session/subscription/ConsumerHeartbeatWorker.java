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

package org.apache.iotdb.session.subscription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerHeartbeatWorker implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerHeartbeatWorker.class);

  private final SubscriptionConsumer consumer;

  public ConsumerHeartbeatWorker(SubscriptionConsumer consumer) {
    this.consumer = consumer;
  }

  @Override
  public void run() {
    if (consumer.isClosed()) {
      return;
    }

    consumer.acquireWriteLock();
    try {
      heartbeatInternal();
    } finally {
      consumer.releaseWriteLock();
    }
  }

  private void heartbeatInternal() {
    for (final SubscriptionProvider provider : consumer.getAllProviders()) {
      try {
        provider.heartbeat();
        provider.setAvailable();
      } catch (final Exception e) {
        LOGGER.warn(
            "something unexpected happened when sending heartbeat to subscription provider {}, set subscription provider unavailable",
            provider,
            e);
        provider.setUnavailable();
      }
    }
  }
}
