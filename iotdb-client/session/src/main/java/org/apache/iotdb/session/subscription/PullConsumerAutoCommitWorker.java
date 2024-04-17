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

import java.util.Map;
import java.util.Set;

public class PullConsumerAutoCommitWorker implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PullConsumerAutoCommitWorker.class);

  private final SubscriptionPullConsumer consumer;

  public PullConsumerAutoCommitWorker(SubscriptionPullConsumer consumer) {
    this.consumer = consumer;
  }

  @Override
  public void run() {
    if (consumer.isClosed()) {
      return;
    }

    long currentTimestamp = System.currentTimeMillis();
    long index = currentTimestamp / consumer.getAutoCommitIntervalMs();
    if (currentTimestamp % consumer.getAutoCommitIntervalMs() == 0) {
      index -= 1;
    }

    for (Map.Entry<Long, Set<SubscriptionMessage>> entry :
        consumer.getUncommittedMessages().headMap(index).entrySet()) {
      try {
        consumer.commitSync(entry.getValue());
        consumer.getUncommittedMessages().remove(entry.getKey());
      } catch (final Exception e) {
        LOGGER.warn("something unexpected happened when auto commit messages...", e);
      }
    }
  }
}
