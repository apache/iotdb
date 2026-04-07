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

package org.apache.iotdb.session.subscription.consumer.base;

import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;

import java.util.List;

/**
 * A processor that transforms, filters, or enriches subscription messages in the pull consumer
 * pipeline. Processors are chained and invoked on each poll() call.
 *
 * <p>Processors may buffer messages internally (e.g., for watermark-based ordering) and return them
 * in later process() calls. Buffered messages should be released via {@link #flush()} when the
 * consumer closes.
 */
public interface SubscriptionMessageProcessor {

  /**
   * Process a batch of messages. May return fewer, more, or different messages than the input.
   *
   * @param messages the messages from the previous stage (or raw poll)
   * @return messages to pass to the next stage (or to the user)
   */
  List<SubscriptionMessage> process(List<SubscriptionMessage> messages);

  /**
   * Flush all internally buffered messages. Called when the consumer is closing.
   *
   * @return any remaining buffered messages
   */
  List<SubscriptionMessage> flush();

  /** Returns the number of messages currently buffered by this processor. */
  default int getBufferedCount() {
    return 0;
  }
}
