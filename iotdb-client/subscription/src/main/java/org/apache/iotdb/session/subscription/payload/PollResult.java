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

package org.apache.iotdb.session.subscription.payload;

import java.util.Collections;
import java.util.List;

/** Result of a poll operation that includes processor metadata alongside the messages. */
public class PollResult {

  private final List<SubscriptionMessage> messages;
  private final int bufferedCount;
  private final long watermark;

  public PollResult(
      final List<SubscriptionMessage> messages, final int bufferedCount, final long watermark) {
    this.messages = messages != null ? messages : Collections.emptyList();
    this.bufferedCount = bufferedCount;
    this.watermark = watermark;
  }

  /** Returns the processed messages ready for consumption. */
  public List<SubscriptionMessage> getMessages() {
    return messages;
  }

  /** Returns the total number of messages currently buffered across all processors. */
  public int getBufferedCount() {
    return bufferedCount;
  }

  /**
   * Returns the current watermark timestamp (-1 if no watermark processor is configured). Messages
   * with timestamps at or before this value have all been emitted.
   */
  public long getWatermark() {
    return watermark;
  }

  @Override
  public String toString() {
    return "PollResult{messages="
        + messages.size()
        + ", bufferedCount="
        + bufferedCount
        + ", watermark="
        + watermark
        + "}";
  }
}
