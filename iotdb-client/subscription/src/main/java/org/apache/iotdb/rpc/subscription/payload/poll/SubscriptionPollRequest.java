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

package org.apache.iotdb.rpc.subscription.payload.poll;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SubscriptionPollRequest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPollRequest.class);

  private final transient short requestType;

  private final transient SubscriptionPollPayload payload;

  private final transient long timeoutMs;

  /** The maximum size, in bytes, for the response payload. */
  private final transient long maxBytes;

  /**
   * Per-topic writer-based progress used by the new consensus subscription model. This preserves
   * topic boundaries while allowing the consumer to provide a recovery hint on reconnect.
   */
  private final transient Map<String, TopicProgress> progressByTopic;

  public SubscriptionPollRequest(
      final short requestType,
      final SubscriptionPollPayload payload,
      final long timeoutMs,
      final long maxBytes) {
    this(requestType, payload, timeoutMs, maxBytes, Collections.emptyMap());
  }

  public SubscriptionPollRequest(
      final short requestType,
      final SubscriptionPollPayload payload,
      final long timeoutMs,
      final long maxBytes,
      final Map<String, TopicProgress> progressByTopic) {
    this.requestType = requestType;
    this.payload = payload;
    this.timeoutMs = timeoutMs;
    this.maxBytes = maxBytes;
    this.progressByTopic = progressByTopic != null ? progressByTopic : Collections.emptyMap();
  }

  public short getRequestType() {
    return requestType;
  }

  public SubscriptionPollPayload getPayload() {
    return payload;
  }

  public long getTimeoutMs() {
    return timeoutMs;
  }

  public long getMaxBytes() {
    return maxBytes;
  }

  public Map<String, TopicProgress> getProgressByTopic() {
    return progressByTopic;
  }

  //////////////////////////// serialization ////////////////////////////

  public static ByteBuffer serialize(final SubscriptionPollRequest request) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      request.serialize(outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(requestType, stream);
    payload.serialize(stream);
    ReadWriteIOUtils.write(timeoutMs, stream);
    ReadWriteIOUtils.write(maxBytes, stream);
    ReadWriteIOUtils.write(progressByTopic.size(), stream);
    for (final Map.Entry<String, TopicProgress> entry : progressByTopic.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      entry.getValue().serialize(stream);
    }
  }

  public static SubscriptionPollRequest deserialize(final ByteBuffer buffer) {
    final short requestType = ReadWriteIOUtils.readShort(buffer);
    SubscriptionPollPayload payload = null;
    if (SubscriptionPollRequestType.isValidatedRequestType(requestType)) {
      switch (SubscriptionPollRequestType.valueOf(requestType)) {
        case POLL:
          payload = new PollPayload().deserialize(buffer);
          break;
        case POLL_FILE:
          payload = new PollFilePayload().deserialize(buffer);
          break;
        case POLL_TABLETS:
          payload = new PollTabletsPayload().deserialize(buffer);
          break;
        default:
          LOGGER.warn("unexpected request type: {}, payload will be null", requestType);
          break;
      }
    } else {
      LOGGER.warn("unexpected request type: {}, payload will be null", requestType);
    }

    final long timeoutMs = ReadWriteIOUtils.readLong(buffer);
    final long maxBytes = ReadWriteIOUtils.readLong(buffer);

    Map<String, TopicProgress> progressByTopic = Collections.emptyMap();
    if (buffer.hasRemaining()) {
      final int mapSize = ReadWriteIOUtils.readInt(buffer);
      if (mapSize > 0) {
        progressByTopic = new HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
          progressByTopic.put(
              ReadWriteIOUtils.readString(buffer), TopicProgress.deserialize(buffer));
        }
      }
    }

    return new SubscriptionPollRequest(requestType, payload, timeoutMs, maxBytes, progressByTopic);
  }

  /////////////////////////////// object ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionPollRequest{requestType="
        + SubscriptionPollRequestType.valueOf(requestType)
        + ", payload="
        + payload
        + ", timeoutMs="
        + timeoutMs
        + ", maxBytes="
        + maxBytes
        + ", progressByTopic.size="
        + progressByTopic.size()
        + "}";
  }
}
