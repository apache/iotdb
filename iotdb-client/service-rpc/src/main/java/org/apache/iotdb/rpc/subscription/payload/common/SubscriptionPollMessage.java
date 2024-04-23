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

package org.apache.iotdb.rpc.subscription.payload.common;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SubscriptionPollMessage {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPolledMessage.class);

  private final transient short messageType;

  private final transient SubscriptionMessagePayload messagePayload;

  private final transient long timeoutMs; // unused now

  public SubscriptionPollMessage(
      short messageType, SubscriptionMessagePayload messagePayload, long timeoutMs) {
    this.messageType = messageType;
    this.messagePayload = messagePayload;
    this.timeoutMs = timeoutMs;
  }

  public short getMessageType() {
    return messageType;
  }

  public SubscriptionMessagePayload getMessagePayload() {
    return messagePayload;
  }

  public long getTimeoutMs() {
    return timeoutMs;
  }

  //////////////////////////// serialization ////////////////////////////

  public static ByteBuffer serialize(SubscriptionPollMessage message) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      message.serialize(outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(messageType, stream);
    messagePayload.serialize(stream);
    ReadWriteIOUtils.write(timeoutMs, stream);
  }

  public static SubscriptionPollMessage deserialize(final ByteBuffer buffer) {
    final short messageType = ReadWriteIOUtils.readShort(buffer);
    SubscriptionMessagePayload messagePayload = null;
    if (SubscriptionPollMessageType.isValidatedMessageType(messageType)) {
      switch (SubscriptionPollMessageType.valueOf(messageType)) {
        case POLL:
          messagePayload = new PollMessagePayload().deserialize(buffer);
          break;
        case POLL_TS_FILE:
          messagePayload = new PollTsFileMessagePayload().deserialize(buffer);
          break;
        default:
          LOGGER.warn("unexpected message type: {}, message payload will be null", messageType);
          break;
      }
    } else {
      LOGGER.warn("unexpected message type: {}, message payload will be null", messageType);
    }

    final long timeoutMs = ReadWriteIOUtils.readLong(buffer);
    return new SubscriptionPollMessage(messageType, messagePayload, timeoutMs);
  }

  /////////////////////////////// object ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionPollMessage{messageType="
        + SubscriptionPollMessageType.valueOf(messageType).toString()
        + ", messagePayload="
        + messagePayload
        + ", timeoutMs="
        + timeoutMs
        + "}";
  }
}
