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
import java.util.Objects;

public class SubscriptionPolledMessage {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPolledMessage.class);

  private final transient short messageType;

  private final transient SubscriptionMessagePayload messagePayload;

  private final transient SubscriptionCommitContext commitContext;

  private ByteBuffer byteBuffer;

  public SubscriptionPolledMessage(
      short messageType,
      SubscriptionMessagePayload messagePayload,
      SubscriptionCommitContext commitContext) {
    this.messageType = messageType;
    this.messagePayload = messagePayload;
    this.commitContext = commitContext;
  }

  public short getMessageType() {
    return messageType;
  }

  public SubscriptionMessagePayload getMessagePayload() {
    return messagePayload;
  }

  public SubscriptionCommitContext getCommitContext() {
    return commitContext;
  }

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  /////////////////////////////// de/ser ///////////////////////////////

  public static ByteBuffer serialize(SubscriptionPolledMessage message) throws IOException {
    if (Objects.nonNull(message.byteBuffer)) {
      return message.byteBuffer;
    }
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      message.serialize(outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(messageType, stream);
    messagePayload.serialize(stream);
    commitContext.serialize(stream);
  }

  public static SubscriptionPolledMessage deserialize(final ByteBuffer buffer) {
    final short messageType = ReadWriteIOUtils.readShort(buffer);
    SubscriptionMessagePayload messagePayload = null;
    if (SubscriptionPolledMessageType.isValidatedMessageType(messageType)) {
      switch (SubscriptionPolledMessageType.valueOf(messageType)) {
        case TABLETS:
          messagePayload = new TabletsMessagePayload().deserialize(buffer);
          break;
        case TS_FILE_INIT:
          messagePayload = new TsFileInitMessagePayload().deserialize(buffer);
          break;
        case TS_FILE_PIECE:
          messagePayload = new TsFilePieceMessagePayload().deserialize(buffer);
          break;
        case TS_FILE_SEAL:
          messagePayload = new TsFileSealMessagePayload().deserialize(buffer);
          break;
        case TS_FILE_ERROR:
          messagePayload = new TsFileErrorMessagePayload().deserialize(buffer);
          break;
        default:
          LOGGER.warn("unexpected message type: {}, message payload will be null", messageType);
          break;
      }
    } else {
      LOGGER.warn("unexpected message type: {}, message payload will be null", messageType);
    }

    final SubscriptionCommitContext commitContext = SubscriptionCommitContext.deserialize(buffer);
    return new SubscriptionPolledMessage(messageType, messagePayload, commitContext);
  }

  //////////////////////////// serialization ////////////////////////////

  /**
   * @return true -> byte buffer is not null
   */
  public boolean trySerialize() {
    if (Objects.isNull(byteBuffer)) {
      try {
        try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
            final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
          serialize(outputStream);
          byteBuffer =
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
        }
        return true;
      } catch (final IOException e) {
        LOGGER.warn(
            "Subscription: something unexpected happened when serializing SubscriptionRawMessage",
            e);
      }
      return false;
    }
    return true;
  }

  public void resetByteBuffer() {
    // maybe friendly for gc
    byteBuffer = null;
  }

  /////////////////////////////// object ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionPolledMessage{messageType="
        + SubscriptionPolledMessageType.valueOf(messageType).toString()
        + ", messagePayload="
        + messagePayload
        + ", commitContext="
        + commitContext
        + "}";
  }
}
