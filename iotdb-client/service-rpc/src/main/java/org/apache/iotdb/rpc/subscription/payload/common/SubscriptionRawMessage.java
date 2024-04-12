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

import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class SubscriptionRawMessage {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionRawMessage.class);

  private final transient short messageType;

  private final transient SubscriptionRawMessagePayload messagePayload;

  private final transient SubscriptionCommitContext commitContext;

  private ByteBuffer byteBuffer;

  public SubscriptionRawMessage(
      short messageType,
      SubscriptionRawMessagePayload messagePayload,
      SubscriptionCommitContext commitContext) {
    this.messageType = messageType;
    this.messagePayload = messagePayload;
    this.commitContext = commitContext;
  }

  public short getMessageType() {
    return messageType;
  }

  public SubscriptionRawMessagePayload getMessagePayload() {
    return messagePayload;
  }

  public SubscriptionCommitContext getCommitContext() {
    return commitContext;
  }

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  /////////////////////////////// de/ser ///////////////////////////////

  public static ByteBuffer serialize(SubscriptionRawMessage message) throws IOException {
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

  public static SubscriptionRawMessage deserialize(final ByteBuffer buffer) {
    final short messageType = ReadWriteIOUtils.readShort(buffer);
    final SubscriptionRawMessagePayload messagePayload;
    if (SubscriptionRawMessageType.isValidatedMessageType(messageType)) {
      switch (SubscriptionRawMessageType.valueOf(messageType)) {
        case TABLETS:
          messagePayload = new TabletsMessagePayload().deserialize(buffer);
          break;
        case TS_FILE_INFO:
          messagePayload = new TsFileInfoMessagePayload().deserialize(buffer);
          break;
        default:
          messagePayload = null;
      }
    } else {
      messagePayload = null;
    }

    final SubscriptionCommitContext commitContext = SubscriptionCommitContext.deserialize(buffer);
    return new SubscriptionRawMessage(messageType, messagePayload, commitContext);
  }

  //////////////////////////// serialization ////////////////////////////

  /** @return true -> byte buffer is not null */
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
            "Subscription: something unexpected happened when serializing SubscriptionRawMessage, exception is {}",
            e.getMessage());
      }
      return false;
    }
    return true;
  }

  public void resetByteBuffer() {
    // maybe friendly for gc
    byteBuffer = null;
  }
}
