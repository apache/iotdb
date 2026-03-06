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

package org.apache.iotdb.rpc.subscription.payload.request;

import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeSubscribeSeekReq extends TPipeSubscribeReq {

  /** Seek type constants. */
  public static final short SEEK_TO_BEGINNING = 1;

  public static final short SEEK_TO_END = 2;
  public static final short SEEK_TO_TIMESTAMP = 3;

  private transient String topicName;
  private transient short seekType;
  private transient long timestamp; // only meaningful when seekType == SEEK_TO_TIMESTAMP

  public String getTopicName() {
    return topicName;
  }

  public short getSeekType() {
    return seekType;
  }

  public long getTimestamp() {
    return timestamp;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  /**
   * Serialize the incoming parameters into {@code PipeSubscribeSeekReq}, called by the subscription
   * client.
   */
  public static PipeSubscribeSeekReq toTPipeSubscribeReq(
      final String topicName, final short seekType, final long timestamp)
      throws IOException {
    final PipeSubscribeSeekReq req = new PipeSubscribeSeekReq();

    req.topicName = topicName;
    req.seekType = seekType;
    req.timestamp = timestamp;

    req.version = PipeSubscribeRequestVersion.VERSION_1.getVersion();
    req.type = PipeSubscribeRequestType.SEEK.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(topicName, outputStream);
      ReadWriteIOUtils.write(seekType, outputStream);
      if (seekType == SEEK_TO_TIMESTAMP) {
        ReadWriteIOUtils.write(timestamp, outputStream);
      }
      req.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return req;
  }

  /** Deserialize {@code TPipeSubscribeReq} to obtain parameters, called by the subscription server. */
  public static PipeSubscribeSeekReq fromTPipeSubscribeReq(final TPipeSubscribeReq seekReq) {
    final PipeSubscribeSeekReq req = new PipeSubscribeSeekReq();

    if (Objects.nonNull(seekReq.body) && seekReq.body.hasRemaining()) {
      req.topicName = ReadWriteIOUtils.readString(seekReq.body);
      req.seekType = ReadWriteIOUtils.readShort(seekReq.body);
      if (req.seekType == SEEK_TO_TIMESTAMP) {
        req.timestamp = ReadWriteIOUtils.readLong(seekReq.body);
      }
    }

    req.version = seekReq.version;
    req.type = seekReq.type;
    req.body = seekReq.body;

    return req;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeSubscribeSeekReq that = (PipeSubscribeSeekReq) obj;
    return Objects.equals(this.topicName, that.topicName)
        && this.seekType == that.seekType
        && this.timestamp == that.timestamp
        && this.version == that.version
        && this.type == that.type
        && Objects.equals(this.body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicName, seekType, timestamp, version, type, body);
  }
}
