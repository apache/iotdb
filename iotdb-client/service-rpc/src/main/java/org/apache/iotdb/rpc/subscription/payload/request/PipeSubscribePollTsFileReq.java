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
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeSubscribePollTsFileReq extends TPipeSubscribeReq {

  private transient String topicName;

  private transient String fileName;

  private transient long endWritingOffset;

  public String getTopicName() {
    return topicName;
  }

  public String getFileName() {
    return fileName;
  }

  public long getEndWritingOffset() {
    return endWritingOffset;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeSubscribePollTsFileReq toTPipeSubscribeReq(
      String topicName, String fileName, long endWritingOffset) throws IOException {
    final PipeSubscribePollTsFileReq req = new PipeSubscribePollTsFileReq();

    req.topicName = topicName;
    req.fileName = fileName;
    req.endWritingOffset = endWritingOffset;

    req.version = PipeSubscribeRequestVersion.VERSION_1.getVersion();
    req.type = PipeSubscribeRequestType.POLL_TS_FILE.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(topicName, outputStream);
      ReadWriteIOUtils.write(fileName, outputStream);
      ReadWriteIOUtils.write(endWritingOffset, outputStream);
      req.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return req;
  }

  public static PipeSubscribePollTsFileReq fromTPipeSubscribeReq(TPipeSubscribeReq pollTsFileReq) {
    final PipeSubscribePollTsFileReq req = new PipeSubscribePollTsFileReq();

    if (Objects.nonNull(pollTsFileReq.body) && pollTsFileReq.body.hasRemaining()) {
      req.topicName = ReadWriteIOUtils.readString(pollTsFileReq.body);
      req.fileName = ReadWriteIOUtils.readString(pollTsFileReq.body);
      req.endWritingOffset = ReadWriteIOUtils.readLong(pollTsFileReq.body);
    }

    req.version = pollTsFileReq.version;
    req.type = pollTsFileReq.type;
    req.body = pollTsFileReq.body;

    return req;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PipeSubscribePollTsFileReq that = (PipeSubscribePollTsFileReq) obj;
    return Objects.equals(this.topicName, that.topicName)
        && Objects.equals(this.fileName, that.fileName)
        && this.endWritingOffset == that.endWritingOffset
        && this.version == that.version
        && this.type == that.type
        && Objects.equals(this.body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicName, fileName, endWritingOffset, version, type, body);
  }
}
