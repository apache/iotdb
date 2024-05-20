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

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PollFilePayload implements SubscriptionPollPayload {

  private transient String topicName;

  private transient String fileName;

  private transient long writingOffset;

  public String getTopicName() {
    return topicName;
  }

  public String getFileName() {
    return fileName;
  }

  public long getWritingOffset() {
    return writingOffset;
  }

  public PollFilePayload() {}

  public PollFilePayload(final String topicName, final String fileName, final long writingOffset) {
    this.topicName = topicName;
    this.fileName = fileName;
    this.writingOffset = writingOffset;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(topicName, stream);
    ReadWriteIOUtils.write(fileName, stream);
    ReadWriteIOUtils.write(writingOffset, stream);
  }

  @Override
  public SubscriptionPollPayload deserialize(final ByteBuffer buffer) {
    topicName = ReadWriteIOUtils.readString(buffer);
    fileName = ReadWriteIOUtils.readString(buffer);
    writingOffset = ReadWriteIOUtils.readLong(buffer);
    return this;
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
    final PollFilePayload that = (PollFilePayload) obj;
    return Objects.equals(this.topicName, that.topicName)
        && Objects.equals(this.fileName, that.fileName)
        && Objects.equals(this.writingOffset, that.writingOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicName, fileName, writingOffset);
  }

  @Override
  public String toString() {
    return "PollFilePayload{topicName="
        + topicName
        + ", fileName="
        + fileName
        + ", writingOffset="
        + writingOffset
        + "}";
  }
}
