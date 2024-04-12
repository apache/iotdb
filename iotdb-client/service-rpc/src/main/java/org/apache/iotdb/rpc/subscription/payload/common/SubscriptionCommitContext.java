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

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class SubscriptionCommitContext implements Comparable<SubscriptionCommitContext> {

  private final int dataNodeId;

  private final int rebootTimes;

  private final String topicName;

  private final String consumerGroupId;

  private final long commitId;

  public SubscriptionCommitContext(
      final int dataNodeId,
      final int rebootTimes,
      final String topicName,
      final String consumerGroupId,
      final long commitId) {
    this.dataNodeId = dataNodeId;
    this.rebootTimes = rebootTimes;
    this.topicName = topicName;
    this.consumerGroupId = consumerGroupId;
    this.commitId = commitId;
  }

  public int getDataNodeId() {
    return dataNodeId;
  }

  public int getRebootTimes() {
    return rebootTimes;
  }

  public String getTopicName() {
    return topicName;
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  public long getCommitId() {
    return commitId;
  }

  /////////////////////////////// de/ser ///////////////////////////////

  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(dataNodeId, stream);
    ReadWriteIOUtils.write(rebootTimes, stream);
    ReadWriteIOUtils.write(topicName, stream);
    ReadWriteIOUtils.write(consumerGroupId, stream);
    ReadWriteIOUtils.write(commitId, stream);
  }

  public static SubscriptionCommitContext deserialize(final ByteBuffer buffer) {
    final int dataNodeId = ReadWriteIOUtils.readInt(buffer);
    final int rebootTimes = ReadWriteIOUtils.readInt(buffer);
    final String topicName = ReadWriteIOUtils.readString(buffer);
    final String consumerGroupId = ReadWriteIOUtils.readString(buffer);
    final long commitId = ReadWriteIOUtils.readLong(buffer);
    return new SubscriptionCommitContext(
        dataNodeId, rebootTimes, topicName, consumerGroupId, commitId);
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
    final SubscriptionCommitContext that = (SubscriptionCommitContext) obj;
    return this.dataNodeId == that.dataNodeId
        && this.rebootTimes == that.rebootTimes
        && Objects.equals(this.topicName, that.topicName)
        && Objects.equals(this.consumerGroupId, that.consumerGroupId)
        && Objects.equals(this.commitId, that.commitId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataNodeId, rebootTimes, topicName, consumerGroupId, commitId);
  }

  @Override
  public String toString() {
    return "SubscriptionCommitContext{dataNodeId="
        + dataNodeId
        + ", rebootTimes="
        + rebootTimes
        + ", topicName="
        + topicName
        + ", consumerGroupId="
        + consumerGroupId
        + ", commitId="
        + commitId
        + "}";
  }

  @Override
  public int compareTo(SubscriptionCommitContext commitContext) {
    return 0;
  }
}
