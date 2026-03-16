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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Objects;

public class SubscriptionCommitContext implements Comparable<SubscriptionCommitContext> {

  /**
   * Version 1: original 5 fields (dataNodeId, rebootTimes, topicName, consumerGroupId, commitId).
   * Version 2: added regionId + epoch.
   */
  private static final byte SERIALIZATION_VERSION = 2;

  private final int dataNodeId;

  private final int rebootTimes;

  private final String topicName;

  private final String consumerGroupId;

  private final long commitId;

  private final long seekGeneration;

  private final String regionId;

  private final long epoch;

  public static final long INVALID_COMMIT_ID = -1;

  public SubscriptionCommitContext(
      final int dataNodeId,
      final int rebootTimes,
      final String topicName,
      final String consumerGroupId,
      final long commitId) {
    this(dataNodeId, rebootTimes, topicName, consumerGroupId, commitId, 0L, "", 0L);
  }

  public SubscriptionCommitContext(
      final int dataNodeId,
      final int rebootTimes,
      final String topicName,
      final String consumerGroupId,
      final long commitId,
      final String regionId,
      final long epoch) {
    this(dataNodeId, rebootTimes, topicName, consumerGroupId, commitId, 0L, regionId, epoch);
  }

  public SubscriptionCommitContext(
      final int dataNodeId,
      final int rebootTimes,
      final String topicName,
      final String consumerGroupId,
      final long commitId,
      final long seekGeneration,
      final String regionId,
      final long epoch) {
    this.dataNodeId = dataNodeId;
    this.rebootTimes = rebootTimes;
    this.topicName = topicName;
    this.consumerGroupId = consumerGroupId;
    this.commitId = commitId;
    this.seekGeneration = seekGeneration;
    this.regionId = regionId;
    this.epoch = epoch;
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

  public long getSeekGeneration() {
    return seekGeneration;
  }

  public String getRegionId() {
    return regionId;
  }

  public long getEpoch() {
    return epoch;
  }

  /////////////////////////////// de/ser ///////////////////////////////

  public static ByteBuffer serialize(final SubscriptionCommitContext commitContext)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      commitContext.serialize(outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(SERIALIZATION_VERSION, stream);
    ReadWriteIOUtils.write(dataNodeId, stream);
    ReadWriteIOUtils.write(rebootTimes, stream);
    ReadWriteIOUtils.write(topicName, stream);
    ReadWriteIOUtils.write(consumerGroupId, stream);
    ReadWriteIOUtils.write(commitId, stream);
    ReadWriteIOUtils.write(seekGeneration, stream);
    ReadWriteIOUtils.write(regionId, stream);
    ReadWriteIOUtils.write(epoch, stream);
  }

  public static SubscriptionCommitContext deserialize(final ByteBuffer buffer) {
    final byte version = ReadWriteIOUtils.readByte(buffer);
    final int dataNodeId = ReadWriteIOUtils.readInt(buffer);
    final int rebootTimes = ReadWriteIOUtils.readInt(buffer);
    final String topicName = ReadWriteIOUtils.readString(buffer);
    final String consumerGroupId = ReadWriteIOUtils.readString(buffer);
    final long commitId = ReadWriteIOUtils.readLong(buffer);

    if (version == 1) {
      // V1 did not carry seekGeneration/regionId/epoch.
      return new SubscriptionCommitContext(
          dataNodeId, rebootTimes, topicName, consumerGroupId, commitId);
    }

    if (version == 2) {
      final long seekGeneration = ReadWriteIOUtils.readLong(buffer);
      final String regionId = ReadWriteIOUtils.readString(buffer);
      final long epoch = ReadWriteIOUtils.readLong(buffer);
      return new SubscriptionCommitContext(
          dataNodeId,
          rebootTimes,
          topicName,
          consumerGroupId,
          commitId,
          seekGeneration,
          regionId,
          epoch);
    }

    throw new IllegalArgumentException("Unsupported SubscriptionCommitContext version: " + version);
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
        && this.commitId == that.commitId
        && this.seekGeneration == that.seekGeneration
        && Objects.equals(this.regionId, that.regionId)
        && this.epoch == that.epoch;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        dataNodeId,
        rebootTimes,
        topicName,
        consumerGroupId,
        commitId,
        seekGeneration,
        regionId,
        epoch);
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
        + ", seekGeneration="
        + seekGeneration
        + ", regionId="
        + regionId
        + ", epoch="
        + epoch
        + "}";
  }

  @Override
  public int compareTo(final SubscriptionCommitContext that) {
    return Comparator.comparingInt(SubscriptionCommitContext::getDataNodeId)
        .thenComparingInt(SubscriptionCommitContext::getRebootTimes)
        .thenComparing(SubscriptionCommitContext::getTopicName)
        .thenComparing(SubscriptionCommitContext::getConsumerGroupId)
        .thenComparingLong(SubscriptionCommitContext::getCommitId)
        .thenComparingLong(SubscriptionCommitContext::getSeekGeneration)
        .thenComparing(SubscriptionCommitContext::getRegionId)
        .thenComparingLong(SubscriptionCommitContext::getEpoch)
        .compare(this, that);
  }
}
