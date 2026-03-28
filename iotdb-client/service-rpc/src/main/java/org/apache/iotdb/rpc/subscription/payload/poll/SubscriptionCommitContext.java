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
   * Version 2: added regionId + physicalTime (serialized in the legacy epoch slot). Version 3:
   * added writerId + writerProgress.
   */
  private static final byte SERIALIZATION_VERSION = 3;

  private final int dataNodeId;

  private final int rebootTimes;

  private final String topicName;

  private final String consumerGroupId;

  private final long commitId;

  private final long seekGeneration;

  private final String regionId;

  private final long epoch;

  private final WriterId writerId;

  private final WriterProgress writerProgress;

  public static final long INVALID_COMMIT_ID = -1;

  public SubscriptionCommitContext(
      final int dataNodeId,
      final int rebootTimes,
      final String topicName,
      final String consumerGroupId,
      final long commitId) {
    this(dataNodeId, rebootTimes, topicName, consumerGroupId, commitId, 0L, "", 0L, null, null);
  }

  public SubscriptionCommitContext(
      final int dataNodeId,
      final int rebootTimes,
      final String topicName,
      final String consumerGroupId,
      final long commitId,
      final String regionId,
      final long epoch) {
    this(
        dataNodeId,
        rebootTimes,
        topicName,
        consumerGroupId,
        commitId,
        0L,
        regionId,
        epoch,
        null,
        null);
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
    this(
        dataNodeId,
        rebootTimes,
        topicName,
        consumerGroupId,
        commitId,
        seekGeneration,
        regionId,
        epoch,
        null,
        null);
  }

  public SubscriptionCommitContext(
      final int dataNodeId,
      final int rebootTimes,
      final String topicName,
      final String consumerGroupId,
      final long seekGeneration,
      final WriterId writerId,
      final WriterProgress writerProgress) {
    this(
        dataNodeId,
        rebootTimes,
        topicName,
        consumerGroupId,
        Objects.nonNull(writerProgress) ? writerProgress.getLocalSeq() : INVALID_COMMIT_ID,
        seekGeneration,
        Objects.nonNull(writerId) ? writerId.getRegionId() : "",
        Objects.nonNull(writerProgress) ? writerProgress.getPhysicalTime() : 0L,
        writerId,
        writerProgress);
  }

  private SubscriptionCommitContext(
      final int dataNodeId,
      final int rebootTimes,
      final String topicName,
      final String consumerGroupId,
      final long commitId,
      final long seekGeneration,
      final String regionId,
      final long epoch,
      final WriterId writerId,
      final WriterProgress writerProgress) {
    this.dataNodeId = dataNodeId;
    this.rebootTimes = rebootTimes;
    this.topicName = topicName;
    this.consumerGroupId = consumerGroupId;
    this.commitId = commitId;
    this.seekGeneration = seekGeneration;
    this.regionId = regionId;
    this.epoch = epoch;
    this.writerId = writerId;
    this.writerProgress = writerProgress;
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

  public long getLocalSeq() {
    return Objects.nonNull(writerProgress) ? writerProgress.getLocalSeq() : commitId;
  }

  public long getSeekGeneration() {
    return seekGeneration;
  }

  public String getRegionId() {
    return regionId;
  }

  public long getPhysicalTime() {
    return Objects.nonNull(writerProgress) ? writerProgress.getPhysicalTime() : epoch;
  }

  public WriterId getWriterId() {
    return writerId;
  }

  public WriterProgress getWriterProgress() {
    return writerProgress;
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
    final boolean hasWriterProgress = Objects.nonNull(writerId) && Objects.nonNull(writerProgress);
    ReadWriteIOUtils.write((byte) (hasWriterProgress ? 1 : 0), stream);
    if (hasWriterProgress) {
      writerId.serialize(stream);
      writerProgress.serialize(stream);
    }
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

    if (version == 3) {
      final long seekGeneration = ReadWriteIOUtils.readLong(buffer);
      final String regionId = ReadWriteIOUtils.readString(buffer);
      final long epoch = ReadWriteIOUtils.readLong(buffer);
      final boolean hasWriterProgress = ReadWriteIOUtils.readByte(buffer) != 0;
      final WriterId writerId = hasWriterProgress ? WriterId.deserialize(buffer) : null;
      final WriterProgress writerProgress =
          hasWriterProgress ? WriterProgress.deserialize(buffer) : null;
      return new SubscriptionCommitContext(
          dataNodeId,
          rebootTimes,
          topicName,
          consumerGroupId,
          commitId,
          seekGeneration,
          regionId,
          epoch,
          writerId,
          writerProgress);
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
        && this.epoch == that.epoch
        && Objects.equals(this.writerId, that.writerId)
        && Objects.equals(this.writerProgress, that.writerProgress);
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
        epoch,
        writerId,
        writerProgress);
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
        + ", localSeq="
        + getLocalSeq()
        + ", seekGeneration="
        + seekGeneration
        + ", regionId="
        + regionId
        + ", physicalTime="
        + getPhysicalTime()
        + ", writerId="
        + writerId
        + ", writerProgress="
        + writerProgress
        + "}";
  }

  @Override
  public int compareTo(final SubscriptionCommitContext that) {
    return Comparator.comparingInt(SubscriptionCommitContext::getDataNodeId)
        .thenComparingInt(SubscriptionCommitContext::getRebootTimes)
        .thenComparing(SubscriptionCommitContext::getTopicName)
        .thenComparing(SubscriptionCommitContext::getConsumerGroupId)
        .thenComparingLong(SubscriptionCommitContext::getSeekGeneration)
        .thenComparing(SubscriptionCommitContext::getRegionId)
        .thenComparingInt(
            context ->
                Objects.nonNull(context.getWriterId()) ? context.getWriterId().getNodeId() : -1)
        .thenComparingLong(
            context ->
                Objects.nonNull(context.getWriterId())
                    ? context.getWriterId().getWriterEpoch()
                    : -1L)
        .thenComparingLong(SubscriptionCommitContext::getPhysicalTime)
        .thenComparingLong(SubscriptionCommitContext::getLocalSeq)
        .compare(this, that);
  }
}
