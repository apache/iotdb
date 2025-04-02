package org.apache.iotdb.commons.consensus.index.impl;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;

import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KafkaProgressIndex extends ProgressIndex {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(KafkaProgressIndex.class) + ProgressIndex.LOCK_SIZE;
  private static final long ENTRY_SIZE =
      RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + Long.BYTES;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final Map<Integer, Long> partitionToOffset;

  public KafkaProgressIndex() {
    this(Collections.emptyMap());
  }

  public KafkaProgressIndex(Integer partition, Long Offset) {
    this(Collections.singletonMap(partition, Offset));
  }

  public KafkaProgressIndex(Map<Integer, Long> partitionToOffset) {
    this.partitionToOffset = new HashMap<>(partitionToOffset);
  }

  public Map<Integer, Long> getPartitionToOffset() {
    return partitionToOffset;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    lock.readLock().lock();
    try {
      ProgressIndexType.KAFKA_PROGRESS_INDEX.serialize(byteBuffer);

      ReadWriteIOUtils.write(partitionToOffset.size(), byteBuffer);
      for (final Map.Entry<Integer, Long> entry : partitionToOffset.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), byteBuffer);
        ReadWriteIOUtils.write(entry.getValue(), byteBuffer);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {

    lock.readLock().lock();
    try {
      ProgressIndexType.KAFKA_PROGRESS_INDEX.serialize(stream);

      ReadWriteIOUtils.write(partitionToOffset.size(), stream);
      for (final Map.Entry<Integer, Long> entry : partitionToOffset.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), stream);
        ReadWriteIOUtils.write(entry.getValue(), stream);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean isAfter(@Nonnull ProgressIndex progressIndex) {
    lock.readLock().lock();
    try {
      if (progressIndex instanceof MinimumProgressIndex) {
        return true;
      }

      if (progressIndex instanceof HybridProgressIndex) {
        return ((HybridProgressIndex) progressIndex).isGivenProgressIndexAfterSelf(this);
      }

      if (!(progressIndex instanceof KafkaProgressIndex)) {
        return false;
      }

      final KafkaProgressIndex thisKafkaProgressIndex = this;
      final KafkaProgressIndex thatKafkaIndex = (KafkaProgressIndex) progressIndex;
      return thatKafkaIndex.partitionToOffset.entrySet().stream()
          .noneMatch(
              entry ->
                  !thisKafkaProgressIndex.partitionToOffset.containsKey(entry.getKey())
                      || thisKafkaProgressIndex.partitionToOffset.get(entry.getKey())
                          <= entry.getValue());
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean equals(ProgressIndex progressIndex) {
    lock.readLock().lock();
    try {
      if (!(progressIndex instanceof KafkaProgressIndex)) {
        return false;
      }

      final KafkaProgressIndex thisKafkaProgressIndex = this;
      final KafkaProgressIndex thatKafkaProgressIndex = (KafkaProgressIndex) progressIndex;
      return thisKafkaProgressIndex.partitionToOffset.size()
              == thatKafkaProgressIndex.partitionToOffset.size()
          && thatKafkaProgressIndex.partitionToOffset.entrySet().stream()
              .allMatch(
                  entry ->
                      thisKafkaProgressIndex.partitionToOffset.containsKey(entry.getKey())
                          && thisKafkaProgressIndex
                              .partitionToOffset
                              .get(entry.getKey())
                              .equals(entry.getValue()));
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof KafkaProgressIndex)) {
      return false;
    }
    return this.equals((KafkaProgressIndex) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionToOffset);
  }

  @Override
  public ProgressIndex updateToMinimumEqualOrIsAfterProgressIndex(ProgressIndex progressIndex) {
    lock.writeLock().lock();
    try {
      if (!(progressIndex instanceof KafkaProgressIndex)) {
        return ProgressIndex.blendProgressIndex(this, progressIndex);
      }

      final KafkaProgressIndex thisKafkaProgressIndex = this;
      final KafkaProgressIndex thatKafkaProgressIndex = (KafkaProgressIndex) progressIndex;
      final Map<Integer, Long> partition2Offset =
          new HashMap<>(thisKafkaProgressIndex.partitionToOffset);
      thatKafkaProgressIndex.partitionToOffset.forEach(
          (thatK, thatV) ->
              partition2Offset.compute(
                  thatK, (thisK, thisV) -> (thisV == null ? thatV : Math.max(thisV, thatV))));
      return new KafkaProgressIndex(partition2Offset);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public static KafkaProgressIndex deserializeFrom(ByteBuffer byteBuffer) {
    final KafkaProgressIndex kafkaProgressIndex = new KafkaProgressIndex();
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      final int peerId = ReadWriteIOUtils.readInt(byteBuffer);
      final long searchIndex = ReadWriteIOUtils.readLong(byteBuffer);
      kafkaProgressIndex.partitionToOffset.put(peerId, searchIndex);
    }
    return kafkaProgressIndex;
  }

  public static KafkaProgressIndex deserializeFrom(InputStream stream) throws IOException {
    final KafkaProgressIndex kafkaProgressIndex = new KafkaProgressIndex();
    final int size = ReadWriteIOUtils.readInt(stream);
    for (int i = 0; i < size; i++) {
      final int peerId = ReadWriteIOUtils.readInt(stream);
      final long searchIndex = ReadWriteIOUtils.readLong(stream);
      kafkaProgressIndex.partitionToOffset.put(peerId, searchIndex);
    }
    return kafkaProgressIndex;
  }

  @Override
  public String toString() {
    return "KafkaProgressIndex{" + "partition2offset=" + partitionToOffset + '}';
  }

  @Override
  public ProgressIndexType getType() {
    return ProgressIndexType.KAFKA_PROGRESS_INDEX;
  }

  @Override
  public TotalOrderSumTuple getTotalOrderSumTuple() {
    lock.readLock().lock();
    try {
      return new TotalOrderSumTuple(
          partitionToOffset.values().stream().mapToLong(Long::longValue).sum());
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE + partitionToOffset.size() * ENTRY_SIZE;
  }
}
