/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate;

import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static java.lang.Math.toIntExact;
import static org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOfInstance;

public class SpaceSaving<K> {

  private static final long INSTANCE_SIZE = shallowSizeOfInstance(SpaceSaving.class);
  private static final long STREAM_SUMMARY_SIZE = shallowSizeOfInstance(StreamSummary.class);
  private static final long LIST_NODE2_SIZE = shallowSizeOfInstance(ListNode2.class);
  private static final long COUNTER_SIZE = shallowSizeOfInstance(Counter.class);

  private StreamSummary<K> streamSummary;
  private final int maxBuckets;
  private final int capacity;

  // Used to serialize and deserialize buckets for different types of keys
  private final ApproxMostFrequentBucketSerializer<K> serializer;
  private final ApproxMostFrequentBucketDeserializer<K> deserializer;
  // Used to calculate the size of keys in bytes
  private final SpaceSavingByteCalculator<K> calculator;

  public SpaceSaving(
      int maxBuckets,
      int capacity,
      ApproxMostFrequentBucketSerializer<K> serializer,
      ApproxMostFrequentBucketDeserializer<K> deserializer,
      SpaceSavingByteCalculator<K> calculator) {
    this.streamSummary = new StreamSummary<>(capacity);
    this.maxBuckets = maxBuckets;
    this.capacity = capacity;
    this.serializer = serializer;
    this.deserializer = deserializer;
    this.calculator = calculator;
  }

  public SpaceSaving(
      byte[] bytes,
      ApproxMostFrequentBucketSerializer<K> serializer,
      ApproxMostFrequentBucketDeserializer<K> deserializer,
      SpaceSavingByteCalculator<K> calculator) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    this.maxBuckets = ReadWriteIOUtils.readInt(byteBuffer);
    this.capacity = ReadWriteIOUtils.readInt(byteBuffer);
    int counterSize = ReadWriteIOUtils.readInt(byteBuffer);
    this.streamSummary = new StreamSummary<>(capacity);
    this.serializer = serializer;
    this.deserializer = deserializer;
    this.calculator = calculator;
    for (int i = 0; i < counterSize; i++) {
      this.deserializer.deserialize(byteBuffer, this);
    }
  }

  public static long getDefaultEstimatedSize() {
    return INSTANCE_SIZE
        + STREAM_SUMMARY_SIZE
        // Long.BYTES as a proxy for the size of K
        + 50 * (LIST_NODE2_SIZE + COUNTER_SIZE + Long.BYTES);
  }

  public long getEstimatedSize() {
    return INSTANCE_SIZE
        + STREAM_SUMMARY_SIZE
        // Long.BYTES as a proxy for the size of K
        + streamSummary.size() * (LIST_NODE2_SIZE + +COUNTER_SIZE + Long.BYTES);
  }

  public interface BucketConsumer<K> {
    void process(K key, long value);
  }

  public void add(K key) {
    streamSummary.offer(key);
  }

  public void add(K key, long incrementCount) {
    streamSummary.offer(key, toIntExact(incrementCount));
  }

  public void merge(SpaceSaving<K> other) {
    List<Counter<K>> counters = other.streamSummary.topK(capacity);
    for (Counter<K> counter : counters) {
      streamSummary.offer(counter.getItem(), toIntExact(counter.getCount()));
    }
  }

  public void forEachBucket(BucketConsumer<K> consumer) {
    List<Counter<K>> counters = streamSummary.topK(maxBuckets);
    for (Counter<K> counter : counters) {
      consumer.process(counter.getItem(), counter.getCount());
    }
  }

  public Map<K, Long> getBuckets() {
    ImmutableMap.Builder<K, Long> buckets = ImmutableMap.builder();
    forEachBucket(buckets::put);
    return buckets.buildOrThrow();
  }

  public byte[] serialize() {
    List<Counter<K>> counters = streamSummary.topK(capacity);
    // Calculate the size of the keys
    int keyBytesSize = calculator.calculateBytes(counters);
    // maxBucket + capacity + counterSize +  keySize + countSize
    int estimatedTotalBytes =
        Integer.BYTES
            + Integer.BYTES
            + Integer.BYTES
            + keyBytesSize
            + counters.size() * (Long.BYTES + Long.BYTES);
    ByteBuffer byteBuffer = ByteBuffer.allocate(estimatedTotalBytes);
    ReadWriteIOUtils.write(maxBuckets, byteBuffer);
    ReadWriteIOUtils.write(capacity, byteBuffer);
    ReadWriteIOUtils.write(counters.size(), byteBuffer);
    // Serialize key and counts.
    for (Counter<K> counter : counters) {
      this.serializer.serialize(counter.getItem(), counter.getCount(), byteBuffer);
    }
    return byteBuffer.array();
  }

  public void reset() {
    this.streamSummary = new StreamSummary<>(capacity);
  }
}
