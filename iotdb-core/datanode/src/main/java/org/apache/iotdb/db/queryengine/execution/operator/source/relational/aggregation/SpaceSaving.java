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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import com.clearspring.analytics.util.ListNode2;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static java.lang.Math.toIntExact;
import static org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOfInstance;

public class SpaceSaving {

  private static final long INSTANCE_SIZE = shallowSizeOfInstance(SpaceSaving.class);
  private static final long STREAM_SUMMARY_SIZE = shallowSizeOfInstance(StreamSummary.class);
  private static final long LIST_NODE2_SIZE = shallowSizeOfInstance(ListNode2.class);
  private static final long COUNTER_SIZE = shallowSizeOfInstance(Counter.class);

  private StreamSummary<String> stremSummary;
  private final int maxBuckets;
  private final int capacity;

  public SpaceSaving(int maxBuckets, int capacity) {
    this.stremSummary = new StreamSummary<>(capacity);
    this.maxBuckets = maxBuckets;
    this.capacity = capacity;
  }

  public SpaceSaving(byte[] bytes) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    this.maxBuckets = ReadWriteIOUtils.readInt(byteBuffer);
    this.capacity = ReadWriteIOUtils.readInt(byteBuffer);
    int counterSize = ReadWriteIOUtils.readInt(byteBuffer);
    this.stremSummary = new StreamSummary<>(capacity);
    for (int i = 0; i < counterSize; i++) {
      String key = ReadWriteIOUtils.readString(byteBuffer);
      long value = ReadWriteIOUtils.readLong(byteBuffer);
      stremSummary.offer(key, toIntExact(value));
    }
  }

  public long getEstimatedSize() {
    return INSTANCE_SIZE
        + STREAM_SUMMARY_SIZE
        + stremSummary.size() * (LIST_NODE2_SIZE + COUNTER_SIZE + Long.BYTES);
  }

  public interface BucketConsumer<K> {
    void process(K key, long value);
  }

  public void add(boolean key) {
    stremSummary.offer(String.valueOf(key));
  }

  public void add(int key) {
    stremSummary.offer(String.valueOf(key));
  }

  public void add(long key) {
    stremSummary.offer(String.valueOf(key));
  }

  public void add(float key) {
    stremSummary.offer(String.valueOf(key));
  }

  public void add(double key) {
    stremSummary.offer(String.valueOf(key));
  }

  public void add(Binary key) {
    stremSummary.offer(key.getStringValue(StandardCharsets.UTF_8));
  }

  public void add(boolean value, long incrementCount) {
    stremSummary.offer(String.valueOf(value), toIntExact(incrementCount));
  }

  public void add(int value, long incrementCount) {
    stremSummary.offer(String.valueOf(value), toIntExact(incrementCount));
  }

  public void add(long value, long incrementCount) {
    stremSummary.offer(String.valueOf(value), toIntExact(incrementCount));
  }

  public void add(float value, long incrementCount) {
    stremSummary.offer(String.valueOf(value), toIntExact(incrementCount));
  }

  public void add(double value, long incrementCount) {
    stremSummary.offer(String.valueOf(value), toIntExact(incrementCount));
  }

  public void add(Binary value, long incrementCount) {
    stremSummary.offer(value.getStringValue(StandardCharsets.UTF_8), toIntExact(incrementCount));
  }

  public void merge(SpaceSaving other) {
    List<Counter<String>> counters = other.stremSummary.topK(capacity);
    for (Counter<String> counter : counters) {
      stremSummary.offer(counter.getItem(), toIntExact(counter.getCount()));
    }
  }

  public void forEachBucket(BucketConsumer<String> consumer) {
    List<Counter<String>> counters = stremSummary.topK(maxBuckets);
    for (Counter<String> counter : counters) {
      consumer.process(counter.getItem(), counter.getCount());
    }
  }

  public Map<String, Long> getBuckets() {
    ImmutableMap.Builder<String, Long> buckets = ImmutableMap.builder();
    forEachBucket(buckets::put);
    return buckets.buildOrThrow();
  }

  public byte[] serialize() {
    List<Counter<String>> counters = stremSummary.topK(maxBuckets);
    int keyBytesSize = counters.stream().mapToInt(counter -> counter.getItem().length()).sum();
    // maxBucket + capacity + counterSize +  keySize + countSize
    int estimatedTotalBytes =
        Integer.BYTES
            + Integer.BYTES
            + Integer.BYTES
            + keyBytesSize
            + counters.size() * (Long.BYTES + Integer.BYTES);
    // for variable length slices, it should work.
    ByteBuffer byteBuffer = ByteBuffer.allocate(estimatedTotalBytes);
    byteBuffer.putInt(maxBuckets);
    byteBuffer.putInt(capacity);
    byteBuffer.putInt(counters.size());
    // Serialize key and counts.
    for (Counter<String> counter : counters) {
      serializeBucket(counter.getItem(), counter.getCount(), byteBuffer);
    }
    return byteBuffer.array();
  }

  public static void serializeBucket(String key, long count, ByteBuffer byteBuffer) {
    byteBuffer.putInt(key.length());
    byteBuffer.put(key.getBytes(StandardCharsets.UTF_8));
    byteBuffer.putLong(count);
  }

  public void reset() {
    this.stremSummary = null;
  }
}
