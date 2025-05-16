package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import java.nio.ByteBuffer;

public interface ApproxMostFrequentBucketSerializer<K> {
  void serialize(K key, long count, ByteBuffer output);
}
