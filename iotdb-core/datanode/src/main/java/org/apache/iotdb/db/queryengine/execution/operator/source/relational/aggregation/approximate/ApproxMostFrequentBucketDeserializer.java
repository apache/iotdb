package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate;

import java.nio.ByteBuffer;

public interface ApproxMostFrequentBucketDeserializer<K> {
  void deserialize(ByteBuffer input, SpaceSaving<K> spaceSaving);
}
