package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

public interface HyperLogLogState {
  HyperLogLog getHyperLogLog();

  void setHyperLogLog(HyperLogLog value);
}
