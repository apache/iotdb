package org.apache.iotdb.db.queryengine.execution.operator;

public interface RowIdComparisonStrategy {
  int compare(long leftRowId, long rightRowId);
}
