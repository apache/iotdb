package org.apache.iotdb.db.queryengine.execution.operator;

public interface RowIdComparisonHashStrategy extends RowIdComparisonStrategy, RowIdHashStrategy {
  @Override
  default boolean equals(long leftRowId, long rightRowId) {
    return compare(leftRowId, rightRowId) == 0;
  }
}
