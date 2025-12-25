package org.apache.iotdb.db.queryengine.execution.operator;

/** Hash strategy that evaluates over row IDs */
public interface RowIdHashStrategy {
  boolean equals(long leftRowId, long rightRowId);

  long hashCode(long rowId);
}
