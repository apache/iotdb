package org.apache.iotdb.db.queryengine.execution.operator;

/**
 * Reference to a row.
 *
 * <p>Note: RowReference gives us the ability to defer row ID generation (which can be expensive in
 * tight loops).
 */
public interface RowReference {
  /**
   * Compares the referenced row to the specified row ID using the provided RowIdComparisonStrategy.
   */
  int compareTo(RowIdComparisonStrategy strategy, long rowId);

  /**
   * Checks equality of the referenced row with the specified row ID using the provided
   * RowIdHashStrategy.
   */
  boolean equals(RowIdHashStrategy strategy, long rowId);

  /** Calculates the hash of the referenced row using the provided RowIdHashStrategy. */
  long hash(RowIdHashStrategy strategy);

  /** Allocate a stable row ID that can be used to reference this row at a future point. */
  long allocateRowId();
}
