package org.apache.iotdb.cluster.exception;

public class TruncateCommittedEntryException extends Exception {
  public TruncateCommittedEntryException(long index, long committed) {
    super(String.format("The committed entries cannot be truncated: parameter: %d, commitIndex : %d", index, committed));
  }
}