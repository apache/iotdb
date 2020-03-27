package org.apache.iotdb.cluster.exception;

public class TruncateCommittedEntryException extends Exception {
  public TruncateCommittedEntryException() {
    super("The logs are going to truncate committed logs");
  }
}