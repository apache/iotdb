package org.apache.iotdb.db.exception.metadata;

public class AcquireLockTimeoutException extends MetadataException {
  public AcquireLockTimeoutException(String msg) {
    super(msg);
  }
}
