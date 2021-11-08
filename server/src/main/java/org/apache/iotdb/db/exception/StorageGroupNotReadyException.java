package org.apache.iotdb.db.exception;

public class StorageGroupNotReadyException extends StorageEngineException {

  public StorageGroupNotReadyException(String storageGroup, int errorCode) {
    super("the sg " + storageGroup + " may not ready now, please wait and retry later", errorCode);
  }
}
