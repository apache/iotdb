package org.apache.iotdb.db.sync.datasource;

public enum SyncResponseType {
  INFO(1),
  WARN(2),
  ERROR(3);

  private int value;

  SyncResponseType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}
