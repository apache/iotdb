package org.apache.iotdb.db.metadata.lastCache;

public abstract class LastCacheValue implements ILastCacheValue {

  protected long timestamp;

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
}
