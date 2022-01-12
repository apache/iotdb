package org.apache.iotdb.db.metadata.mtree.store.disk.cache;

public class CacheEntry {

  protected boolean isVolatile = true;

  public boolean isVolatile() {
    return isVolatile;
  }

  public void setVolatile(boolean aVolatile) {
    isVolatile = aVolatile;
  }
}
