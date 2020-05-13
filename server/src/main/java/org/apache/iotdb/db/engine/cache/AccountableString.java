package org.apache.iotdb.db.engine.cache;

import org.apache.iotdb.tsfile.common.cache.Accountable;

public class AccountableString implements Accountable {

  private String string;
  private long RAMSize;

  public AccountableString(String string) {
    this.string = string;
  }

  public String getString() {
    return string;
  }

  @Override
  public void setRAMSize(long size) {
    this.RAMSize = size;
  }

  @Override
  public long getRAMSize() {
    return RAMSize;
  }
}
