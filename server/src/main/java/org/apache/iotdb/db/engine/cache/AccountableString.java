package org.apache.iotdb.db.engine.cache;

import java.util.Objects;
import org.apache.iotdb.tsfile.common.cache.Accountable;

public class AccountableString implements Accountable {

  private final String string;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AccountableString that = (AccountableString) o;
    return Objects.equals(string, that.string);
  }

  @Override
  public int hashCode() {
    return Objects.hash(string);
  }
}
