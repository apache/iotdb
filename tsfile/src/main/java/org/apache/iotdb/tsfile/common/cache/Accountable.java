package org.apache.iotdb.tsfile.common.cache;

public interface Accountable {

  void setRAMSize(long size);

  long getRAMSize();
}
