package org.apache.iotdb.db.nvm.space;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class NVMStringSpace extends NVMSpace {

  List<String> existStringList = new ArrayList<>();

  NVMStringSpace(long offset, long size, ByteBuffer byteBuffer) {
    super(offset, size, byteBuffer);

    // TODO recover set
  }

  public int put(String s) {
    // TODO
  }

  public String get(int index) {
    // TODO
    return null;
  }
}
