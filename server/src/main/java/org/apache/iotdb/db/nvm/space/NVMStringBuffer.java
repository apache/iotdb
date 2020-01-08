package org.apache.iotdb.db.nvm.space;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class NVMStringBuffer {

  private List<String> existStringList;

  private long size;
  private NVMSpace count;
  private NVMSpace lens;
  private NVMSpace values;

  public NVMStringBuffer(long size) throws IOException {
    this.size = size;

    NVMSpaceManager spaceManager = NVMSpaceManager.getInstance();
    count = spaceManager.allocateSpace(NVMSpaceManager.getPrimitiveTypeByteSize(TSDataType.INT32));
    lens = spaceManager.allocateSpace(size / 2);
    values = spaceManager.allocateSpace(size / 2);

    recover();
  }

  private void recover() {
    int stringListLen = count.getByteBuffer().getInt(0);
    existStringList = new ArrayList<>(stringListLen);
    for (int i = 0; i < stringListLen; i++) {
      int stringLen = lens.getByteBuffer().getInt(i);
      byte[] bytes = new byte[stringLen];
      values.getByteBuffer().get(bytes);
      existStringList.add(new String(bytes));
    }
  }

  public int put(String s) {
    if (existStringList.contains(s)) {
      return existStringList.indexOf(s);
    } else {
      existStringList.add(s);
      serialize(s);
      return existStringList.size() - 1;
    }
  }

  private void serialize(String s) {
    count.getByteBuffer().putInt(0, existStringList.size());
    lens.getByteBuffer().putInt(existStringList.size() - 1, s.length());
    values.getByteBuffer().put(s.getBytes());
  }

  public String get(int index) {
    return existStringList.get(index);
  }
}
