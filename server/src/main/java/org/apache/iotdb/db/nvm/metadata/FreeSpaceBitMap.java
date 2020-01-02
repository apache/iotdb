package org.apache.iotdb.db.nvm.metadata;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class FreeSpaceBitMap extends NVMSpaceMetadata {

  public FreeSpaceBitMap(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  public void update(int index, boolean setFree) {
    byteBuffer.put(index, setFree ? (byte) 0 : (byte) 1);
  }

  public Set<Integer> getValidSpaceIndexSet() {
    Set<Integer> freeSpaceIndexList = new HashSet<>();
    for (int i = 0; i < byteBuffer.capacity(); i++) {
      byte flag = byteBuffer.get(i);
      if (flag == 1) {
        freeSpaceIndexList.add(i);
      }
    }
    return freeSpaceIndexList;
  }
}
