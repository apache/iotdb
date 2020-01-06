package org.apache.iotdb.db.nvm.metadata;

import java.util.HashSet;
import java.util.Set;
import org.apache.iotdb.db.nvm.space.NVMSpace;

public class FreeSpaceBitMap extends NVMSpaceMetadata {

  public FreeSpaceBitMap(NVMSpace space) {
    super(space);
  }

  public void update(int index, boolean setFree) {
    space.getByteBuffer().put(index, setFree ? (byte) 0 : (byte) 1);
  }

  public Set<Integer> getValidSpaceIndexSet() {
    Set<Integer> freeSpaceIndexList = new HashSet<>();
    for (int i = 0; i < space.getSize(); i++) {
      byte flag = space.getByteBuffer().get(i);
      if (flag == 1) {
        freeSpaceIndexList.add(i);
      }
    }
    return freeSpaceIndexList;
  }
}
