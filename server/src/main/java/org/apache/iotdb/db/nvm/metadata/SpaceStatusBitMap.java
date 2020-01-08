package org.apache.iotdb.db.nvm.metadata;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.nvm.space.NVMSpace;

public class SpaceStatusBitMap extends NVMSpaceMetadata {

  public SpaceStatusBitMap(NVMSpace space) {
    super(space);
  }

  public void setUse(int index, boolean isTime) {
    space.getByteBuffer().put(index, isTime ? (byte) 1 : (byte) 2);
  }

  public void setFree(int index) {
    space.getByteBuffer().put(index, (byte) 0);
  }

  public List<Integer> getValidTimeSpaceIndexList(int count) {
    List<Integer> validTimeSpaceIndexList = new ArrayList<>();
    for (int i = 0; i < space.getSize(); i++) {
      byte flag = space.getByteBuffer().get(i);
      if (flag == 1) {
        validTimeSpaceIndexList.add(i);
        if (validTimeSpaceIndexList.size() == count) {
          break;
        }
      }
    }
    return validTimeSpaceIndexList;
  }
}
