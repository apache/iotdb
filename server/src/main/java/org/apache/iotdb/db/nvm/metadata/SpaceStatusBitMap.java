package org.apache.iotdb.db.nvm.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.nvm.space.NVMSpaceManager;

public class SpaceStatusBitMap extends NVMSpaceMetadata {

  public SpaceStatusBitMap() throws IOException {
  }

  public void setUse(int index, boolean isTime) {
    space.put(index, isTime ? (byte) 1 : (byte) 2);
  }

  public void setFree(int index) {
    space.put(index, (byte) 0);
  }

  public List<Integer> getValidTimeSpaceIndexList(int count) {
    List<Integer> validTimeSpaceIndexList = new ArrayList<>();
    for (int i = 0; i < space.getSize(); i++) {
      byte flag = space.get(i);
      if (flag == 1) {
        validTimeSpaceIndexList.add(i);
        if (validTimeSpaceIndexList.size() == count) {
          break;
        }
      }
    }
    return validTimeSpaceIndexList;
  }

  @Override
  int getUnitSize() {
    return Byte.BYTES;
  }

  @Override
  int getUnitNum() {
    return NVMSpaceManager.NVMSPACE_NUM_MAX;
  }
}
