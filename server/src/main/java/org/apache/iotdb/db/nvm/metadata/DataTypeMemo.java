package org.apache.iotdb.db.nvm.metadata;

import java.io.IOException;
import org.apache.iotdb.db.nvm.space.NVMSpaceManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class DataTypeMemo extends NVMSpaceMetadata {

  public DataTypeMemo() throws IOException {
  }

  public void set(int index, TSDataType dataType) {
    space.putShort(index, dataType.serialize());
  }

  public TSDataType get(int index) {
    return TSDataType.deserialize(space.getShort(index));
  }

  @Override
  int getUnitSize() {
    return Short.BYTES;
  }

  @Override
  int getUnitNum() {
    return NVMSpaceManager.NVMSPACE_NUM_MAX;
  }
}
