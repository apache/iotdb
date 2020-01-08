package org.apache.iotdb.db.nvm.metadata;

import org.apache.iotdb.db.nvm.space.NVMSpace;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class DataTypeMemo extends NVMSpaceMetadata {

  public DataTypeMemo(NVMSpace space) {
    super(space);
  }
  
  public void set(int index, TSDataType dataType) {
    space.getByteBuffer().putShort(index, dataType.serialize());
  }

  public TSDataType get(int index) {
    return TSDataType.deserialize(space.getByteBuffer().getShort(index));
  }
}
