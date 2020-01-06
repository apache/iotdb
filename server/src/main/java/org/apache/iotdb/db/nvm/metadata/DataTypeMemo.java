package org.apache.iotdb.db.nvm.metadata;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.nvm.space.NVMSpace;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class DataTypeMemo extends NVMSpaceMetadata {

  public DataTypeMemo(NVMSpace space) {
    super(space);
  }
  
  public void set(int index, TSDataType dataType) {
    space.getByteBuffer().putShort(index, dataType.serialize());
  }

  public List<TSDataType> getDataTypeList(int num) {
    List<TSDataType> dataTypeList = new ArrayList<>(num);
    for (int i = 0; i < num; i++) {
      TSDataType dataType = TSDataType.deserialize(space.getByteBuffer().getShort(i));
      dataTypeList.add(dataType);
    }
    return dataTypeList;
  }
}
