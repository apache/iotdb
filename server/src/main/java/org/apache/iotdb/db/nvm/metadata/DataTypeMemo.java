package org.apache.iotdb.db.nvm.metadata;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class DataTypeMemo extends NVMSpaceMetadata {

  public DataTypeMemo(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }
  
  public void set(int index, TSDataType dataType) {
    byteBuffer.putShort(index, dataType.serialize());
  }

  public List<TSDataType> getDataTypeList() {
    List<TSDataType> dataTypeList = new ArrayList<>();
    for (int i = 0; i < byteBuffer.capacity(); i++) {
      TSDataType dataType = TSDataType.deserialize(byteBuffer.getShort(i));
      dataTypeList.add(dataType);
    }
    return dataTypeList;
  }
}
