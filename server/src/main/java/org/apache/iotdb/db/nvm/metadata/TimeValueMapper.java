package org.apache.iotdb.db.nvm.metadata;

import java.nio.ByteBuffer;

public class TimeValueMapper extends NVMSpaceMetadata {

  public TimeValueMapper(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  public void map(int timeSpaceIndex, int valueSpaceIndex) {
    byteBuffer.putInt(timeSpaceIndex);
    byteBuffer.putInt(valueSpaceIndex);
  }
}
