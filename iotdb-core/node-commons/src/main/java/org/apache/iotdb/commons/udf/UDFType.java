package org.apache.iotdb.commons.udf;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public enum UDFType {
  ILLEGAL((byte) -1),
  TREE_EXTERNAL((byte) 0),
  TREE_BUILT_IN((byte) 1),
  TREE_UNAVAILABLE((byte) 2),
  TABLE_EXTERNAL((byte) 3),
  TABLE_UNAVAILABLE((byte) 4);

  private final byte type;

  UDFType(byte type) {
    this.type = type;
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(type, stream);
  }

  public static UDFType deserialize(ByteBuffer buffer) {
    byte type = ReadWriteIOUtils.readByte(buffer);
    for (UDFType udfType : UDFType.values()) {
      if (udfType.type == type) {
        return udfType;
      }
    }
    return ILLEGAL;
  }

  public boolean isTreeModel() {
    return this == TREE_EXTERNAL || this == TREE_BUILT_IN || this == TREE_UNAVAILABLE;
  }

  public boolean isTableModel() {
    return this == TABLE_EXTERNAL || this == TABLE_UNAVAILABLE;
  }

  public boolean isAvailable() {
    return this == TREE_EXTERNAL || this == TREE_BUILT_IN || this == TABLE_EXTERNAL;
  }
}
