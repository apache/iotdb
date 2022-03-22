package org.apache.iotdb.db.metadata.rocksdb.mnode;

import org.apache.iotdb.db.metadata.rocksdb.RSchemaConstants;

public enum RMNodeValueType {
  TTL(RSchemaConstants.FLAG_SET_TTL, RSchemaConstants.DATA_BLOCK_TYPE_TTL),
  SCHEMA(RSchemaConstants.FLAG_HAS_SCHEMA, RSchemaConstants.DATA_BLOCK_TYPE_SCHEMA),
  ALIAS(RSchemaConstants.FLAG_HAS_ALIAS, RSchemaConstants.DATA_BLOCK_TYPE_ALIAS),
  TAGS(RSchemaConstants.FLAG_HAS_TAGS, RSchemaConstants.DATA_BLOCK_TYPE_TAGS),
  ATTRIBUTES(RSchemaConstants.FLAG_HAS_ATTRIBUTES, RSchemaConstants.DATA_BLOCK_TYPE_ATTRIBUTES),
  ORIGIN_KEY(null, RSchemaConstants.DATA_BLOCK_TYPE_ORIGIN_KEY);

  private byte type;
  private Byte flag;

  RMNodeValueType(Byte flag, byte type) {
    this.type = type;
    this.flag = flag;
  }

  public byte getType() {
    return type;
  }

  public Byte getFlag() {
    return flag;
  }
}
