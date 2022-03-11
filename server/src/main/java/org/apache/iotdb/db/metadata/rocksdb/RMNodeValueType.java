package org.apache.iotdb.db.metadata.rocksdb;

public enum RMNodeValueType {
  TTL(RockDBConstants.FLAG_SET_TTL, RockDBConstants.DATA_BLOCK_TYPE_TTL),
  SCHEMA(RockDBConstants.FLAG_HAS_SCHEMA, RockDBConstants.DATA_BLOCK_TYPE_SCHEMA),
  ALIAS(RockDBConstants.FLAG_HAS_ALIAS, RockDBConstants.DATA_BLOCK_TYPE_ALIAS),
  TAGS(RockDBConstants.FLAG_HAS_TAGS, RockDBConstants.DATA_BLOCK_TYPE_TAGS),
  ATTRIBUTES(RockDBConstants.FLAG_HAS_ATTRIBUTES, RockDBConstants.DATA_BLOCK_TYPE_ATTRIBUTES),
  ORIGIN_KEY(null, RockDBConstants.DATA_BLOCK_TYPE_ORIGIN_KEY);

  byte type;
  Byte flag;

  RMNodeValueType(Byte flag, byte type) {
    this.type = type;
    this.flag = flag;
  }
}
