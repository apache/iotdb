package org.apache.iotdb.db.metadata.rocksdb;

import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.*;
import static org.apache.iotdb.db.metadata.rocksdb.RocksDBUtils.NODE_TYPE_ARRAY;

public enum RocksDBMNodeType {
  ROOT(NODE_TYPE_ROOT),
  INTERNAL(NODE_TYPE_INTERNAL),
  STORAGE_GROUP(NODE_TYPE_SG),
  ENTITY(NODE_TYPE_ENTITY),
  MEASUREMENT(NODE_TYPE_MEASUREMENT),
  ALISA(NODE_TYPE_ALIAS);

  byte value;

  RocksDBMNodeType(byte value) {
    this.value = value;
  }

  public RocksDBMNodeType of(byte type) {
    return NODE_TYPE_ARRAY[type];
  }

  public byte getValue() {
    return value;
  }
}
