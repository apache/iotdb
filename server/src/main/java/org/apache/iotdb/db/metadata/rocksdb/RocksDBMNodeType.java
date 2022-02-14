package org.apache.iotdb.db.metadata.rocksdb;

import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.NODE_TYPE_ALIAS;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.NODE_TYPE_ENTITY;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.NODE_TYPE_INTERNAL;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.NODE_TYPE_MEASUREMENT;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.NODE_TYPE_SG;
import static org.apache.iotdb.db.metadata.rocksdb.RocksDBUtils.NODE_TYPE_ARRAY;

public enum RocksDBMNodeType {
  INTERNAL(NODE_TYPE_INTERNAL),
  STORAGE_GROUP(NODE_TYPE_SG),
  ENTITY(NODE_TYPE_ENTITY),
  MEASUREMENT(NODE_TYPE_MEASUREMENT),
  ALISA(NODE_TYPE_ALIAS);

  char value;

  RocksDBMNodeType(char value) {
    this.value = value;
  }

  public RocksDBMNodeType of(char type) {
    return NODE_TYPE_ARRAY[type];
  }

  public char getValue() {
    return value;
  }
}
