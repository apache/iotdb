package org.apache.iotdb.db.metadata.rocksdb;

import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.MAX_NODE_TYPE_NUM;

public class CheckKeyResult {

  private boolean[] result = new boolean[MAX_NODE_TYPE_NUM];
  private boolean existAnyKey = false;

  public void setSingleCheckValue(char index, boolean value) {
    if (value) {
      existAnyKey = true;
    }
    result[index] = value;
  }

  public boolean existAnyKey() {
    return existAnyKey;
  }

  public boolean getResult(RocksDBMNodeType type) {
    return result[type.value];
  }
}
