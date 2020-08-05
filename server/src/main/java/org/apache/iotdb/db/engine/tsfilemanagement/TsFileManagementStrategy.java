package org.apache.iotdb.db.engine.tsfilemanagement;

import org.apache.iotdb.db.engine.tsfilemanagement.level.LevelTsFileManagement;
import org.apache.iotdb.db.engine.tsfilemanagement.normal.NormalTsFileManagement;

public enum TsFileManagementStrategy {
  LevelStrategy,
  NormalStrategy;

  public TsFileManagement getTsFileManagement(String storageGroupName, String storageGroupDir) {
    switch (this) {
      case LevelStrategy:
        return new LevelTsFileManagement(storageGroupName, storageGroupDir);
      case NormalStrategy:
      default:
        return new NormalTsFileManagement(storageGroupName, storageGroupDir);
    }
  }
}
