package org.apache.iotdb.db.conf.directories.strategy;

public enum DirectoryStrategyType {
  SEQUENCE_STRATEGY,
  MAX_DISK_USABLE_SPACE_FIRST_STRATEGY,
  MIN_FOLDER_OCCUPIED_SPACE_FIRST_STRATEGY,
  RANDOM_ON_DISK_USABLE_SPACE_STRATEGY,
}
