package org.apache.iotdb.db.metadata;

public enum MetadataManagerType {
  MEMORY_MANAGER,
  ROCKSDB_MANAGER;

  public static MetadataManagerType of(String value) {
    try {
      return Enum.valueOf(MetadataManagerType.class, value);
    } catch (Exception e) {
      return MEMORY_MANAGER;
    }
  }
}
