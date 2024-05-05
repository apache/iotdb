package org.apache.iotdb.db.queryengine.plan.relational.analyzer.schema.cache;

public class CacheMemoryControlUtil {

  public static int estimateStringSize(String string) {
    // each char takes 2B in Java
    return string == null ? 0 : 32 + 2 * string.length();
  }
}
