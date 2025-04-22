package org.apache.iotdb.udf.api.relational.table;

/**
 * An area to store all information necessary to execute the table function, gathered at analysis
 * time
 */
public interface TableFunctionHandle {
  /**
   * Serialize your state into byte array. The order of serialization must be consistent with
   * deserialization.
   */
  byte[] serialize();

  /**
   * Deserialize byte array into your state. The order of deserialization must be consistent with
   * serialization.
   */
  void deserialize(byte[] bytes);
}
