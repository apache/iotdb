package org.apache.iotdb.influxdb.protocol.util;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.influxdb.InfluxDBException;

public class DataTypeUtils {

  /**
   * convert normal type to a type
   *
   * @param value need to convert value
   * @return corresponding TSDataType
   */
  public static TSDataType normalTypeToTSDataType(Object value) {
    if (value instanceof Boolean) {
      return TSDataType.BOOLEAN;
    } else if (value instanceof Integer) {
      return TSDataType.INT32;
    } else if (value instanceof Long) {
      return TSDataType.INT64;
    } else if (value instanceof Double) {
      return TSDataType.DOUBLE;
    } else if (value instanceof String) {
      return TSDataType.TEXT;
    } else {
      throw new InfluxDBException("not valid type:" + value.toString());
    }
  }
}
