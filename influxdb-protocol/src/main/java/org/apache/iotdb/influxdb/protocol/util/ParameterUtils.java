package org.apache.iotdb.influxdb.protocol.util;

public class ParameterUtils {

  /**
   * check whether the field is empty. If it is empty, an error will be thrown
   *
   * @param string string to check
   * @param name prompt information in error throwing
   */
  public static void checkNonEmptyString(String string, String name)
      throws IllegalArgumentException {
    if (string == null || string.isEmpty()) {
      throw new IllegalArgumentException("Expecting a non-empty string for " + name);
    }
  }
}
