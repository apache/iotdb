package org.apache.iotdb.flink.sql.exception;

public class IllegalSchemaException extends RuntimeException {
  public IllegalSchemaException(String s) {
    super(s);
  }
}
