package org.apache.iotdb.commons.exception;

import org.apache.iotdb.rpc.TSStatusCode;

public class TTLException extends Exception {
  private TSStatusCode code;

  public TTLException(String path) {
    super(
        String.format(
            "Illegal pattern path: %s, pattern path should end with **, otherwise, it should be a specific database or device path without *",
            path));
    code = TSStatusCode.ILLEGAL_PARAMETER;
  }

  public TSStatusCode getCode() {
    return code;
  }
}
