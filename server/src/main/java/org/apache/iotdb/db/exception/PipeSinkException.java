package org.apache.iotdb.db.exception;

import org.apache.iotdb.rpc.TSStatusCode;

public class PipeSinkException extends IoTDBException {

  private static final long serialVersionUID = -2355881952697245662L;

  public PipeSinkException(String message, int errorCode) {
    super(message, errorCode);
  }

  public PipeSinkException(String message) {
    super(message, TSStatusCode.PIPESINK_ERROR.getStatusCode());
  }

  public PipeSinkException(String attr, String value, String attrType) {
    super(
        String.format("%s %s has wrong format, require for %s.", attr, value, attrType),
        TSStatusCode.PIPESINK_ERROR.getStatusCode());
  }
}
