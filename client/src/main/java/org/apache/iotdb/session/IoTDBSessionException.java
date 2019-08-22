package org.apache.iotdb.session;

public class IoTDBSessionException extends RuntimeException {

  private static final long serialVersionUID = 2405104784097667293L;

  public IoTDBSessionException(String msg) {
    super(msg);
  }

  public IoTDBSessionException(String message, Throwable cause) {
    super(message, cause);
  }

  public IoTDBSessionException(Throwable cause) {
    super(cause);
  }
}
